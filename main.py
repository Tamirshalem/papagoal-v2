import os, time, json, logging, threading
from datetime import datetime, timezone
from urllib.parse import urlparse
from flask import Flask, jsonify, render_template_string, request
import pg8000.native
import requests

# ─── Config ───────────────────────────────────────────────────────────────────
ODDSAPI_KEY       = os.environ.get("ODDSPAPI_KEY", "")
FOOTBALL_API_KEY  = os.environ.get("FOOTBALL_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
BETFAIR_APP_KEY   = os.environ.get("BETFAIR_APP_KEY", "")
BETFAIR_USERNAME  = os.environ.get("BETFAIR_USERNAME", "")
BETFAIR_PASSWORD  = os.environ.get("BETFAIR_PASSWORD", "")
PORT              = int(os.environ.get("PORT", 8080))
POLL_INTERVAL     = 30
ODDSAPI_BASE      = "https://api.odds-api.io/v3"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("papagoal")
app = Flask(__name__)

# ─── Expected Odds Curves ─────────────────────────────────────────────────────
EXPECTED_OVER05_HT = {0:1.25,5:1.28,10:1.32,15:1.38,20:1.45,25:1.55,30:1.68,35:1.85,40:2.10,45:2.50}
EXPECTED_OVER15_HT = {0:2.10,5:2.15,10:2.22,15:2.32,20:2.45,25:2.65,30:2.90,35:3.20,40:3.60,45:4.20}
EXPECTED_OVER25    = {0:1.85,5:1.88,10:1.92,15:1.98,20:2.05,25:2.15,30:2.28,35:2.45,40:2.65,45:2.90,
                      50:3.10,55:3.35,60:3.65,65:4.00,70:4.50,75:5.20,80:6.50,85:9.00,88:12.0,90:20.0}

def get_expected(curve, minute):
    keys = sorted(curve.keys())
    for i, k in enumerate(keys):
        if minute <= k:
            if i == 0: return curve[k]
            prev_k = keys[i-1]
            r = (minute - prev_k) / (k - prev_k)
            return curve[prev_k] + r * (curve[k] - curve[prev_k])
    return curve[keys[-1]]

def calc_pressure(current, opening, minute, market="over25"):
    if not opening or not current or opening == 0: return 0
    rise = current / opening
    exp  = get_expected(EXPECTED_OVER05_HT if market=="over05ht" else
                        EXPECTED_OVER15_HT if market=="over15ht" else
                        EXPECTED_OVER25, min(minute,45) if "ht" in market else minute)
    exp_ratio = exp / opening
    if exp_ratio <= 0: return 0
    return max(0, min(100, int((1 - rise/exp_ratio)*100)))

# ─── DB ───────────────────────────────────────────────────────────────────────
def parse_db(url):
    p = urlparse(url)
    return {"host":p.hostname,"port":p.port or 5432,"database":p.path.lstrip("/"),
            "user":p.username,"password":p.password,"ssl_context":True}

def get_db():
    return pg8000.native.Connection(**parse_db(DATABASE_URL))

def init_db():
    conn = get_db()
    try:
        conn.run("""CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY, match_id TEXT UNIQUE,
            league TEXT, home_team TEXT, away_team TEXT,
            minute INT DEFAULT 0, score_home INT DEFAULT 0, score_away INT DEFAULT 0,
            status TEXT DEFAULT 'upcoming', event_id TEXT,
            last_updated TIMESTAMPTZ DEFAULT NOW()
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS odds_snapshots (
            id SERIAL PRIMARY KEY, match_id TEXT,
            captured_at TIMESTAMPTZ DEFAULT NOW(),
            minute INT DEFAULT 0, score_home INT DEFAULT 0, score_away INT DEFAULT 0,
            market TEXT, outcome TEXT, bookmaker TEXT DEFAULT 'bet365',
            odd_value FLOAT, prev_odd FLOAT, opening_odd FLOAT,
            odd_change FLOAT DEFAULT 0, direction TEXT DEFAULT 'stable',
            held_seconds INT DEFAULT 0, pressure INT DEFAULT 0,
            expected_odd FLOAT, is_live BOOLEAN DEFAULT FALSE,
            goal_30s BOOLEAN DEFAULT FALSE, goal_60s BOOLEAN DEFAULT FALSE,
            goal_120s BOOLEAN DEFAULT FALSE, goal_300s BOOLEAN DEFAULT FALSE
        )""")
        conn.run("CREATE INDEX IF NOT EXISTS idx_os_match ON odds_snapshots(match_id)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_os_time ON odds_snapshots(captured_at)")
        conn.run("""CREATE TABLE IF NOT EXISTS goals (
            id SERIAL PRIMARY KEY, match_id TEXT,
            minute INT, score_before TEXT, score_after TEXT,
            goal_time TIMESTAMPTZ DEFAULT NOW(),
            auto_detected BOOLEAN DEFAULT TRUE,
            had_snapshots BOOLEAN DEFAULT FALSE,
            odds_10s JSONB DEFAULT '{}', odds_30s JSONB DEFAULT '{}',
            odds_60s JSONB DEFAULT '{}', odds_120s JSONB DEFAULT '{}',
            odds_300s JSONB DEFAULT '{}'
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS rules (
            id SERIAL PRIMARY KEY,
            rule_name TEXT UNIQUE,
            description TEXT,
            conditions_json JSONB DEFAULT '{}',
            action TEXT DEFAULT 'GOAL',
            source TEXT DEFAULT 'manual',
            is_active BOOLEAN DEFAULT TRUE,
            total_signals INT DEFAULT 0,
            success_count INT DEFAULT 0,
            fail_count INT DEFAULT 0,
            success_rate FLOAT DEFAULT 0,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            last_updated TIMESTAMPTZ DEFAULT NOW()
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY, match_id TEXT,
            detected_at TIMESTAMPTZ DEFAULT NOW(),
            home_team TEXT, away_team TEXT, league TEXT,
            rule_num INT, rule_name TEXT,
            minute INT DEFAULT 0, score TEXT DEFAULT '0-0',
            signal_type TEXT, verdict TEXT, confidence INT,
            reason TEXT, over_odd FLOAT, draw_odd FLOAT,
            over05ht_odd FLOAT, over15ht_odd FLOAT,
            opening_over25 FLOAT, opening_over05ht FLOAT, opening_over15ht FLOAT,
            pressure_score INT DEFAULT 0,
            held_seconds INT DEFAULT 0, direction TEXT DEFAULT 'stable',
            odd_change FLOAT DEFAULT 0
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS paper_trades (
            id SERIAL PRIMARY KEY,
            signal_id INT, match_id TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            home_team TEXT, away_team TEXT, league TEXT,
            entry_odd FLOAT, market TEXT DEFAULT 'over_goal',
            verdict TEXT, rule_name TEXT,
            minute_entry INT, score_entry TEXT,
            pressure_at_entry INT DEFAULT 0,
            result TEXT DEFAULT 'pending',
            resolved_at TIMESTAMPTZ,
            profit_loss FLOAT DEFAULT 0,
            notes TEXT
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS ai_insights (
            id SERIAL PRIMARY KEY, created_at TIMESTAMPTZ DEFAULT NOW(),
            insight_type TEXT, content TEXT, goals_analyzed INT DEFAULT 0
        )""")
        # Insert default rules if none exist
        existing = conn.run("SELECT COUNT(*) FROM rules")[0][0]
        if existing == 0:
            _seed_default_rules(conn)
        log.info("✅ DB ready")
    except Exception as e:
        log.error(f"DB init: {e}")
    finally:
        conn.close()

def _seed_default_rules(conn):
    defaults = [
        ("Early Draw Signal","Draw 1.57-1.66 + Over 1.83-2.10 at min 21-25","DRAW_UNDER","manual"),
        ("Frozen Over","Over stuck 1.80-1.86 at min 26-30","NO_ENTRY","manual"),
        ("Two Early Goals Trap","Over 1.66-1.75 – trap zone","TRAP","manual"),
        ("Over 2.10 Value","Over >= 2.10 at min 30-34","GOAL","manual"),
        ("1.66 Trap","Any market shows 1.66","TRAP","manual"),
        ("Pair Signal","Draw ~1.61 + Over ~1.90","GOAL","manual"),
        ("3rd Goal Moment","Over >= 2.15 at min 65-70","GOAL","manual"),
        ("Market Shut","Over >= 2.80 after min 82","NO_GOAL","manual"),
        ("Early Drop Signal","Over drops to 1.55 at min 17-20","GOAL","manual"),
        ("Opening 1.30 Rule","Home win opens at 1.30","GOAL","manual"),
        ("1.57 Entry Point","Over 1.54-1.60","GOAL","manual"),
        ("Duration HELD","Over 2.30-2.70 held 2+ min","GOAL","manual"),
        ("Duration REJECTED","Over spikes in 30s","NO_GOAL","manual"),
        ("Sharp Drop Signal","Over drops 0.15+ quickly","GOAL","manual"),
        ("Market Reversal","Sharp drop then rise in 60s","TRAP","manual"),
        ("HT Pressure Over 0.5","Over 0.5 HT rising slower than expected","GOAL","manual"),
        ("HT Pressure Over 1.5","Over 1.5 HT rising slower than expected","GOAL","manual"),
        ("Late Game Pressure","Over 1.0+ below expected at min 80+","GOAL","manual"),
        ("Late Odd Sweet Spot","Over 2.7-3.5 at min 85-93","GOAL","manual"),
        ("High Market Pressure","Pressure score >= 60%","GOAL","manual"),
    ]
    for r in defaults:
        try:
            conn.run("""INSERT INTO rules (rule_name,description,action,source)
                VALUES (:a,:b,:c,:d) ON CONFLICT (rule_name) DO NOTHING""",
                a=r[0],b=r[1],c=r[2],d=r[3])
        except: pass

# ─── Opening Odds Cache ───────────────────────────────────────────────────────
opening_cache = {}
last_prices   = {}
last_scores   = {}

def get_opening(mid, market):
    return opening_cache.get(f"{mid}_{market}")

def set_opening(mid, market, odd):
    key = f"{mid}_{market}"
    if key not in opening_cache:
        opening_cache[key] = odd
        return True
    return False

# ─── Rules Engine ─────────────────────────────────────────────────────────────
def run_rules(match_id, home, away, league, over, draw, hw, aw,
              over05ht, over15ht, op25, op05ht, op15ht,
              minute, held, direction, change, pressure):
    signals = []
    o=over or 0; d=draw or 0; m=minute or 0; p=pressure or 0
    o5=over05ht or 0; o15=over15ht or 0

    def add(num, name, stype, verdict, conf, reason):
        signals.append({"rule_num":num,"rule_name":name,"signal_type":stype,
                        "verdict":verdict,"confidence":conf,"reason":reason})

    # HT Pressure
    if o5>0 and op05ht and m<=45 and m>=15:
        if (o5/op05ht) < (get_expected(EXPECTED_OVER05_HT,m)/op05ht)*0.88:
            p5=calc_pressure(o5,op05ht,m,"over05ht")
            add(101,"HT Pressure Over 0.5","goal",f"GOAL ENTRY – HT 0.5 @ {o5:.2f}",min(92,70+p5//5),
                f"Opening {op05ht:.2f} → min {m}: {o5:.2f} | Expected: {get_expected(EXPECTED_OVER05_HT,m):.2f} | Pressure: {p5}%")

    if o15>0 and op15ht and m<=45 and m>=15:
        if (o15/op15ht) < (get_expected(EXPECTED_OVER15_HT,m)/op15ht)*0.88:
            p15=calc_pressure(o15,op15ht,m,"over15ht")
            add(102,"HT Pressure Over 1.5","goal",f"GOAL ENTRY – HT 1.5 @ {o15:.2f}",min(90,65+p15//5),
                f"Opening {op15ht:.2f} → min {m}: {o15:.2f} | Pressure: {p15}%")

    # Late game
    if m>=80 and o>0:
        exp=get_expected(EXPECTED_OVER25,m); gap=exp-o
        if gap>=0.8:
            add(103,"Late Game Pressure","goal",f"GOAL LIKELY – Over {o:.2f} vs expected {exp:.1f}",min(92,int(70+gap*8)),
                f"Min {m}: Over {o:.2f} | Expected: {exp:.1f} | Gap: {gap:.1f} | Pressure: {p}%")

    if 85<=m<=93 and 2.7<=o<=3.5:
        add(104,"Late Odd Sweet Spot","goal",f"HOT – Over {o:.2f} @ min {m}",88,
            f"Odds {o:.2f} at min {m} – too low for this stage. Goal expected.")

    # Standard rules
    if 21<=m<=25 and 1.57<=d<=1.66 and 1.83<=o<=2.10: add(1,"Early Draw Signal","no_goal","DRAW or UNDER",75,f"Draw {d} + Over {o} at min {m}")
    if 26<=m<=30 and 1.80<=o<=1.86 and 1.58<=d<=1.64: add(2,"Frozen Over","no_goal","NO ENTRY",70,f"Over stuck {o} at min {m}")
    if 1.66<=o<=1.75: add(3,"Two Early Goals Trap","trap","UNDER / TRAP",72,f"Over {o} – trap zone")
    if 30<=m<=34 and o>=2.10: add(4,"Over 2.10 Value","goal","GOAL ENTRY",78,f"Over {o} at min {m}")
    if 1.63<=o<=1.69: add(5,"1.66 Trap","trap","DO NOT ENTER",80,f"Over {o} – classic trap zone")
    if 1.58<=d<=1.64 and 1.87<=o<=1.93: add(6,"Pair Signal","goal","GOAL",83,f"Draw {d} + Over {o}")
    if 65<=m<=70 and o>=2.15: add(7,"3rd Goal Moment","goal","GOAL ENTRY",76,f"Over {o} at min {m}")
    if m>=82 and o>=2.80 and p<30: add(8,"Market Shut","no_goal","NO GOAL",88,f"Over {o} at min {m} – market closed")
    if 17<=m<=20 and o<=1.55: add(11,"Early Drop Signal","goal","GOAL VERY SOON",86,f"Over dropped to {o} at min {m}")
    if m<=15 and (hw or 0)<=1.32: add(12,"Opening 1.30 Rule","goal","EARLY GOAL",88,f"Opening {hw}")
    if 1.54<=o<=1.60: add(13,"1.57 Entry Point","goal","ENTRY",79,f"Over {o}")
    if 2.30<=o<=2.70:
        if held>=120: add(14,"Duration HELD","goal","POSSIBLE GOAL",82,f"Over {o} held {held}s – market believes")
        elif 0<held<=30 and direction=="up": add(14,"Duration REJECTED","no_goal","NO GOAL",80,f"Over {o} spiked in {held}s")
    if direction=="down" and change<=-0.15: add(15,"Sharp Drop Signal","goal","GOAL PRESSURE",74,f"Over dropped {change}")
    if p>=60 and o>1.60 and not any(s["rule_num"] in [101,102,103,104] for s in signals):
        add(200,"High Market Pressure","goal",f"HOT – {p}% market pressure",min(90,p),f"Over {o} at min {m} – pressure {p}%")

    return signals

# ─── Live Data ────────────────────────────────────────────────────────────────
live_data      = {}
betfair_ht     = {}
betfair_session= {"token":None,"expires":0}

def fetch_live_football():
    if not FOOTBALL_API_KEY: return
    try:
        r = requests.get("https://v3.football.api-sports.io/fixtures",
                        headers={"x-apisports-key":FOOTBALL_API_KEY},
                        params={"live":"all"},timeout=10)
        if r.status_code!=200: return
        live_data.clear()
        for f in r.json().get("response",[]):
            try:
                home=f["teams"]["home"]["name"]; away=f["teams"]["away"]["name"]
                min_=f["fixture"]["status"]["elapsed"] or 0
                hg=f["goals"]["home"] or 0; ag=f["goals"]["away"] or 0
                live_data[f"{home}_{away}"]={
                    "minute":min_,"score":f"{hg}-{ag}","hg":hg,"ag":ag,
                    "league":f["league"]["name"],
                    "h1":home.split()[0].lower(),"a1":away.split()[0].lower()}
            except: continue
        log.info(f"⏱ {len(live_data)} live fixtures")
    except Exception as e: log.error(f"Football API: {e}")

def betfair_login():
    if not BETFAIR_APP_KEY: return False
    try:
        for url in ["https://identitysso-cert.betfair.com/api/login","https://identitysso.betfair.com/api/login"]:
            try:
                r=requests.post(url,data={"username":BETFAIR_USERNAME,"password":BETFAIR_PASSWORD},
                    headers={"X-Application":BETFAIR_APP_KEY,"Content-Type":"application/x-www-form-urlencoded","Accept":"application/json"},timeout=10)
                if r.text:
                    d=r.json()
                    if d.get("status")=="SUCCESS":
                        betfair_session["token"]=d["token"]; betfair_session["expires"]=time.time()+3600
                        log.info("✅ Betfair logged in"); return True
            except: continue
        return False
    except: return False

def fetch_betfair_ht():
    if not BETFAIR_APP_KEY: return
    token=betfair_session.get("token")
    if not token or time.time()>betfair_session.get("expires",0):
        if not betfair_login(): return
        token=betfair_session.get("token")
    if not token: return
    try:
        r=requests.post("https://api.betfair.com/exchange/betting/json-rpc/v1",
            headers={"X-Application":BETFAIR_APP_KEY,"X-Authentication":token,"Content-Type":"application/json"},
            json=[{"jsonrpc":"2.0","method":"SportsAPING/v1.0/listMarketCatalogue",
                   "params":{"filter":{"eventTypeIds":["1"],"inPlayOnly":True,"marketTypeCodes":["OVER_UNDER_05","OVER_UNDER_15"]},
                   "marketProjection":["EVENT","MARKET_TYPE"],"maxResults":100},"id":1}],timeout=10)
        result=r.json(); markets=(result[0].get("result",[]) or []) if result and isinstance(result,list) else []
        if not markets: return
        ids=[m["marketId"] for m in markets[:10]]
        r2=requests.post("https://api.betfair.com/exchange/betting/json-rpc/v1",
            headers={"X-Application":BETFAIR_APP_KEY,"X-Authentication":token,"Content-Type":"application/json"},
            json=[{"jsonrpc":"2.0","method":"SportsAPING/v1.0/listMarketBook",
                   "params":{"marketIds":ids,"priceProjection":{"priceData":["EX_BEST_OFFERS"]}},"id":1}],timeout=10)
        books=r2.json()[0].get("result",[]) or []
        odds_by_id={b["marketId"]:b.get("runners",[{}])[0].get("ex",{}).get("availableToBack",[{}])[0].get("price",0) for b in books}
        for m in markets:
            key=m.get("event",{}).get("name","").replace(" v ","_").replace(" vs ","_")
            mtype=m.get("marketType",""); mid=m.get("marketId","")
            if key not in betfair_ht: betfair_ht[key]={}
            if mtype=="OVER_UNDER_05" and mid in odds_by_id: betfair_ht[key]["over05ht"]=odds_by_id[mid]
            elif mtype=="OVER_UNDER_15" and mid in odds_by_id: betfair_ht[key]["over15ht"]=odds_by_id[mid]
        log.info(f"🎰 Betfair HT: {len(betfair_ht)} markets")
    except Exception as e: log.error(f"Betfair HT: {e}")

def get_live(home,away):
    key=f"{home}_{away}"
    if key in live_data:
        d=live_data[key]; return d["minute"],d["score"],d["hg"],d["ag"],d["league"],True
    h1=home.split()[0].lower(); a1=away.split()[0].lower()
    for v in live_data.values():
        if h1 in v.get("h1","") or a1 in v.get("a1",""):
            return v["minute"],v["score"],v["hg"],v["ag"],v["league"],True
    return 0,"0-0",0,0,"",False

def get_ht(home,away):
    key=f"{home}_{away}"
    if key in betfair_ht: return betfair_ht[key]
    h1=home.split()[0].lower(); a1=away.split()[0].lower()
    for k,v in betfair_ht.items():
        if h1 in k.lower() or a1 in k.lower(): return v
    return {}

def get_odds_at_time(conn,match_id,goal_time,seconds_before):
    try:
        rows=conn.run("""SELECT market,outcome,odd_value FROM odds_snapshots
            WHERE match_id=:a AND captured_at BETWEEN :b::timestamptz - INTERVAL '1 second'*:d AND :b::timestamptz - INTERVAL '1 second'*:c
            ORDER BY captured_at DESC LIMIT 20""",
            a=match_id,b=str(goal_time),c=max(0,seconds_before-8),d=seconds_before+15)
        return {f"{r[0]}_{r[1]}":r[2] for r in rows} if rows else {}
    except: return {}

# ─── OddsAPI.io ───────────────────────────────────────────────────────────────
def fetch_odds_api():
    if not ODDSAPI_KEY: return []
    try:
        r=requests.get(f"{ODDSAPI_BASE}/events",
            params={"apiKey":ODDSAPI_KEY,"sport":"football","status":"live","limit":50},timeout=15)
        if r.status_code!=200: log.warning(f"OddsAPI events: {r.status_code}"); return []
        raw=r.json()
        # Handle both list and dict responses
        if isinstance(raw, list):
            events = raw
        elif isinstance(raw, dict):
            events = raw.get("data") or raw.get("events") or raw.get("results") or []
        else:
            events = []
        if not events: return []
        log.info(f"📡 OddsAPI: {len(events)} live events")
        result=[]; ids=[str(e.get("id") or e.get("eventId") or e.get("event_id","")) for e in events if e.get("id") or e.get("eventId") or e.get("event_id")]
        for i in range(0,len(ids),10):
            batch=ids[i:i+10]
            try:
                r2=requests.get(f"{ODDSAPI_BASE}/odds",
                    params={"apiKey":ODDSAPI_KEY,"eventId":",".join(batch),"bookmakers":"Bet365",
                           "markets":"1x2,over_under_25,over_under_05_ht,over_under_15_ht"},timeout=15)
                if r2.status_code==200:
                    d2=r2.json()
                    if isinstance(d2, list): result.extend(d2)
                    elif isinstance(d2, dict): result.extend(d2.get("data") or d2.get("results") or [])
            except Exception as e: log.error(f"OddsAPI batch: {e}")
        return result
    except Exception as e: log.error(f"OddsAPI: {e}"); return []

def parse_odds(items):
    parsed=[]
    for item in (items if isinstance(items,list) else [items]):
        try:
            home=(item.get("homeTeam") or item.get("home_team") or item.get("home") or "")
            away=(item.get("awayTeam") or item.get("away_team") or item.get("away") or "")
            if not home or not away: continue
            event_id=str(item.get("id") or item.get("eventId") or "")
            league=(item.get("league") or item.get("competition") or item.get("tournament") or "")
            over25=draw=hw=aw=over05ht=over15ht=None
            for bk in (item.get("bookmakers") or item.get("odds") or []):
                for mkt in (bk.get("markets") or bk.get("bets") or []):
                    mname=(mkt.get("name") or mkt.get("key") or mkt.get("type") or "").lower()
                    for out in (mkt.get("outcomes") or mkt.get("selections") or []):
                        oname=str(out.get("name") or out.get("label") or "").lower()
                        price=float(out.get("price") or out.get("odd") or 0)
                        if price<=1: continue
                        if "over_under_25" in mname or ("2.5" in mname and "over" in mname):
                            if "over" in oname: over25=price
                        elif "1x2" in mname or "match_winner" in mname:
                            if "draw" in oname or oname=="x": draw=price
                            elif oname in ["1","home"]: hw=price
                            elif oname in ["2","away"]: aw=price
                        elif "0.5" in mname and ("ht" in mname or "half" in mname):
                            if "over" in oname: over05ht=price
                        elif "1.5" in mname and ("ht" in mname or "half" in mname):
                            if "over" in oname: over15ht=price
            if over25 or draw:
                parsed.append({"event_id":event_id,"home":home,"away":away,"league":league,
                               "over25":over25,"draw":draw,"hw":hw,"aw":aw,
                               "over05ht":over05ht,"over15ht":over15ht})
        except: continue
    return parsed

# ─── Collector ────────────────────────────────────────────────────────────────
def collect():
    try:
        parsed=parse_odds(fetch_odds_api())
        if not parsed: log.info("No odds data"); return
        live_cnt=0; conn=get_db()
        try:
            for game in parsed:
                home=game["home"]; away=game["away"]
                event_id=game["event_id"]; league=game["league"]
                over25=game["over25"]; draw=game["draw"]
                hw=game["hw"]; aw=game["aw"]
                over05ht=game.get("over05ht"); over15ht=game.get("over15ht")
                min_,score,hg,ag,fb_league,is_live=get_live(home,away)
                if not league and fb_league: league=fb_league
                if is_live: live_cnt+=1
                if not over05ht or not over15ht:
                    ht=get_ht(home,away)
                    over05ht=over05ht or ht.get("over05ht")
                    over15ht=over15ht or ht.get("over15ht")
                match_id=f"oa_{event_id}" if event_id else f"oa_{home}_{away}".replace(" ","_")
                if over25:   set_opening(match_id,"over25",over25)
                if over05ht: set_opening(match_id,"over05ht",over05ht)
                if over15ht: set_opening(match_id,"over15ht",over15ht)
                op25=get_opening(match_id,"over25"); op05ht=get_opening(match_id,"over05ht"); op15ht=get_opening(match_id,"over15ht")
                try:
                    conn.run("""INSERT INTO matches (match_id,league,home_team,away_team,minute,score_home,score_away,status,event_id,last_updated)
                        VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,NOW()) ON CONFLICT (match_id) DO UPDATE SET
                        league=:b,minute=:e,score_home=:f,score_away=:g,status=:h,event_id=:i,last_updated=NOW()""",
                        a=match_id,b=league,c=home,d=away,e=min_,f=hg,g=ag,h='live' if is_live else 'upcoming',i=event_id)
                except: pass

                def track(market,outcome,price):
                    key=f"{match_id}_{market}_{outcome}"; now=time.time()
                    prev=held=None; direction="stable"; change=0.0
                    if key in last_prices:
                        lp=last_prices[key]; prev=lp["price"]; change=round(price-prev,3)
                        if abs(change)<0.005: held=int(now-lp["since"])
                        else: last_prices[key]={"price":price,"since":now}; direction="down" if change<0 else "up"
                    else: last_prices[key]={"price":price,"since":now}
                    held=int(now-last_prices[key]["since"])
                    opening=get_opening(match_id,market) or price
                    pres=calc_pressure(price,opening,min_)
                    exp=get_expected(EXPECTED_OVER25,min_) if market=="over25" else None
                    try:
                        conn.run("""INSERT INTO odds_snapshots (match_id,minute,score_home,score_away,market,outcome,bookmaker,
                            odd_value,prev_odd,opening_odd,odd_change,direction,held_seconds,pressure,expected_odd,is_live)
                            VALUES (:a,:b,:c,:d,:e,:f,'bet365',:g,:h,:i,:j,:k,:l,:m,:n,:o)""",
                            a=match_id,b=min_,c=hg,d=ag,e=market,f=outcome,g=price,h=prev,i=opening,
                            j=change,k=direction,l=held,m=pres,n=exp,o=is_live)
                    except: pass
                    return prev,held,direction,change

                prev_over=last_prices.get(f"{match_id}_over25_Over",{}).get("price")
                if over25:   track("over25","Over",over25)
                if draw:     track("1x2","Draw",draw)
                if hw:       track("1x2","Home",hw)
                if aw:       track("1x2","Away",aw)
                if over05ht: track("over05ht","Over",over05ht)
                if over15ht: track("over15ht","Over",over15ht)

                # Goal detection
                prev_total=last_scores.get(match_id); curr_total=hg+ag
                goal_time=datetime.now(timezone.utc)
                if prev_total is not None and curr_total>prev_total and is_live:
                    log.info(f"⚽ GOAL: {home} vs {away} {score} min:{min_}")
                    o10=get_odds_at_time(conn,match_id,goal_time,10)
                    o30=get_odds_at_time(conn,match_id,goal_time,30)
                    o60=get_odds_at_time(conn,match_id,goal_time,60)
                    o120=get_odds_at_time(conn,match_id,goal_time,120)
                    o300=get_odds_at_time(conn,match_id,goal_time,300)
                    conn.run("""INSERT INTO goals (match_id,minute,score_before,score_after,goal_time,auto_detected,had_snapshots,odds_10s,odds_30s,odds_60s,odds_120s,odds_300s)
                        VALUES (:a,:b,:c,:d,:e,TRUE,:f,:g,:h,:i,:j,:k)""",
                        a=match_id,b=min_,c=str(prev_total),d=score,e=str(goal_time),f=bool(o30),
                        g=json.dumps(o10),h=json.dumps(o30),i=json.dumps(o60),j=json.dumps(o120),k=json.dumps(o300))
                    for t,col in [(30,"goal_30s"),(60,"goal_60s"),(120,"goal_120s"),(300,"goal_300s")]:
                        try: conn.run(f"UPDATE odds_snapshots SET {col}=TRUE WHERE match_id=:a AND captured_at>NOW()-INTERVAL '{t} seconds'",a=match_id)
                        except: pass
                    # Resolve pending paper trades for this match
                    _resolve_match_trades(conn,match_id,score)
                last_scores[match_id]=curr_total

                # Run rules on live matches only
                if is_live and over25:
                    key_over=f"{match_id}_over25_Over"
                    held_over=int(time.time()-last_prices.get(key_over,{}).get("since",time.time()))
                    dir_over="stable"; chg_over=0.0
                    if prev_over:
                        chg_over=round(over25-prev_over,3)
                        dir_over="down" if chg_over<0 else ("up" if chg_over>0 else "stable")
                    pres=calc_pressure(over25,op25,min_)
                    sigs=run_rules(match_id,home,away,league,over25,draw,hw,aw,
                                  over05ht,over15ht,op25,op05ht,op15ht,
                                  min_,held_over,dir_over,chg_over,pres)
                    for s in sigs:
                        try:
                            res=conn.run("""INSERT INTO signals
                                (match_id,home_team,away_team,league,rule_num,rule_name,minute,score,
                                 signal_type,verdict,confidence,reason,over_odd,draw_odd,
                                 over05ht_odd,over15ht_odd,opening_over25,opening_over05ht,opening_over15ht,
                                 pressure_score,held_seconds,direction,odd_change)
                                VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,:p,:q,:r,:s,:t,:u,:v,:w)
                                RETURNING id""",
                                a=match_id,b=home,c=away,d=league,e=s["rule_num"],f=s["rule_name"],
                                g=min_,h=score,i=s["signal_type"],j=s["verdict"],k=s["confidence"],l=s["reason"],
                                m=over25,n=draw,o=over05ht,p=over15ht,q=op25,r=op05ht,s=op15ht,
                                t=pres,u=held_over,v=dir_over,w=chg_over)
                            sig_id=res[0][0] if res else None

                            # Update rule stats
                            try: conn.run("UPDATE rules SET total_signals=total_signals+1,last_updated=NOW() WHERE rule_name=:a",a=s["rule_name"])
                            except: pass

                            # Create paper trade for goal signals
                            if s["signal_type"]=="goal" and s["confidence"]>=70 and sig_id and over25:
                                conn.run("""INSERT INTO paper_trades
                                    (signal_id,match_id,home_team,away_team,league,entry_odd,market,verdict,rule_name,minute_entry,score_entry,pressure_at_entry)
                                    VALUES (:a,:b,:c,:d,:e,:f,'over_goal',:g,:h,:i,:j,:k)""",
                                    a=sig_id,b=match_id,c=home,d=away,e=league,f=over25,
                                    g=s["verdict"],h=s["rule_name"],i=min_,j=score,k=pres)
                        except: pass

                    # AI for hot signals
                    hot=[s for s in sigs if s["signal_type"]=="goal" and s["confidence"]>=75]
                    if hot and ANTHROPIC_API_KEY:
                        try:
                            sig_text=" | ".join([f"R{s['rule_num']} {s['rule_name']} ({s['confidence']}%)" for s in hot])
                            exp_now=get_expected(EXPECTED_OVER25,min_)
                            prompt=f"""You are PapaGoal AI – a professional betting market analyst.

Match: {home} vs {away} ({league})
Minute: {min_} | Score: {score}
Over 2.5: {over25} (opening: {op25 or '?'}) | Draw: {draw}
Expected at this minute: {exp_now:.2f} | Gap: {round(exp_now-over25,2) if over25 else '?'}
Over 0.5 HT: {over05ht or '?'} (opening: {op05ht or '?'})
Over 1.5 HT: {over15ht or '?'} (opening: {op15ht or '?'})
Market pressure: {pres}%
Signals: {sig_text}

3 short precise sentences in English:
1. What is the market saying right now?
2. Should we enter and at what odds?
3. What is the risk?"""
                            resp=requests.post("https://api.anthropic.com/v1/messages",
                                headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                                json={"model":"claude-sonnet-4-20250514","max_tokens":200,"messages":[{"role":"user","content":prompt}]},timeout=15)
                            if resp.status_code==200:
                                analysis=resp.json()["content"][0]["text"]
                                conn.run("INSERT INTO ai_insights (insight_type,content,goals_analyzed) VALUES ('live_signal',:a,1)",a=f"{match_id}|||{analysis}")
                                log.info(f"🤖 AI: {home} vs {away}")
                        except Exception as e: log.error(f"AI: {e}")

            # Auto-resolve old pending trades
            _resolve_old_trades(conn)
            log.info(f"✅ Saved | live:{live_cnt}/{len(parsed)}")
        finally: conn.close()
    except Exception as e: log.error(f"Collect: {e}")

def _resolve_match_trades(conn, match_id, new_score):
    """Resolve pending trades for a match when a goal is scored"""
    try:
        trades=conn.run("""SELECT id,score_entry,entry_odd FROM paper_trades
            WHERE match_id=:a AND result='pending'""",a=match_id)
        for t in trades:
            tid,score_before,odd=t[0],t[1],t[2]
            # Goal scored after trade was created = success
            conn.run("""UPDATE paper_trades SET result='success',resolved_at=NOW(),
                profit_loss=:a,notes='Goal detected after signal'
                WHERE id=:b""",a=round((odd-1)*100,2),b=tid)
            try: conn.run("""UPDATE rules SET success_count=success_count+1,
                success_rate=ROUND(success_count::float/(NULLIF(success_count+fail_count,0))*100,1),
                last_updated=NOW() WHERE rule_name=(SELECT rule_name FROM paper_trades WHERE id=:a)""",a=tid)
            except: pass
    except Exception as e: log.error(f"Resolve match trades: {e}")

def _resolve_old_trades(conn):
    """Resolve trades older than 15 minutes with no goal"""
    try:
        old=conn.run("""SELECT id,rule_name FROM paper_trades
            WHERE result='pending' AND created_at < NOW() - INTERVAL '15 minutes'""")
        for t in old:
            tid,rname=t[0],t[1]
            conn.run("""UPDATE paper_trades SET result='miss',resolved_at=NOW(),
                profit_loss=-100,notes='No goal in 15 minutes' WHERE id=:a""",a=tid)
            try: conn.run("""UPDATE rules SET fail_count=fail_count+1,
                success_rate=ROUND(success_count::float/(NULLIF(success_count+fail_count,0))*100,1),
                last_updated=NOW() WHERE rule_name=:a""",a=rname)
            except: pass
    except Exception as e: log.error(f"Resolve old trades: {e}")

def collector_loop():
    time.sleep(5)
    fetch_live_football()
    if BETFAIR_APP_KEY:
        betfair_login()
        fetch_betfair_ht()
    while True:
        collect()
        fetch_live_football()
        if BETFAIR_APP_KEY: fetch_betfair_ht()
        time.sleep(POLL_INTERVAL)

# ─── Dashboard HTML ───────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>PapaGoal — Read the Market</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Inter:wght@300;400;600;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#030308;--bg2:#060610;--card:#0a0a15;--card2:#0e0e1c;--border:#141428;--border2:#1c1c35;--green:#00ff88;--red:#ff3355;--yellow:#ffcc00;--orange:#ff6b35;--blue:#4488ff;--purple:#8855ff;--text:#e8e8f8;--muted:#6666aa}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;display:flex}
.sidebar{width:220px;min-height:100vh;background:var(--bg2);border-right:1px solid var(--border);display:flex;flex-direction:column;position:fixed;top:0;left:0;bottom:0;z-index:100}
.logo{padding:20px 16px;border-bottom:1px solid var(--border)}
.logo-main{font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:700;color:#fff;letter-spacing:2px}
.logo-main span{color:var(--green)}
.logo-sub{font-size:10px;color:var(--muted);letter-spacing:1px;margin-top:2px}
.nav{flex:1;padding:12px 8px}
.nav-item{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:8px;font-size:13px;color:var(--muted);cursor:pointer;transition:all 0.15s;margin-bottom:2px;border:none;background:none;width:100%;text-align:left;font-family:'Inter',sans-serif}
.nav-item:hover{background:var(--card);color:var(--text)}
.nav-item.active{background:rgba(0,255,136,0.1);color:var(--green)}
.main{margin-left:220px;flex:1}
.page{display:none;padding:24px;max-width:1200px}
.page.active{display:block}
.page-header{margin-bottom:20px;display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:10px}
.page-title{font-size:22px;font-weight:700}
.page-sub{font-size:12px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:4px}
.stats-row{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:20px}
.stat-card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px}
.stat-num{font-size:26px;font-weight:900;font-family:'JetBrains Mono',monospace}
.stat-label{font-size:11px;color:var(--muted);margin-top:4px}
.section-title{font-size:11px;letter-spacing:3px;color:var(--muted);text-transform:uppercase;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;margin-bottom:10px}
.card.goal{border-color:rgba(0,255,136,0.5);background:linear-gradient(135deg,rgba(0,255,136,0.04),var(--card))}
.card.trap{border-color:rgba(255,51,85,0.5);background:linear-gradient(135deg,rgba(255,51,85,0.04),var(--card))}
.card.hot{border-color:rgba(0,255,136,0.9);box-shadow:0 0 20px rgba(0,255,136,0.12);background:linear-gradient(135deg,rgba(0,255,136,0.07),var(--card))}
.card-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:8px}
.match-name{font-size:16px;font-weight:700}
.match-league{font-size:11px;color:var(--muted);margin-top:2px}
.badges{display:flex;gap:5px;align-items:center;flex-wrap:wrap}
.badge{padding:3px 8px;border-radius:5px;font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace}
.b-live{background:rgba(0,255,136,0.12);color:var(--green);border:1px solid rgba(0,255,136,0.3)}
.b-min{background:rgba(255,255,255,0.06);color:var(--text)}
.b-score{background:rgba(255,204,0,0.1);color:var(--yellow)}
.b-hot{background:rgba(0,255,136,0.2);color:var(--green);border:1px solid rgba(0,255,136,0.5);animation:pulse 1.5s infinite}
.b-pres{background:rgba(255,107,53,0.15);color:var(--orange);border:1px solid rgba(255,107,53,0.3)}
.b-pending{background:rgba(255,204,0,0.1);color:var(--yellow);border:1px solid rgba(255,204,0,0.3)}
.b-success{background:rgba(0,255,136,0.12);color:var(--green);border:1px solid rgba(0,255,136,0.3)}
.b-miss{background:rgba(255,51,85,0.12);color:var(--red);border:1px solid rgba(255,51,85,0.3)}
.odds-row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.odd-tag{background:var(--card2);border:1px solid var(--border2);border-radius:6px;padding:5px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;display:flex;flex-direction:column;align-items:center;gap:1px}
.odd-label{font-size:9px;color:var(--muted);letter-spacing:1px}
.odd-val{font-size:13px;font-weight:700}
.odd-tag.ht{border-color:rgba(68,136,255,0.3);background:rgba(68,136,255,0.06)}
.odd-tag.ht .odd-label{color:var(--blue)}
.pres-bar{height:4px;background:var(--border2);border-radius:2px;margin-bottom:10px;overflow:hidden}
.pres-fill{height:100%;border-radius:2px;transition:width 0.5s}
.verdict{padding:9px 14px;border-radius:8px;font-size:13px;font-weight:700;margin-bottom:8px}
.v-goal{background:rgba(0,255,136,0.12);color:var(--green);border:1px solid rgba(0,255,136,0.3)}
.v-trap{background:rgba(255,51,85,0.12);color:var(--red);border:1px solid rgba(255,51,85,0.3)}
.v-warn{background:rgba(255,204,0,0.1);color:var(--yellow);border:1px solid rgba(255,204,0,0.3)}
.ai-box{background:rgba(68,136,255,0.05);border:1px solid rgba(68,136,255,0.2);border-radius:8px;padding:12px;margin-top:10px;font-size:13px;line-height:1.7;color:#aaaacc}
.ai-label{font-size:10px;letter-spacing:2px;color:var(--blue);margin-bottom:6px;font-family:'JetBrains Mono',monospace}
.ot-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin-top:10px}
.ot-cell{background:var(--card2);border-radius:6px;padding:6px;text-align:center}
.ot-label{font-size:9px;color:var(--muted);letter-spacing:1px}
.ot-val{font-size:13px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--green);margin-top:2px}
.progress-bar{height:6px;background:var(--card2);border-radius:3px;overflow:hidden;margin:4px 0}
.progress-fill{height:100%;border-radius:3px;transition:width 0.5s}
.rule-card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:8px}
.rule-top{display:flex;justify-content:space-between;align-items:center;gap:8px;margin-bottom:6px}
.rule-name{font-size:14px;font-weight:600}
.toggle-btn{padding:4px 12px;border-radius:6px;font-size:12px;font-weight:700;cursor:pointer;border:none;font-family:'JetBrains Mono',monospace;transition:all 0.2s}
.toggle-on{background:rgba(0,255,136,0.15);color:var(--green);border:1px solid rgba(0,255,136,0.3)!important}
.toggle-off{background:rgba(255,255,255,0.05);color:var(--muted);border:1px solid var(--border)!important}
.ai-run-btn{background:rgba(136,85,255,0.1);border:1px solid rgba(136,85,255,0.3);color:var(--purple);border-radius:8px;padding:10px 20px;font-size:14px;font-family:'Inter',sans-serif;font-weight:600;cursor:pointer;transition:all 0.2s}
.ai-run-btn:hover{background:rgba(136,85,255,0.2)}
.ai-run-btn:disabled{opacity:0.5;cursor:not-allowed}
.insight-card{background:var(--card);border:1px solid rgba(136,85,255,0.2);border-radius:12px;padding:16px;margin-bottom:10px}
.insight-text{font-size:13px;line-height:1.7;color:#aaaacc;white-space:pre-line}
.empty{text-align:center;padding:60px 20px;color:var(--muted)}
.empty-icon{font-size:42px;margin-bottom:12px}
.live-dot{width:8px;height:8px;border-radius:50%;background:var(--green);animation:blink 1.2s infinite;display:inline-block;margin-right:6px}
.upd{font-size:11px;color:var(--muted);font-family:'JetBrains Mono',monospace}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:16px}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.2}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.6}}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:var(--bg)}::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
@media(max-width:900px){.sidebar{width:56px}.main{margin-left:56px}.sidebar .nav-item span,.logo-sub,.logo-main{display:none}.stats-row{grid-template-columns:repeat(2,1fr)}.two-col{grid-template-columns:1fr}.ot-grid{grid-template-columns:repeat(3,1fr)}}
</style>
</head>
<body>
<div class="sidebar">
  <div class="logo"><div class="logo-main">PAPA<span>GOAL</span></div><div class="logo-sub">READ THE MARKET</div></div>
  <nav class="nav">
    <button class="nav-item active" onclick="showPage('live',this)"><span>📡</span><span>Live Dashboard</span></button>
    <button class="nav-item" onclick="showPage('goals',this)"><span>⚽</span><span>Goals</span></button>
    <button class="nav-item" onclick="showPage('trades',this)"><span>📈</span><span>Simulation</span></button>
    <button class="nav-item" onclick="showPage('signals',this)"><span>🔥</span><span>Signals</span></button>
    <button class="nav-item" onclick="showPage('rules',this)"><span>📋</span><span>Rules Engine</span></button>
    <button class="nav-item" onclick="showPage('analytics',this)"><span>📊</span><span>Analytics</span></button>
    <button class="nav-item" onclick="showPage('ai',this)"><span>🤖</span><span>AI Insights</span></button>
  </nav>
</div>
<div class="main">

<!-- LIVE -->
<div class="page active" id="page-live">
  <div class="page-header">
    <div><div class="page-title"><span class="live-dot"></span>Live Dashboard</div><div class="page-sub">Don't predict football. Read the market.</div></div>
    <div class="upd" id="upd-live">Updating...</div>
  </div>
  <div class="stats-row">
    <div class="stat-card"><div class="stat-num" style="color:var(--blue)" id="sl-live">—</div><div class="stat-label">Live Matches</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--green)" id="sl-hot">—</div><div class="stat-label">HOT Signals</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--yellow)" id="sl-goals">—</div><div class="stat-label">Goals Today</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--purple)" id="sl-trades">—</div><div class="stat-label">Open Trades</div></div>
  </div>
  <div class="section-title">🎯 Active Signals – Live Matches Only</div>
  <div id="live-cards"><div class="empty"><div class="empty-icon">📡</div><div>Scanning live matches...</div></div></div>
</div>

<!-- GOALS -->
<div class="page" id="page-goals">
  <div class="page-header"><div><div class="page-title">⚽ Goals Detected</div><div class="page-sub">Odds before each goal – the core learning data</div></div></div>
  <div id="goals-list"><div class="empty"><div class="empty-icon">⚽</div><div>Loading goals...</div></div></div>
</div>

<!-- SIMULATION -->
<div class="page" id="page-trades">
  <div class="page-header"><div><div class="page-title">📈 Simulation</div><div class="page-sub">Paper trading – track signal accuracy in real time</div></div></div>
  <div id="trades-content"><div class="empty"><div class="empty-icon">📈</div><div>Loading trades...</div></div></div>
</div>

<!-- SIGNALS -->
<div class="page" id="page-signals">
  <div class="page-header"><div><div class="page-title">🔥 All Signals</div><div class="page-sub">Last 3 hours</div></div></div>
  <div id="signals-list"><div class="empty"><div class="empty-icon">🔥</div><div>Loading...</div></div></div>
</div>

<!-- RULES ENGINE -->
<div class="page" id="page-rules">
  <div class="page-header">
    <div><div class="page-title">📋 Rules Engine</div><div class="page-sub">Active rules, hit rates & AI suggestions</div></div>
    <button class="ai-run-btn" onclick="runAIRules()" id="ai-rules-btn">🤖 Ask AI to Improve Rules</button>
  </div>
  <div class="stats-row">
    <div class="stat-card"><div class="stat-num" style="color:var(--green)" id="r-active">—</div><div class="stat-label">Active Rules</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--blue)" id="r-strong">—</div><div class="stat-label">Strong Rules (>60%)</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--orange)" id="r-ai">—</div><div class="stat-label">AI Suggested</div></div>
    <div class="stat-card"><div class="stat-num" style="color:var(--yellow)" id="r-total">—</div><div class="stat-label">Total Signals</div></div>
  </div>
  <div id="rules-list"><div class="empty"><div class="empty-icon">📋</div><div>Loading rules...</div></div></div>
</div>

<!-- ANALYTICS -->
<div class="page" id="page-analytics">
  <div class="page-header"><div><div class="page-title">📊 Analytics</div><div class="page-sub">Historical pattern analysis</div></div></div>
  <div id="analytics-content"><div class="empty"><div class="empty-icon">📊</div><div>Loading...</div></div></div>
</div>

<!-- AI INSIGHTS -->
<div class="page" id="page-ai">
  <div class="page-header">
    <div><div class="page-title">🤖 AI Insights</div><div class="page-sub">Claude analyzes historical market patterns</div></div>
    <button class="ai-run-btn" onclick="runAI()" id="ai-btn">🤖 Run Analysis</button>
  </div>
  <div id="ai-content"><div class="empty"><div class="empty-icon">🤖</div><div>Click "Run Analysis" to get insights</div></div></div>
</div>

</div>
<script>
let currentPage='live';
const vc={'goal':'v-goal','no_goal':'v-trap','trap':'v-trap','warn':'v-warn'};
const mc={'goal':'goal','no_goal':'trap','trap':'trap'};
const ic={'goal':'🟢','no_goal':'🔴','trap':'🔴','warn':'🟡'};

function showPage(p,btn){
  document.querySelectorAll('.page').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(x=>x.classList.remove('active'));
  document.getElementById('page-'+p).classList.add('active');
  if(btn) btn.classList.add('active');
  currentPage=p;
  const fn={goals:loadGoals,trades:loadTrades,signals:loadSignals,rules:loadRules,analytics:loadAnalytics,ai:loadAI};
  if(fn[p]) fn[p]();
}

async function loadLive(){
  try{
    const[st,si,ai]=await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/signals').then(r=>r.json()),
      fetch('/api/ai_live').then(r=>r.json())
    ]);
    document.getElementById('sl-live').textContent=st.live||0;
    document.getElementById('sl-hot').textContent=st.hot_signals||0;
    document.getElementById('sl-goals').textContent=st.goals_today||0;
    document.getElementById('sl-trades').textContent=st.open_trades||0;
    document.getElementById('upd-live').textContent='Updated: '+new Date().toLocaleTimeString();
    const aiMap={};
    ai.forEach(a=>aiMap[a.match_id]=a.analysis);
    const el=document.getElementById('live-cards');
    if(!si.length){
      el.innerHTML='<div class="empty"><div class="empty-icon">✅</div><div style="font-size:15px;font-weight:700;margin-bottom:6px">No active signals</div><div style="font-size:12px">Watching '+(st.live||0)+' live matches</div></div>';
      return;
    }
    const byMatch={};
    si.forEach(s=>{
      if(!byMatch[s.match_id]) byMatch[s.match_id]={...s,rules:[]};
      if(!byMatch[s.match_id].rules.find(r=>r.rule_num===s.rule_num)) byMatch[s.match_id].rules.push(s);
    });
    el.innerHTML=Object.values(byMatch).map(m=>{
      const c=m.signal_type||'warn';
      const isHot=(m.pressure_score||0)>=60||m.rule_num>=100;
      const rules=m.rules.map(r=>'R'+r.rule_num+' '+r.rule_name).join(' · ');
      const pres=m.pressure_score||0;
      const presColor=pres>=70?'var(--green)':pres>=40?'var(--orange)':'var(--muted)';
      const ai=aiMap[m.match_id]?`<div class="ai-box"><div class="ai-label">🤖 CLAUDE AI ANALYSIS</div>${aiMap[m.match_id]}</div>`:'';
      return `<div class="card ${isHot?'hot':mc[c]||''}">
        <div class="card-top">
          <div><div class="match-name">${m.home_team} vs ${m.away_team}</div><div class="match-league">${m.league||''}</div></div>
          <div class="badges">
            ${m.minute>0?`<span class="badge b-min">⏱ ${m.minute}'</span>`:''}
            ${m.score&&m.score!='0-0'?`<span class="badge b-score">${m.score}</span>`:''}
            ${isHot?'<span class="badge b-hot">🔥 HOT</span>':''}
            <span class="badge b-live">LIVE</span>
            ${pres>0?`<span class="badge b-pres">${pres}% pressure</span>`:''}
          </div>
        </div>
        ${pres>0?`<div class="pres-bar"><div class="pres-fill" style="width:${pres}%;background:${presColor}"></div></div>`:''}
        <div class="odds-row">
          ${m.over_odd?`<div class="odd-tag"><div class="odd-label">OVER 2.5</div><div class="odd-val">${m.over_odd}</div></div>`:''}
          ${m.draw_odd?`<div class="odd-tag"><div class="odd-label">DRAW</div><div class="odd-val">${m.draw_odd}</div></div>`:''}
          ${m.over05ht_odd?`<div class="odd-tag ht"><div class="odd-label">HT OVER 0.5</div><div class="odd-val">${(+m.over05ht_odd).toFixed(2)}</div></div>`:''}
          ${m.over15ht_odd?`<div class="odd-tag ht"><div class="odd-label">HT OVER 1.5</div><div class="odd-val">${(+m.over15ht_odd).toFixed(2)}</div></div>`:''}
          ${m.held_seconds>0?`<div class="odd-tag"><div class="odd-label">HELD</div><div class="odd-val">${m.held_seconds}s</div></div>`:''}
        </div>
        <div class="verdict ${vc[c]||'v-warn'}">${ic[c]||'🟡'} ${m.verdict} · ${rules}</div>
        ${ai}
      </div>`;
    }).join('');
  }catch(e){console.error(e);}
}

async function loadGoals(){
  const goals=await fetch('/api/goals').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('goals-list');
  if(!goals.length){el.innerHTML='<div class="empty"><div class="empty-icon">⚽</div><div>No goals detected yet</div></div>';return;}
  el.innerHTML=goals.map(g=>{
    const getOdd=(obj,key)=>{if(!obj)return'—';const k=Object.keys(obj).find(k=>k.toLowerCase().includes(key));return k?(+obj[k]).toFixed(2):'—';};
    return `<div class="card" style="border-color:rgba(0,255,136,0.2)">
      <div class="card-top">
        <div><div class="match-name">${g.home_team||''} vs ${g.away_team||''}</div><div class="match-league">${g.league||''}</div></div>
        <div style="font-size:16px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--green)">⚽ Min ${g.minute}</div>
      </div>
      <div style="font-size:12px;color:var(--muted);margin-bottom:8px">${g.score_before||'?'} → ${g.score_after||'?'} ${g.had_snapshots?'✅ has snapshots':'⚠️ no snapshots yet'}</div>
      <div class="ot-grid">
        <div class="ot-cell"><div class="ot-label">10s before</div><div class="ot-val">${getOdd(g.odds_10s,'over')}</div></div>
        <div class="ot-cell"><div class="ot-label">30s before</div><div class="ot-val">${getOdd(g.odds_30s,'over')}</div></div>
        <div class="ot-cell"><div class="ot-label">60s before</div><div class="ot-val">${getOdd(g.odds_60s,'over')}</div></div>
        <div class="ot-cell"><div class="ot-label">2m before</div><div class="ot-val">${getOdd(g.odds_120s,'over')}</div></div>
        <div class="ot-cell"><div class="ot-label">5m before</div><div class="ot-val">${getOdd(g.odds_300s,'over')}</div></div>
      </div>
    </div>`;
  }).join('');
}

async function loadTrades(){
  const trades=await fetch('/api/trades').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('trades-content');
  const pending=trades.filter(t=>t.result==='pending');
  const success=trades.filter(t=>t.result==='success');
  const miss=trades.filter(t=>t.result==='miss');
  const total=success.length+miss.length;
  const pct=total>0?Math.round(success.length/total*100):0;
  const profit=trades.reduce((s,t)=>s+(t.profit_loss||0),0);
  el.innerHTML=`
    <div class="stats-row">
      <div class="stat-card"><div class="stat-num" style="color:var(--yellow)">${pending.length}</div><div class="stat-label">⏳ Pending</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--green)">${success.length}</div><div class="stat-label">✅ Hit</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--red)">${miss.length}</div><div class="stat-label">❌ Miss</div></div>
      <div class="stat-card"><div class="stat-num" style="color:${profit>=0?'var(--green)':'var(--red)'}">€${profit.toFixed(0)}</div><div class="stat-label">${pct}% Hit Rate</div></div>
    </div>
    <div class="section-title">All Trades (${trades.length})</div>
    ${!trades.length?'<div class="empty"><div class="empty-icon">📈</div><div>No trades yet – waiting for live signals</div></div>':
      trades.map(t=>{
        const statusBadge=t.result==='pending'?'<span class="badge b-pending">⏳ PENDING</span>':
          t.result==='success'?'<span class="badge b-success">✅ HIT</span>':
          '<span class="badge b-miss">❌ MISS</span>';
        return `<div class="card" style="border-color:${t.result==='pending'?'rgba(255,204,0,0.3)':t.result==='success'?'rgba(0,255,136,0.3)':'rgba(255,51,85,0.3)'}">
          <div class="card-top">
            <div>
              <div class="match-name">${t.home_team} vs ${t.away_team}</div>
              <div class="match-league">${t.league||''} · ${t.rule_name}</div>
            </div>
            <div class="badges">
              ${t.minute_entry>0?`<span class="badge b-min">⏱ ${t.minute_entry}'</span>`:''}
              ${statusBadge}
            </div>
          </div>
          <div class="odds-row">
            <div class="odd-tag"><div class="odd-label">ENTRY ODD</div><div class="odd-val" style="color:var(--yellow)">${t.entry_odd}</div></div>
            <div class="odd-tag"><div class="odd-label">VERDICT</div><div class="odd-val" style="font-size:11px">${t.verdict||'—'}</div></div>
            ${t.pressure_at_entry>0?`<div class="odd-tag"><div class="odd-label">PRESSURE</div><div class="odd-val">${t.pressure_at_entry}%</div></div>`:''}
            ${t.result!=='pending'?`<div class="odd-tag"><div class="odd-label">P/L</div><div class="odd-val" style="color:${t.profit_loss>=0?'var(--green)':'var(--red)'}">€${(t.profit_loss||0).toFixed(0)}</div></div>`:''}
          </div>
          ${t.result==='pending'?`<div style="font-size:12px;color:var(--muted)">Score at entry: ${t.score_entry||'?'} · Waiting for result...</div>`:''}
          ${t.notes?`<div style="font-size:12px;color:var(--muted);margin-top:4px">${t.notes}</div>`:''}
        </div>`;
      }).join('')}
  `;
}

async function loadSignals(){
  const sigs=await fetch('/api/all_signals').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('signals-list');
  if(!sigs.length){el.innerHTML='<div class="empty"><div class="empty-icon">🔥</div><div>No signals</div></div>';return;}
  el.innerHTML=sigs.map(s=>{
    const c=s.signal_type||'warn';
    const pres=s.pressure_score||0;
    return `<div class="card">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:8px">
        <div><div style="font-size:14px;font-weight:600">${s.home_team} vs ${s.away_team}</div>
        <div style="font-size:11px;color:var(--muted)">R${s.rule_num} · ${s.rule_name} · ${s.league||''}</div></div>
        <div class="badges">
          ${s.minute>0?`<span class="badge b-min">⏱ ${s.minute}'</span>`:''}
          <span class="verdict ${vc[c]||'v-warn'}" style="padding:4px 10px;font-size:12px">${ic[c]||'🟡'} ${s.verdict}</span>
        </div>
      </div>
      <div style="font-size:12px;color:var(--muted);margin-top:6px">${s.reason}</div>
      ${pres>0?`<div class="progress-bar" style="margin-top:8px"><div class="progress-fill" style="width:${pres}%;background:var(--green)"></div></div>
      <div style="font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace">${pres}% market pressure</div>`:''}
    </div>`;
  }).join('');
}

async function loadRules(){
  const rules=await fetch('/api/rules').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('rules-list');
  const active=rules.filter(r=>r.is_active).length;
  const strong=rules.filter(r=>(r.success_rate||0)>=60).length;
  const ai_sug=rules.filter(r=>r.source==='ai_suggested').length;
  const total_sigs=rules.reduce((s,r)=>s+(r.total_signals||0),0);
  document.getElementById('r-active').textContent=active;
  document.getElementById('r-strong').textContent=strong;
  document.getElementById('r-ai').textContent=ai_sug;
  document.getElementById('r-total').textContent=total_sigs;
  if(!rules.length){el.innerHTML='<div class="empty"><div class="empty-icon">📋</div><div>No rules found</div></div>';return;}
  const sorted=[...rules].sort((a,b)=>(b.total_signals||0)-(a.total_signals||0));
  el.innerHTML=sorted.map(r=>{
    const rate=r.success_rate||0;
    const rateColor=rate>=60?'var(--green)':rate>=40?'var(--yellow)':'var(--red)';
    const isAI=r.source==='ai_suggested';
    return `<div class="rule-card">
      <div class="rule-top">
        <div>
          <div class="rule-name">${isAI?'🤖 ':''}<span style="color:${r.is_active?'var(--text)':'var(--muted)'}">${r.rule_name}</span></div>
          <div style="font-size:11px;color:var(--muted);margin-top:2px">${r.description||''}</div>
        </div>
        <button class="toggle-btn ${r.is_active?'toggle-on':'toggle-off'}" onclick="toggleRule('${r.rule_name}',${!r.is_active})">
          ${r.is_active?'ON':'OFF'}
        </button>
      </div>
      <div style="display:flex;align-items:center;gap:16px;margin-top:8px">
        <div style="flex:1">
          <div class="progress-bar"><div class="progress-fill" style="width:${rate}%;background:${rateColor}"></div></div>
        </div>
        <span style="font-size:12px;font-family:'JetBrains Mono',monospace;color:${rateColor};width:36px;text-align:right">${rate}%</span>
        <span style="font-size:11px;color:var(--muted)">${r.total_signals||0} signals</span>
        <span style="font-size:11px;color:var(--green)">✅ ${r.success_count||0}</span>
        <span style="font-size:11px;color:var(--red)">❌ ${r.fail_count||0}</span>
      </div>
      <div style="font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:6px">
        action: ${r.action} · source: ${r.source}
      </div>
    </div>`;
  }).join('');
}

async function toggleRule(name, newState){
  try{
    await fetch('/api/rules/toggle',{method:'POST',headers:{'Content-Type':'application/json'},
      body:JSON.stringify({rule_name:name,is_active:newState})});
    loadRules();
  }catch(e){console.error(e);}
}

async function runAIRules(){
  const btn=document.getElementById('ai-rules-btn');
  btn.disabled=true;btn.textContent='🤖 Analyzing...';
  try{
    const r=await fetch('/api/ai_improve_rules',{method:'POST'});
    const d=await r.json();
    if(d.error) alert('Error: '+d.error);
    else{alert('AI analysis complete! Check Rules Engine for new suggestions.');loadRules();}
  }catch(e){alert('Error running AI analysis');}
  btn.disabled=false;btn.textContent='🤖 Ask AI to Improve Rules';
}

async function loadAnalytics(){
  const data=await fetch('/api/analytics').then(r=>r.json()).catch(()=>({}));
  const el=document.getElementById('analytics-content');
  const targets=[
    {l:"Goals collected",v:data.total_goals,t:500,c:"var(--green)"},
    {l:"Signals collected",v:data.total_signals,t:2000,c:"var(--blue)"},
    {l:"Snapshots saved",v:data.total_snapshots,t:50000,c:"var(--yellow)"},
    {l:"Paper trades",v:data.total_trades,t:200,c:"var(--purple)"}
  ];
  el.innerHTML=`
    <div class="stats-row">
      <div class="stat-card"><div class="stat-num" style="color:var(--green)">${data.total_goals||0}</div><div class="stat-label">Goals</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--blue)">${data.total_signals||0}</div><div class="stat-label">Signals</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--yellow)">${(data.total_snapshots||0).toLocaleString()}</div><div class="stat-label">Snapshots</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--orange)">${data.success_rate||0}%</div><div class="stat-label">Hit Rate</div></div>
    </div>
    <div class="two-col">
      <div class="card">
        <div class="section-title">Data Collection Progress</div>
        ${targets.map(t=>`
          <div style="display:flex;justify-content:space-between;margin-top:12px;font-size:12px">
            <span style="color:var(--muted)">${t.l}</span>
            <span style="color:${t.c};font-family:'JetBrains Mono',monospace">${t.v||0} / ${t.t}</span>
          </div>
          <div class="progress-bar"><div class="progress-fill" style="width:${Math.min(100,(t.v||0)/t.t*100)}%;background:${t.c}"></div></div>
        `).join('')}
      </div>
      <div class="card">
        <div class="section-title">Top Rules by Signal Count</div>
        ${(data.top_rules||[]).map(r=>`
          <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px">
            <span>R${r.rule_num} ${r.rule_name}</span>
            <span style="color:var(--green);font-family:'JetBrains Mono',monospace">${r.cnt}</span>
          </div>`).join('')}
      </div>
    </div>
  `;
}

async function loadAI(){
  const ins=await fetch('/api/insights').then(r=>r.json()).catch(()=>[]);
  const el=document.getElementById('ai-content');
  if(!ins.length){el.innerHTML='<div class="empty"><div class="empty-icon">🤖</div><div>Click "Run Analysis" to get insights</div></div>';return;}
  el.innerHTML=ins.map(i=>`<div class="insight-card">
    <div style="font-size:13px;font-weight:700;color:var(--purple);margin-bottom:4px">🧠 Market Analysis</div>
    <div style="font-size:11px;color:var(--muted);margin-bottom:10px;font-family:'JetBrains Mono',monospace">${new Date(i.created_at).toLocaleString()} · ${i.goals_analyzed||0} goals analyzed</div>
    <div class="insight-text">${i.content}</div>
  </div>`).join('');
}

async function runAI(){
  const btn=document.getElementById('ai-btn');
  btn.disabled=true;btn.textContent='⏳ Analyzing...';
  try{
    const r=await fetch('/api/run_ai',{method:'POST'});
    const d=await r.json();
    if(d.error) btn.textContent='❌ '+d.error;
    else{await loadAI();btn.textContent='✅ Done';}
  }catch(e){btn.textContent='❌ Error';}
  setTimeout(()=>{btn.disabled=false;btn.textContent='🤖 Run Analysis';},3000);
}

async function autoRefresh(){if(currentPage==='live') await loadLive();}
loadLive();
setInterval(autoRefresh,20000);
</script>
</body>
</html>"""

# ─── API Routes ───────────────────────────────────────────────────────────────
@app.route("/")
def index(): return render_template_string(HTML)

@app.route("/api/stats")
def api_stats():
    try:
        conn=get_db()
        try:
            r1=conn.run("SELECT COUNT(DISTINCT match_id) FROM odds_snapshots WHERE captured_at>NOW()-INTERVAL '1 hour' AND is_live=TRUE")
            r2=conn.run("SELECT COUNT(*) FROM signals WHERE detected_at>NOW()-INTERVAL '30 minutes' AND signal_type='goal' AND confidence>=75")
            r3=conn.run("SELECT COUNT(*) FROM goals WHERE goal_time>NOW()-INTERVAL '24 hours'")
            r4=conn.run("SELECT COUNT(*) FROM paper_trades WHERE result='pending'")
            return jsonify({"live":r1[0][0],"hot_signals":r2[0][0],"goals_today":r3[0][0],"open_trades":r4[0][0]})
        finally: conn.close()
    except: return jsonify({"live":0,"hot_signals":0,"goals_today":0,"open_trades":0})

@app.route("/api/signals")
def api_signals():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT DISTINCT ON (match_id,rule_num)
                match_id,home_team,away_team,league,rule_num,rule_name,minute,score,
                signal_type,verdict,confidence,reason,over_odd,draw_odd,
                over05ht_odd,over15ht_odd,pressure_score,held_seconds,direction,detected_at
                FROM signals WHERE detected_at>NOW()-INTERVAL '30 minutes'
                ORDER BY match_id,rule_num,detected_at DESC LIMIT 40""")
            cols=["match_id","home_team","away_team","league","rule_num","rule_name","minute","score","signal_type","verdict","confidence","reason","over_odd","draw_odd","over05ht_odd","over15ht_odd","pressure_score","held_seconds","direction","detected_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["detected_at"]=str(r["detected_at"])
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/all_signals")
def api_all_signals():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT match_id,home_team,away_team,league,rule_num,rule_name,minute,score,
                signal_type,verdict,confidence,reason,over_odd,draw_odd,pressure_score,held_seconds,direction,detected_at
                FROM signals WHERE detected_at>NOW()-INTERVAL '3 hours'
                ORDER BY detected_at DESC LIMIT 100""")
            cols=["match_id","home_team","away_team","league","rule_num","rule_name","minute","score","signal_type","verdict","confidence","reason","over_odd","draw_odd","pressure_score","held_seconds","direction","detected_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["detected_at"]=str(r["detected_at"])
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/goals")
def api_goals():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT g.match_id,g.minute,g.score_before,g.score_after,g.had_snapshots,
                g.odds_10s,g.odds_30s,g.odds_60s,g.odds_120s,g.odds_300s,g.goal_time,
                m.home_team,m.away_team,m.league
                FROM goals g LEFT JOIN matches m ON g.match_id=m.match_id
                ORDER BY g.goal_time DESC LIMIT 50""")
            result=[]
            for r in rows:
                result.append({"match_id":r[0],"minute":r[1],"score_before":r[2],"score_after":r[3],
                               "had_snapshots":r[4],"odds_10s":r[5]or{},"odds_30s":r[6]or{},
                               "odds_60s":r[7]or{},"odds_120s":r[8]or{},"odds_300s":r[9]or{},
                               "goal_time":str(r[10]),"home_team":r[11]or"","away_team":r[12]or"","league":r[13]or""})
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/trades")
def api_trades():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT home_team,away_team,league,entry_odd,verdict,rule_name,
                minute_entry,score_entry,pressure_at_entry,result,profit_loss,notes,created_at
                FROM paper_trades ORDER BY created_at DESC LIMIT 100""")
            cols=["home_team","away_team","league","entry_odd","verdict","rule_name","minute_entry","score_entry","pressure_at_entry","result","profit_loss","notes","created_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["created_at"]=str(r["created_at"])
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/rules")
def api_rules():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT rule_name,description,action,source,is_active,
                total_signals,success_count,fail_count,success_rate,created_at
                FROM rules ORDER BY total_signals DESC""")
            cols=["rule_name","description","action","source","is_active","total_signals","success_count","fail_count","success_rate","created_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["created_at"]=str(r["created_at"])
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/rules/toggle", methods=["POST"])
def api_rules_toggle():
    try:
        data=request.json
        conn=get_db()
        try:
            conn.run("UPDATE rules SET is_active=:a,last_updated=NOW() WHERE rule_name=:b",
                a=data["is_active"],b=data["rule_name"])
            return jsonify({"status":"ok"})
        finally: conn.close()
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/api/ai_live")
def api_ai_live():
    try:
        conn=get_db()
        try:
            rows=conn.run("""SELECT content FROM ai_insights
                WHERE insight_type='live_signal' AND created_at>NOW()-INTERVAL '30 minutes'
                ORDER BY created_at DESC LIMIT 30""")
            seen={}
            for r in rows:
                parts=(r[0]or"").split("|||",1)
                if len(parts)==2 and parts[0] not in seen:
                    seen[parts[0]]={"match_id":parts[0],"analysis":parts[1]}
            return jsonify(list(seen.values()))
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/analytics")
def api_analytics():
    try:
        conn=get_db()
        try:
            r1=conn.run("SELECT COUNT(*) FROM goals")[0][0]
            r2=conn.run("SELECT COUNT(*) FROM signals")[0][0]
            r3=conn.run("SELECT COUNT(*) FROM odds_snapshots")[0][0]
            r4=conn.run("SELECT COUNT(*) FROM paper_trades")[0][0]
            success=conn.run("SELECT COUNT(*) FROM paper_trades WHERE result='success'")[0][0]
            total_res=conn.run("SELECT COUNT(*) FROM paper_trades WHERE result!='pending'")[0][0]
            rate=round(success/total_res*100) if total_res>0 else 0
            top=conn.run("SELECT rule_num,rule_name,COUNT(*) as cnt FROM signals GROUP BY rule_num,rule_name ORDER BY cnt DESC LIMIT 10")
            return jsonify({"total_goals":r1,"total_signals":r2,"total_snapshots":r3,"total_trades":r4,
                           "success_rate":rate,"top_rules":[{"rule_num":r[0],"rule_name":r[1],"cnt":r[2]} for r in top]})
        finally: conn.close()
    except: return jsonify({"total_goals":0,"total_signals":0,"total_snapshots":0,"total_trades":0,"success_rate":0})

@app.route("/api/insights")
def api_insights():
    try:
        conn=get_db()
        try:
            rows=conn.run("SELECT insight_type,content,goals_analyzed,created_at FROM ai_insights WHERE insight_type='market_analysis' ORDER BY created_at DESC LIMIT 10")
            return jsonify([{"insight_type":r[0],"content":r[1],"goals_analyzed":r[2],"created_at":str(r[3])} for r in rows])
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/run_ai", methods=["POST"])
def api_run_ai():
    if not ANTHROPIC_API_KEY: return jsonify({"error":"No Anthropic API key"}),400
    try:
        conn=get_db()
        try:
            goals=conn.run("""SELECT g.minute,g.score_before,g.odds_30s,g.odds_60s,m.league,m.home_team,m.away_team,g.had_snapshots
                FROM goals g LEFT JOIN matches m ON g.match_id=m.match_id ORDER BY g.goal_time DESC LIMIT 200""")
            sigs=conn.run("SELECT rule_num,rule_name,COUNT(*) as cnt,AVG(confidence) as c,AVG(pressure_score) as p FROM signals GROUP BY rule_num,rule_name ORDER BY cnt DESC")
            trades=conn.run("SELECT result,COUNT(*) FROM paper_trades WHERE result!='pending' GROUP BY result")
            snaps=conn.run("SELECT COUNT(*) FROM odds_snapshots")[0][0]
            goals_txt=f"Total {len(goals)} goals ({sum(1 for g in goals if g[7])} with snapshots)\n"
            for g in goals[:30]:
                o30=g[2]or{}; over30=next((v for k,v in o30.items() if 'over' in str(k).lower()),None)
                goals_txt+=f"Min {g[0]} | {g[1]} | Over 30s before: {over30 or '?'} | {g[4] or ''}\n"
            rules_txt="Rules performance:\n"+"\n".join([f"R{s[0]} {s[1]}: {s[2]}x | conf:{round(s[3]or 0)}% | pressure:{round(s[4]or 0)}%" for s in sigs])
            trades_txt=", ".join([f"{t[0]}: {t[1]}" for t in trades])
            prompt=f"""You are PapaGoal AI – a professional betting market analyst.

Data:
- {snaps:,} total snapshots
- {len(goals)} goals recorded
- Paper Trading: {trades_txt}

{goals_txt}

{rules_txt}

Answer in English with detail:
1. Which odds patterns appeared most before goals? (averages and patterns)
2. What is the average market pressure before a goal?
3. Which rules are performing best?
4. New patterns you discovered?
5. What is the 80% entry point – at which odds should we enter?
6. Which new rule do you recommend adding?
7. What does the Paper Trading hit rate tell us?"""
            resp=requests.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-sonnet-4-20250514","max_tokens":1500,"messages":[{"role":"user","content":prompt}]},timeout=30)
            if resp.status_code==200:
                analysis=resp.json()["content"][0]["text"]
                conn.run("INSERT INTO ai_insights (insight_type,content,goals_analyzed) VALUES ('market_analysis',:a,:b)",a=analysis,b=len(goals))
                return jsonify({"status":"ok","analysis":analysis})
            else: return jsonify({"error":f"Claude: {resp.status_code}"}),500
        finally: conn.close()
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/api/ai_improve_rules", methods=["POST"])
def api_ai_improve_rules():
    if not ANTHROPIC_API_KEY: return jsonify({"error":"No Anthropic API key"}),400
    try:
        conn=get_db()
        try:
            goals=conn.run("""SELECT g.minute,g.odds_30s,g.odds_60s,g.odds_120s,m.league
                FROM goals g LEFT JOIN matches m ON g.match_id=m.match_id
                ORDER BY g.goal_time DESC LIMIT 200""")
            rules=conn.run("SELECT rule_name,action,total_signals,success_count,fail_count,success_rate FROM rules ORDER BY total_signals DESC")
            trades=conn.run("""SELECT rule_name,COUNT(*) total,SUM(CASE WHEN result='success' THEN 1 ELSE 0 END) hits
                FROM paper_trades WHERE result!='pending' GROUP BY rule_name ORDER BY total DESC""")

            goals_txt=f"Goals ({len(goals)} total):\n"
            for g in goals[:50]:
                o30=g[1]or{}; over30=next((v for k,v in o30.items() if 'over' in str(k).lower()),None)
                o60=g[2]or{}; over60=next((v for k,v in o60.items() if 'over' in str(k).lower()),None)
                goals_txt+=f"min {g[0]}: over30s={over30 or '?'} over60s={over60 or '?'} league={g[4] or '?'}\n"

            rules_txt="Current rules:\n"+"\n".join([f"{r[0]}: {r[2]} signals, {r[5] or 0}% success" for r in rules])
            trades_txt="Trade performance by rule:\n"+"\n".join([f"{t[0]}: {t[2]}/{t[1]} ({round(t[2]/t[1]*100) if t[1]>0 else 0}%)" for t in trades])

            prompt=f"""You are PapaGoal AI. Analyze this betting market data and suggest rule improvements.

{goals_txt}

{rules_txt}

{trades_txt}

Based on this data, provide in JSON format:
{{
  "disable_rules": ["rule names that are underperforming"],
  "new_rules": [
    {{
      "rule_name": "unique_name",
      "description": "clear description",
      "action": "GOAL or NO_GOAL or TRAP",
      "conditions": "exact conditions as text",
      "confidence": 65
    }}
  ],
  "insights": "2-3 sentences about what you found"
}}

Only suggest rules with clear statistical backing from the data."""

            resp=requests.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":ANTHROPIC_API_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-sonnet-4-20250514","max_tokens":1000,"messages":[{"role":"user","content":prompt}]},timeout=30)

            if resp.status_code==200:
                text=resp.json()["content"][0]["text"]
                # Parse JSON from response
                import re
                json_match=re.search(r'\{.*\}',text,re.DOTALL)
                if json_match:
                    try:
                        data=json.loads(json_match.group())
                        # Add new AI-suggested rules
                        for nr in data.get("new_rules",[]):
                            try:
                                conn.run("""INSERT INTO rules (rule_name,description,action,source,is_active)
                                    VALUES (:a,:b,:c,'ai_suggested',TRUE)
                                    ON CONFLICT (rule_name) DO NOTHING""",
                                    a=nr["rule_name"],b=nr.get("description",""),c=nr.get("action","GOAL"))
                            except: pass
                        # Save insight
                        conn.run("INSERT INTO ai_insights (insight_type,content,goals_analyzed) VALUES ('rule_improvement',:a,:b)",
                            a=data.get("insights","AI analysis complete"),b=len(goals))
                        return jsonify({"status":"ok","new_rules":len(data.get("new_rules",[])),"insights":data.get("insights","")})
                    except: pass
                return jsonify({"status":"ok","message":"Analysis complete"})
            else: return jsonify({"error":f"Claude: {resp.status_code}"}),500
        finally: conn.close()
    except Exception as e: return jsonify({"error":str(e)}),500

@app.route("/health")
def health():
    return jsonify({"status":"ok","version":"v4","betfair":bool(betfair_session.get("token")),"time":datetime.now(timezone.utc).isoformat()})

# ─── Start ────────────────────────────────────────────────────────────────────
init_db()
threading.Thread(target=collector_loop,daemon=True).start()
log.info("🚀 PapaGoal v4 started")

if __name__=="__main__":
    app.run(host="0.0.0.0",port=PORT,debug=False)
