import os, time, json, logging, threading
from datetime import datetime, timezone, timedelta
from urllib.parse import urlparse
from flask import Flask, jsonify, render_template_string, request
import pg8000.native
import requests
import re
import unicodedata
from difflib import SequenceMatcher

# ─── Config ───────────────────────────────────────────────────────────────────
ODDS_API_KEY      = os.environ.get("ODDS_API_KEY", "")  # legacy The Odds API
ODDSPAPI_KEY       = os.environ.get("ODDSPAPI_KEY", "")
USE_ODDSPAPI       = os.environ.get("USE_ODDSPAPI", "true").lower() == "true"
ODDSPAPI_BOOKMAKER = os.environ.get("ODDSPAPI_BOOKMAKER", "Bet365")
FOOTBALL_API_KEY  = os.environ.get("FOOTBALL_API_KEY", "")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
OPENAI_API_KEY    = os.environ.get("OPENAI_API_KEY", "")
OPENAI_MODEL      = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
DATABASE_URL      = os.environ.get("DATABASE_URL", "")
BETFAIR_APP_KEY   = os.environ.get("BETFAIR_APP_KEY", "")
BETFAIR_USERNAME  = os.environ.get("BETFAIR_USERNAME", "")
BETFAIR_PASSWORD  = os.environ.get("BETFAIR_PASSWORD", "")
# Betfair is disabled by default to keep the collector fast and clean.
# To enable it later, set USE_BETFAIR=true in Railway environment variables.
USE_BETFAIR       = os.environ.get("USE_BETFAIR", "false").lower() == "true"
PORT              = int(os.environ.get("PORT", 8080))
POLL_INTERVAL     = 30

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("papagoal")
app = Flask(__name__)

# ─── Expected Odds Curve ──────────────────────────────────────────────────────
# What Over 0.5 HT "should" be at each minute if no goal yet
# Based on typical market behavior
EXPECTED_OVER05_HT = {
    0: 1.25, 5: 1.28, 10: 1.32, 15: 1.38, 20: 1.45,
    25: 1.55, 30: 1.68, 35: 1.85, 40: 2.10, 45: 2.50
}
EXPECTED_OVER15_HT = {
    0: 2.10, 5: 2.15, 10: 2.22, 15: 2.32, 20: 2.45,
    25: 2.65, 30: 2.90, 35: 3.20, 40: 3.60, 45: 4.20
}
EXPECTED_OVER25 = {
    0: 1.85, 5: 1.88, 10: 1.92, 15: 1.98, 20: 2.05,
    25: 2.15, 30: 2.28, 35: 2.45, 40: 2.65, 45: 2.90,
    50: 3.10, 55: 3.35, 60: 3.65, 65: 4.00, 70: 4.50,
    75: 5.20, 80: 6.50, 85: 9.00, 88: 12.0, 90: 20.0
}

def get_expected(curve, minute):
    keys = sorted(curve.keys())
    for i, k in enumerate(keys):
        if minute <= k:
            if i == 0: return curve[k]
            prev_k = keys[i-1]
            ratio = (minute - prev_k) / (k - prev_k)
            return curve[prev_k] + ratio * (curve[k] - curve[prev_k])
    return curve[keys[-1]]

def pressure_score(current_odd, opening_odd, minute, market="over25"):
    """
    How much pressure is the market showing?
    Higher = more likely a goal is coming
    """
    if not opening_odd or not current_odd:
        return 0
    # How much has it risen vs opening
    rise_ratio = current_odd / opening_odd
    # What's expected at this minute
    if market == "over05ht":
        expected = get_expected(EXPECTED_OVER05_HT, min(minute, 45))
        expected_ratio = expected / opening_odd
    elif market == "over15ht":
        expected = get_expected(EXPECTED_OVER15_HT, min(minute, 45))
        expected_ratio = expected / opening_odd
    else:
        expected = get_expected(EXPECTED_OVER25, minute)
        expected_ratio = expected / opening_odd
    # If current rise < expected rise = pressure (goal likely)
    if expected_ratio > 0:
        score = max(0, min(100, int((1 - rise_ratio / expected_ratio) * 100)))
        return score
    return 0

# ─── DB ───────────────────────────────────────────────────────────────────────
def parse_db(url):
    p = urlparse(url)
    return {"host": p.hostname, "port": p.port or 5432,
            "database": p.path.lstrip("/"),
            "user": p.username, "password": p.password, "ssl_context": True}

def get_db():
    return pg8000.native.Connection(**parse_db(DATABASE_URL))

def init_db():
    conn = get_db()
    try:
        conn.run("""CREATE TABLE IF NOT EXISTS matches (
            id SERIAL PRIMARY KEY, match_id TEXT UNIQUE,
            league TEXT, home_team TEXT, away_team TEXT,
            minute INT DEFAULT 0, score_home INT DEFAULT 0, score_away INT DEFAULT 0,
            status TEXT DEFAULT 'upcoming',
            betfair_market_id TEXT,
            last_updated TIMESTAMPTZ DEFAULT NOW()
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS odds_snapshots (
            id SERIAL PRIMARY KEY, match_id TEXT,
            captured_at TIMESTAMPTZ DEFAULT NOW(),
            minute INT DEFAULT 0,
            score_home INT DEFAULT 0, score_away INT DEFAULT 0,
            market TEXT, outcome TEXT,
            odd_value FLOAT, prev_odd FLOAT,
            opening_odd FLOAT,
            odd_change FLOAT DEFAULT 0,
            direction TEXT DEFAULT 'stable',
            held_seconds INT DEFAULT 0,
            pressure INT DEFAULT 0,
            expected_odd FLOAT,
            is_live BOOLEAN DEFAULT FALSE,
            source TEXT DEFAULT 'odds_api',
            goal_30s BOOLEAN DEFAULT FALSE,
            goal_60s BOOLEAN DEFAULT FALSE,
            goal_120s BOOLEAN DEFAULT FALSE,
            goal_300s BOOLEAN DEFAULT FALSE
        )""")
        conn.run("CREATE INDEX IF NOT EXISTS idx_os_match ON odds_snapshots(match_id)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_os_time ON odds_snapshots(captured_at)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_os_market ON odds_snapshots(market)")
        conn.run("""CREATE TABLE IF NOT EXISTS opening_odds (
            id SERIAL PRIMARY KEY,
            match_id TEXT,
            recorded_at TIMESTAMPTZ DEFAULT NOW(),
            market TEXT,
            odd_value FLOAT,
            UNIQUE(match_id, market)
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS goals (
            id SERIAL PRIMARY KEY, match_id TEXT,
            minute INT, score_before TEXT, score_after TEXT,
            recorded_at TIMESTAMPTZ DEFAULT NOW(),
            auto_detected BOOLEAN DEFAULT TRUE,
            odds_10s JSONB DEFAULT '{}',
            odds_30s JSONB DEFAULT '{}',
            odds_60s JSONB DEFAULT '{}',
            odds_120s JSONB DEFAULT '{}',
            odds_300s JSONB DEFAULT '{}'
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS signals (
            id SERIAL PRIMARY KEY, match_id TEXT,
            detected_at TIMESTAMPTZ DEFAULT NOW(),
            home_team TEXT, away_team TEXT, league TEXT,
            rule_num INT, rule_name TEXT,
            minute INT DEFAULT 0, score TEXT DEFAULT '0-0',
            signal_type TEXT, verdict TEXT, confidence INT,
            reason TEXT,
            over_odd FLOAT, draw_odd FLOAT,
            over05ht_odd FLOAT, over15ht_odd FLOAT,
            opening_over05ht FLOAT, opening_over15ht FLOAT,
            pressure_score INT DEFAULT 0,
            held_seconds INT DEFAULT 0,
            direction TEXT DEFAULT 'stable',
            odd_change FLOAT DEFAULT 0
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS ai_insights (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            insight_type TEXT, content TEXT,
            goals_analyzed INT DEFAULT 0
        )""")
        conn.run("""CREATE TABLE IF NOT EXISTS ai_rule_candidates (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            source TEXT DEFAULT 'openai',
            rule_name TEXT,
            description TEXT,
            conditions_json JSONB DEFAULT '{}',
            expected_outcome TEXT DEFAULT 'goal',
            status TEXT DEFAULT 'candidate',
            active BOOLEAN DEFAULT FALSE,
            total_cases INT DEFAULT 0,
            goals_2m INT DEFAULT 0,
            goals_5m INT DEFAULT 0,
            goals_10m INT DEFAULT 0,
            success_rate_10m FLOAT DEFAULT 0,
            confidence_level TEXT DEFAULT 'low',
            promotion_reason TEXT,
            UNIQUE(rule_name)
        )""")
        conn.run("CREATE INDEX IF NOT EXISTS idx_ai_rule_candidates_status ON ai_rule_candidates(status, active)")
        conn.run("""CREATE TABLE IF NOT EXISTS simulated_bets (
            id SERIAL PRIMARY KEY,
            signal_id INT UNIQUE,
            match_id TEXT,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            resolved_at TIMESTAMPTZ,
            home_team TEXT, away_team TEXT, league TEXT,
            minute INT DEFAULT 0, score TEXT DEFAULT '0-0',
            action_type TEXT,
            market TEXT DEFAULT 'Over/No Goal',
            entry_odd FLOAT,
            virtual_stake FLOAT DEFAULT 100,
            virtual_profit FLOAT DEFAULT 0,
            confidence INT DEFAULT 0,
            reason TEXT,
            status TEXT DEFAULT 'open',
            result TEXT,
            goal_within_10m BOOLEAN,
            validated BOOLEAN DEFAULT FALSE
        )""")
        conn.run("CREATE INDEX IF NOT EXISTS idx_simulated_bets_status ON simulated_bets(status, validated)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_simulated_bets_match ON simulated_bets(match_id, created_at)")

        # --- Validation columns for learning engine ---
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS checked_2m BOOLEAN DEFAULT FALSE")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS checked_5m BOOLEAN DEFAULT FALSE")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS checked_10m BOOLEAN DEFAULT FALSE")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS goal_2m BOOLEAN")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS goal_5m BOOLEAN")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS goal_10m BOOLEAN")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS validated BOOLEAN DEFAULT FALSE")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS false_positive BOOLEAN DEFAULT FALSE")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS failure_reason TEXT")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS pattern_id TEXT")
        conn.run("ALTER TABLE signals ADD COLUMN IF NOT EXISTS validation_updated_at TIMESTAMPTZ")

        conn.run("""CREATE TABLE IF NOT EXISTS pattern_stats (
            id SERIAL PRIMARY KEY,
            pattern_id TEXT UNIQUE,
            rule_num INT,
            rule_name TEXT,
            minute_bucket TEXT,
            odds_bucket TEXT,
            pressure_bucket TEXT,
            duration_bucket TEXT,
            total_cases INT DEFAULT 0,
            goals_2m INT DEFAULT 0,
            goals_5m INT DEFAULT 0,
            goals_10m INT DEFAULT 0,
            no_goal_cases INT DEFAULT 0,
            false_positive_cases INT DEFAULT 0,
            success_rate_2m FLOAT DEFAULT 0,
            success_rate_5m FLOAT DEFAULT 0,
            success_rate_10m FLOAT DEFAULT 0,
            confidence_level TEXT DEFAULT 'low',
            last_updated TIMESTAMPTZ DEFAULT NOW()
        )""")
        conn.run("CREATE INDEX IF NOT EXISTS idx_pattern_stats_pid ON pattern_stats(pattern_id)")
        conn.run("CREATE INDEX IF NOT EXISTS idx_signals_validation ON signals(validated, detected_at)")
        log.info("✅ DB ready")
    except Exception as e:
        log.error(f"DB init: {e}")
    finally:
        conn.close()

# ─── Betfair Auth ─────────────────────────────────────────────────────────────
betfair_session = {"token": None, "expires": 0}

def betfair_login():
    try:
        # Try primary endpoint first
        for url in [
            "https://identitysso-cert.betfair.com/api/login",
            "https://identitysso.betfair.com/api/login"
        ]:
            try:
                resp = requests.post(
                    url,
                    data={"username": BETFAIR_USERNAME, "password": BETFAIR_PASSWORD},
                    headers={"X-Application": BETFAIR_APP_KEY,
                            "Content-Type": "application/x-www-form-urlencoded",
                            "Accept": "application/json"},
                    timeout=10
                )
                if resp.text:
                    data = resp.json()
                    if data.get("status") == "SUCCESS":
                        betfair_session["token"] = data["token"]
                        betfair_session["expires"] = time.time() + 3600
                        log.info("✅ Betfair logged in")
                        return True
                    else:
                        log.warning(f"Betfair login failed: {data}")
            except Exception as e:
                log.warning(f"Betfair endpoint {url} failed: {e}")
                continue
        return False
    except Exception as e:
        log.error(f"Betfair login error: {e}")
        return False

def get_betfair_token():
    if betfair_session["token"] and time.time() < betfair_session["expires"] - 60:
        return betfair_session["token"]
    betfair_login()
    return betfair_session["token"]

def betfair_request(endpoint, body):
    token = get_betfair_token()
    if not token:
        return None
    try:
        resp = requests.post(
            f"https://api.betfair.com/exchange/betting/json-rpc/v1",
            headers={
                "X-Application": BETFAIR_APP_KEY,
                "X-Authentication": token,
                "Content-Type": "application/json"
            },
            json=[{"jsonrpc": "2.0", "method": f"SportsAPING/v1.0/{endpoint}", "params": body, "id": 1}],
            timeout=10
        )
        result = resp.json()
        if result and isinstance(result, list):
            return result[0].get("result")
        return None
    except Exception as e:
        log.error(f"Betfair request error: {e}")
        return None

def get_betfair_live_markets():
    """Get live football markets from Betfair including HT markets"""
    result = betfair_request("listMarketCatalogue", {
        "filter": {
            "eventTypeIds": ["1"],  # Football
            "inPlayOnly": True,
            "marketTypeCodes": ["OVER_UNDER_05", "OVER_UNDER_15", "OVER_UNDER_25",
                               "HALF_TIME_SCORE", "NEXT_GOAL"]
        },
        "marketProjection": ["EVENT", "MARKET_TYPE", "COMPETITION"],
        "maxResults": 200
    })
    return result or []

def get_betfair_odds(market_ids):
    """Get current odds for given market IDs"""
    if not market_ids:
        return []
    result = betfair_request("listMarketBook", {
        "marketIds": market_ids[:10],
        "priceProjection": {"priceData": ["EX_BEST_OFFERS"]},
        "orderProjection": "EXECUTABLE"
    })
    return result or []

# ─── Rules Engine ─────────────────────────────────────────────────────────────
def run_rules(match_id, home, away, league, over, draw, hw, aw,
              over05ht, over15ht, opening_over05ht, opening_over15ht,
              minute, held, direction, change, pressure):
    signals = []
    o   = over or 0
    d   = draw or 0
    m   = minute or 0
    p   = pressure or 0
    o05 = over05ht or 0
    o15 = over15ht or 0
    op05 = opening_over05ht or 0
    op15 = opening_over15ht or 0

    def add(num, name, stype, verdict, conf, reason):
        signals.append({"rule_num": num, "rule_name": name,
                        "signal_type": stype, "verdict": verdict,
                        "confidence": conf, "reason": reason})

    # ── HT Rules (מחצית ראשונה) ────────────────────────────────────────────
    if o05 > 0 and op05 > 0 and m <= 45:
        rise = o05 / op05
        exp  = get_expected(EXPECTED_OVER05_HT, m) / op05
        if rise < exp * 0.85 and m >= 20:
            conf = min(90, int(70 + p * 0.2))
            add(101, "HT Pressure – Over 0.5 HT",
                "goal", f"GOAL ENTRY – Over 0.5 HT @ {o05:.2f}",
                conf,
                f"פתיחה {op05:.2f} → עכשיו {o05:.2f} בדקה {m}. "
                f"צפוי {get_expected(EXPECTED_OVER05_HT, m):.2f} – לחץ שוק: {p}%")

    if o15 > 0 and op15 > 0 and m <= 45:
        rise = o15 / op15
        exp  = get_expected(EXPECTED_OVER15_HT, m) / op15
        if rise < exp * 0.85 and m >= 15:
            conf = min(88, int(65 + p * 0.2))
            add(102, "HT Pressure – Over 1.5 HT",
                "goal", f"GOAL ENTRY – Over 1.5 HT @ {o15:.2f}",
                conf,
                f"פתיחה {op15:.2f} → עכשיו {o15:.2f} בדקה {m}. "
                f"שוק מצפה ל-2 גולים במחצית – לחץ: {p}%")

    # ── Late Game Rules (דקות מאוחרות) ────────────────────────────────────
    if m >= 80 and o > 0:
        exp_over = get_expected(EXPECTED_OVER25, m)
        if o < exp_over * 0.75:
            conf = min(92, int(75 + (exp_over - o) * 10))
            add(103, "Late Game Pressure",
                "goal", f"GOAL LIKELY – Over {o:.2f} נמוך לדקה {m}",
                conf,
                f"דקה {m}: Over {o:.2f} | צפוי: {exp_over:.1f} | "
                f"נמוך ב-{int((1-o/exp_over)*100)}% מהצפוי")

    if 85 <= m <= 92 and 2.7 <= o <= 3.5:
        add(104, "Late Odd Sweet Spot",
            "goal", f"HOT – Over {o:.2f} בדקה {m}",
            85,
            f"יחס {o:.2f} בדקה {m} – נמוך מאוד לשלב הזה. גול צפוי.")

    # ── Standard Rules ─────────────────────────────────────────────────────
    if 21 <= m <= 25 and 1.57 <= d <= 1.66 and 1.83 <= o <= 2.10:
        add(1, "Early Draw Signal", "no_goal", "DRAW or UNDER", 75,
            f"Draw {d} + Over {o} בדקה {m}")
    if 26 <= m <= 30 and 1.80 <= o <= 1.86 and 1.58 <= d <= 1.64:
        add(2, "Frozen Over", "no_goal", "NO ENTRY", 70,
            f"Over תקוע {o} בדקה {m}")
    if 1.66 <= o <= 1.75:
        add(3, "Two Early Goals Trap", "trap", "UNDER / TRAP", 72,
            f"Over {o} – מלכודת")
    if 30 <= m <= 34 and o >= 2.10:
        add(4, "Over 2.10 Value", "goal", "GOAL ENTRY", 78,
            f"Over {o} בדקה {m}")
    if 1.63 <= o <= 1.69:
        add(5, "1.66 Trap", "trap", "DO NOT ENTER", 80,
            f"Over {o} – אזור מלכודת")
    if 1.58 <= d <= 1.64 and 1.87 <= o <= 1.93:
        add(6, "Pair Signal 1.61+1.90", "goal", "GOAL", 83,
            f"Draw {d} + Over {o}")
    if 65 <= m <= 70 and o >= 2.15:
        add(7, "3rd Goal Moment", "goal", "GOAL ENTRY", 76,
            f"Over {o} בדקה {m}")
    if m >= 82 and o >= 2.80 and p < 30:
        add(8, "Market Shut", "no_goal", "NO GOAL", 88,
            f"Over {o} בדקה {m} – שוק סגור")
    if 17 <= m <= 20 and o <= 1.55:
        add(11, "Early Drop Signal", "goal", "GOAL VERY SOON", 86,
            f"Over ירד ל-{o} בדקה {m}")
    if m <= 15 and (hw or 0) <= 1.32:
        add(12, "Opening 1.30 Rule", "goal", "EARLY GOAL", 88,
            f"פתיחה {hw} – גול מוקדם")
    if 1.54 <= o <= 1.60:
        add(13, "1.57 Entry Point", "goal", "ENTRY", 79,
            f"Over {o}")
    if 2.30 <= o <= 2.70:
        if held >= 120:
            add(14, "Duration HELD 2min+", "goal", "POSSIBLE GOAL", 82,
                f"Over {o} החזיק {held}s")
        elif 0 < held <= 30 and direction == "up":
            add(14, "Duration REJECTED", "no_goal", "NO GOAL", 80,
                f"Over {o} קפץ ב-{held}s")
    if direction == "down" and change <= -0.15:
        add(15, "Sharp Drop Signal", "goal", "GOAL PRESSURE", 74,
            f"Over ירד {change}")
    if direction == "up" and change >= 0.15 and held <= 60:
        add(15, "Market Reversal", "trap", "POSSIBLE TRAP", 65,
            f"Over קפץ {change}")

    # ── Pressure + Standard combo ──────────────────────────────────────────
    if p >= 60 and o > 1.60:
        # Only add if not already covered by HT rules
        has_ht = any(s["rule_num"] in [101, 102] for s in signals)
        if not has_ht:
            add(200, "High Market Pressure",
                "goal", f"HOT – {p}% לחץ שוק",
                min(90, p),
                f"Over {o} בדקה {m} – לחץ שוק {p}% מעל הצפוי")

    return signals

# ─── Live Match Data ──────────────────────────────────────────────────────────
live_data   = {}
last_prices = {}
last_scores = {}
opening_odds_cache = {}  # match_id -> {market: odd}
betfair_ht_odds = {}     # match_id -> {over05ht, over15ht}

# Dashboard visibility for the data pipeline.
# This explains the difference between all API-Football live fixtures
# and only the live matches that have successfully linked odds.
last_pipeline_stats = {
    "live_fixtures": 0,
    "odds_games": 0,
    "linked_live": 0,
    "untracked_live": 0,
    "last_odds_update": None,
    "linked_examples": [],
    "unlinked_examples": [],
}

def fetch_live_football():
    """Fetch live fixtures from API-Football and keep raw team names for robust linking."""
    if not FOOTBALL_API_KEY:
        log.warning("FOOTBALL_API_KEY missing")
        return
    try:
        r = requests.get(
            "https://v3.football.api-sports.io/fixtures",
            headers={"x-apisports-key": FOOTBALL_API_KEY},
            params={"live": "all"},
            timeout=10
        )
        if r.status_code != 200:
            log.warning(f"Football API status: {r.status_code} body={r.text[:200]}")
            return

        live_data.clear()
        for f in r.json().get("response", []):
            try:
                home = f["teams"]["home"]["name"]
                away = f["teams"]["away"]["name"]
                status = f["fixture"]["status"] or {}
                min_ = status.get("elapsed") or 0
                extra = status.get("extra")
                short = status.get("short") or ""
                long_status = status.get("long") or ""

                hg = f["goals"]["home"] or 0
                ag = f["goals"]["away"] or 0
                league = f["league"]["name"]

                key = f"{home}_{away}"
                live_data[key] = {
                    "home_team": home,
                    "away_team": away,
                    "home_norm": normalize_team_name(home),
                    "away_norm": normalize_team_name(away),
                    "minute": min_,
                    "extra": extra,
                    "status_short": short,
                    "status_long": long_status,
                    "score": f"{hg}-{ag}",
                    "hg": hg,
                    "ag": ag,
                    "league": league,
                    "league_norm": normalize_team_name(league),
                }
            except Exception as e:
                log.warning(f"Skipping malformed fixture: {e}")
                continue

        log.info(f"⏱ {len(live_data)} live fixtures")
        if live_data:
            sample = list(live_data.values())[:5]
            log.info("Live sample: " + " | ".join(
                [f"{x['home_team']} vs {x['away_team']} {x['minute']}' {x['score']}" for x in sample]
            ))
    except Exception as e:
        log.error(f"Football API: {e}")

def fetch_betfair_ht():
    """Fetch Half Time Over/Under odds from Betfair"""
    if not BETFAIR_APP_KEY:
        return
    token = get_betfair_token()
    if not token:
        return
    try:
        markets = get_betfair_live_markets()
        if not markets:
            return
        ht_markets = {}
        for m in markets:
            etype = m.get("marketType", "")
            event = m.get("event", {})
            name  = event.get("name", "")
            mid   = m.get("marketId", "")
            if etype in ["OVER_UNDER_05", "OVER_UNDER_15"]:
                key = name.replace(" v ", "_").replace(" vs ", "_")
                if key not in ht_markets:
                    ht_markets[key] = {}
                ht_markets[key][etype] = mid

        if not ht_markets:
            return

        all_market_ids = []
        for v in ht_markets.values():
            all_market_ids.extend(v.values())

        books = get_betfair_odds(all_market_ids[:10])
        if not books:
            return

        odds_by_id = {}
        for book in books:
            mid  = book.get("marketId")
            runs = book.get("runners", [])
            if runs:
                best_back = runs[0].get("ex", {}).get("availableToBack", [])
                if best_back:
                    odds_by_id[mid] = best_back[0].get("price", 0)

        for match_key, mkt_ids in ht_markets.items():
            result = {}
            if "OVER_UNDER_05" in mkt_ids:
                result["over05ht"] = odds_by_id.get(mkt_ids["OVER_UNDER_05"])
            if "OVER_UNDER_15" in mkt_ids:
                result["over15ht"] = odds_by_id.get(mkt_ids["OVER_UNDER_15"])
            if result:
                betfair_ht_odds[match_key] = result

        log.info(f"🎰 Betfair: {len(betfair_ht_odds)} HT markets")
    except Exception as e:
        log.error(f"Betfair HT error: {e}")

TEAM_ALIASES = {
    # Common provider naming differences
    "club universitario de deportes": "universitario",
    "universitario de deportes": "universitario",
    "nacional de montevideo": "club nacional",
    "nacional montevideo": "club nacional",
    "club nacional de football": "club nacional",
    "la fc": "los angeles fc",
    "lafc": "los angeles fc",
    "los angeles": "los angeles fc",
    "america women": "america w",
    "america femenil": "america w",
    "juarez women": "juarez w",
    "juarez femenil": "juarez w",
}

STOP_TEAM_WORDS = {
    "fc", "cf", "sc", "afc", "club", "cd", "ac", "as", "fk", "ik", "if", "bk",
    "de", "da", "do", "del", "la", "le", "the", "real", "sporting",
    "deportivo", "athletic", "atletico", "atlético",
    "u17", "u18", "u19", "u20", "u21", "u23",
    "women", "woman", "femenil", "feminino", "feminina", "w",
    "reserves", "reserve", "youth", "academy",
    "ii", "iii", "b"
}

IMPORTANT_SHORT_TOKENS = {"la", "ny", "psg", "usa"}


def normalize_team_name(name):
    """Normalize team names from different providers for reliable matching."""
    if not name:
        return ""

    raw = str(name).strip().lower()
    raw = unicodedata.normalize("NFKD", raw)
    raw = "".join(c for c in raw if not unicodedata.combining(c))

    raw = raw.replace("&", " and ").replace("+", " ")
    raw = re.sub(r"\b(v|vs)\b", " ", raw)
    raw = re.sub(r"[^a-z0-9\s]", " ", raw)
    raw = re.sub(r"\s+", " ", raw).strip()

    if raw in TEAM_ALIASES:
        raw = TEAM_ALIASES[raw]

    parts = []
    for token in raw.split():
        if token in STOP_TEAM_WORDS:
            continue
        if len(token) <= 1 and token not in IMPORTANT_SHORT_TOKENS:
            continue
        parts.append(token)

    normalized = " ".join(parts).strip()
    return TEAM_ALIASES.get(normalized, normalized)


def token_set_score(a, b):
    """Lightweight fuzzy token-set score; no extra dependency required."""
    a_norm = normalize_team_name(a)
    b_norm = normalize_team_name(b)
    if not a_norm or not b_norm:
        return 0
    if a_norm == b_norm:
        return 100
    if a_norm in b_norm or b_norm in a_norm:
        return 92

    a_tokens = set(a_norm.split())
    b_tokens = set(b_norm.split())
    if not a_tokens or not b_tokens:
        return 0

    inter = a_tokens & b_tokens
    union = a_tokens | b_tokens
    jaccard = int(len(inter) / len(union) * 100)
    dice = int((2 * len(inter) / (len(a_tokens) + len(b_tokens))) * 100)
    seq = int(SequenceMatcher(None, a_norm, b_norm).ratio() * 100)

    containment = 0
    if inter:
        containment = int(len(inter) / min(len(a_tokens), len(b_tokens)) * 100)

    return max(jaccard, dice, seq, containment)


def match_pair_score(odds_home, odds_away, live_home, live_away):
    """Return best pair score and details. Requires BOTH teams to be plausible."""
    dh = token_set_score(odds_home, live_home)
    da = token_set_score(odds_away, live_away)
    direct_min = min(dh, da)
    direct_avg = (dh + da) / 2

    rh = token_set_score(odds_home, live_away)
    ra = token_set_score(odds_away, live_home)
    rev_min = min(rh, ra)
    rev_avg = (rh + ra) / 2

    direct = direct_avg if direct_min >= 45 else direct_avg * 0.55
    reversed_score = rev_avg if rev_min >= 45 else rev_avg * 0.55

    if reversed_score > direct:
        return reversed_score, "reversed", rh, ra, rev_min, rev_avg
    return direct, "direct", dh, da, direct_min, direct_avg


def get_live(home, away):
    """
    Link an Odds API game to an API-Football live fixture.

    Improvements:
    - Normalizes names deeply.
    - Uses token-set style scoring.
    - Requires both teams to match, not only one team.
    - Rejects weak random links.
    """
    if not live_data:
        return 0, "0-0", 0, 0, "", False

    exact_key = f"{home}_{away}"
    if exact_key in live_data:
        d = live_data[exact_key]
        return d["minute"], d["score"], d["hg"], d["ag"], d["league"], True

    best = None
    best_score = 0
    best_detail = ""

    for _, v in live_data.items():
        live_home = v.get("home_team", "")
        live_away = v.get("away_team", "")
        score, mode, s1, s2, min_team, avg_team = match_pair_score(home, away, live_home, live_away)

        detail = (
            f"candidate={live_home} vs {live_away} "
            f"mode={mode} team_scores={s1}/{s2} min={min_team:.1f} avg={avg_team:.1f}"
        )

        if score > best_score:
            best_score = score
            best = v
            best_detail = detail

    if best and best_score >= 72:
        log.info(
            f"🔗 MATCH LINKED odds='{home} vs {away}' -> "
            f"api='{best['home_team']} vs {best['away_team']}' score={best_score:.1f} {best_detail}"
        )
        return best["minute"], best["score"], best["hg"], best["ag"], best["league"], True

    log.warning(
        f"❌ NO LIVE LINK for odds='{home} vs {away}' "
        f"best_score={best_score:.1f} {best_detail}"
    )
    return 0, "0-0", 0, 0, "", False

def get_ht_odds(home, away):
    """Get HT odds from Betfair cache"""
    key = f"{home}_{away}"
    if key in betfair_ht_odds:
        return betfair_ht_odds[key]
    h1 = home.split()[0].lower()
    a1 = away.split()[0].lower()
    for k, v in betfair_ht_odds.items():
        if h1 in k.lower() or a1 in k.lower():
            return v
    return {}

def get_opening(match_id, market):
    """Get opening odd for a match/market"""
    key = f"{match_id}_{market}"
    return opening_odds_cache.get(key)

def set_opening(match_id, market, odd):
    """Set opening odd if not set yet"""
    key = f"{match_id}_{market}"
    if key not in opening_odds_cache:
        opening_odds_cache[key] = odd
        return True
    return False

def get_odds_before(conn, match_id, goal_time, seconds):
    """Get odds snapshots before the actual goal time, not before NOW()."""
    try:
        rows = conn.run("""SELECT market, outcome, odd_value FROM odds_snapshots
            WHERE match_id=:a
            AND captured_at BETWEEN (:gt - INTERVAL '1 second'*:c)
                                AND (:gt - INTERVAL '1 second'*:b)
            ORDER BY captured_at DESC LIMIT 20""",
            a=match_id, gt=goal_time, b=max(0, seconds-10), c=seconds+25)
        return {f"{r[0]}_{r[1]}": r[2] for r in rows} if rows else {}
    except Exception as e:
        log.error(f"get_odds_before error match={match_id} seconds={seconds}: {e}")
        return {}
# ─── OddsAPI.io Collector ─────────────────────────────────────────────────────
# Pull ALL live football events from OddsAPI.io, then fetch Bet365 odds for every
# event in batches of 10 every POLL_INTERVAL seconds.
# Railway variable needed: ODDSPAPI_KEY=...

last_oddsapi_events = []
last_oddsapi_raw_count = 0


def chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def first_present(d, keys, default=None):
    if not isinstance(d, dict):
        return default
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return default


def as_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        for k in ["data", "events", "odds", "results", "items"]:
            if isinstance(value.get(k), list):
                return value[k]
        return list(value.values())
    return []


def extract_event_id(event):
    return str(first_present(event, ["id", "eventId", "event_id", "eventID", "fixtureId", "fixture_id"], ""))


def extract_teams(event):
    home = first_present(event, ["home", "homeTeam", "home_team", "homeName", "home_name"])
    away = first_present(event, ["away", "awayTeam", "away_team", "awayName", "away_name"])
    if not home or not away:
        teams_list = as_list(first_present(event, ["teams", "participants", "competitors"], []))
        if len(teams_list) >= 2:
            def tname(t):
                if isinstance(t, dict):
                    return first_present(t, ["name", "team", "title", "shortName", "displayName"], "")
                return str(t)
            for t in teams_list:
                if isinstance(t, dict):
                    side = str(first_present(t, ["side", "homeAway", "type", "qualifier"], "")).lower()
                    if side == "home": home = tname(t)
                    elif side == "away": away = tname(t)
            home = home or tname(teams_list[0])
            away = away or tname(teams_list[1])
    name = first_present(event, ["name", "eventName", "event_name", "match", "title"], "")
    if (not home or not away) and isinstance(name, str):
        for sep in [" vs ", " v ", " - ", " @ "]:
            if sep in name:
                a, b = name.split(sep, 1)
                home = home or a.strip()
                away = away or b.strip()
                break
    return (home or "").strip(), (away or "").strip()


def fetch_oddspapi_events():
    if not ODDSPAPI_KEY:
        log.warning("OddsAPI.io: missing ODDSPAPI_KEY")
        return []
    try:
        r = requests.get("https://api.odds-api.io/v3/events", params={"apiKey": ODDSPAPI_KEY, "sport": "football", "status": "live"}, timeout=20)
        if r.status_code != 200:
            log.warning(f"OddsAPI.io events: {r.status_code} {r.text[:250]}")
            return []
        events = as_list(r.json())
        log.info(f"🎯 OddsAPI.io live events: {len(events)}")
        return events
    except Exception as e:
        log.error(f"OddsAPI.io events error: {e}")
        return []


def fetch_oddspapi_odds_multi(event_ids):
    if not ODDSPAPI_KEY or not event_ids:
        return []
    all_items = []
    base = "https://api.odds-api.io/v3"
    bookmaker = ODDSPAPI_BOOKMAKER or "Bet365"
    for batch in chunked(event_ids, 10):
        ids_csv = ",".join(batch)
        attempts = [
            (f"{base}/odds/multi", {"apiKey": ODDSPAPI_KEY, "eventIds": ids_csv, "bookmakers": bookmaker}),
            (f"{base}/odds/multi", {"apiKey": ODDSPAPI_KEY, "eventId": ids_csv, "bookmakers": bookmaker}),
            (f"{base}/odds/multi", {"apiKey": ODDSPAPI_KEY, "ids": ids_csv, "bookmakers": bookmaker}),
        ]
        ok = False
        for url, params in attempts:
            try:
                r = requests.get(url, params=params, timeout=25)
                if r.status_code == 200:
                    all_items.extend(as_list(r.json()))
                    ok = True
                    break
                log.warning(f"OddsAPI.io multi try failed: {r.status_code} {r.text[:180]}")
            except Exception as e:
                log.warning(f"OddsAPI.io multi request error: {e}")
        if ok:
            continue
        for eid in batch:
            try:
                r = requests.get(f"{base}/odds", params={"apiKey": ODDSPAPI_KEY, "eventId": eid, "bookmakers": bookmaker}, timeout=20)
                if r.status_code == 200:
                    data = r.json()
                    if isinstance(data, dict) and not extract_event_id(data):
                        data["eventId"] = eid
                    all_items.extend(as_list(data) if isinstance(data, list) else [data])
                else:
                    log.warning(f"OddsAPI.io odds event={eid}: {r.status_code} {r.text[:180]}")
            except Exception as e:
                log.warning(f"OddsAPI.io odds event={eid} error: {e}")
    log.info(f"📈 OddsAPI.io odds items: {len(all_items)} for events={len(event_ids)}")
    return all_items


def iter_bookmakers(odds_obj):
    bks = first_present(odds_obj, ["bookmakers", "sportsbooks", "sites", "providers"], [])
    if isinstance(bks, dict):
        out=[]
        for name, v in bks.items():
            vv = dict(v) if isinstance(v, dict) else {"markets": v}
            vv.setdefault("name", name)
            out.append(vv)
        return out
    return as_list(bks)


def is_bet365_bookmaker(bk):
    name = str(first_present(bk, ["name", "title", "key", "bookmaker", "id"], "")).lower()
    return "365" in name or not name


def iter_markets(obj):
    markets = first_present(obj, ["markets", "odds", "lines"], [])
    if isinstance(markets, dict):
        out=[]
        for name, v in markets.items():
            vv = dict(v) if isinstance(v, dict) else {"outcomes": v}
            vv.setdefault("key", name)
            out.append(vv)
        return out
    return as_list(markets)


def normalize_market_key(raw):
    s = str(raw or "").lower().replace(" ", "_").replace("-", "_")
    if any(x in s for x in ["h2h", "moneyline", "match_winner", "winner", "1x2", "full_time_result"]): return "h2h"
    if any(x in s for x in ["total", "over_under", "goals", "overunder"]): return "totals"
    if "next_goal" in s: return "next_goal"
    return s or "unknown"


def parse_outcomes(market):
    outcomes = first_present(market, ["outcomes", "runners", "selections", "prices"], [])
    if isinstance(outcomes, dict):
        out=[]
        for name, v in outcomes.items():
            vv = dict(v) if isinstance(v, dict) else {"price": v}
            vv.setdefault("name", name)
            out.append(vv)
        return out
    return as_list(outcomes)


def outcome_name_and_price(outcome):
    if not isinstance(outcome, dict): return "", None
    name = first_present(outcome, ["name", "label", "selection", "runnerName", "outcome", "type"], "")
    price = first_present(outcome, ["price", "odds", "decimal", "value", "back", "bestBackPrice"], None)
    if isinstance(price, dict): price = first_present(price, ["price", "decimal", "value"], None)
    try: price = float(price)
    except Exception: price = None
    return str(name), price


def build_game_from_oddspapi(event, odds_obj=None):
    eid = extract_event_id(event) or extract_event_id(odds_obj or {})
    home, away = extract_teams(event)
    if (not home or not away) and odds_obj: home, away = extract_teams(odds_obj)
    league = first_present(event, ["league", "competition", "tournament"], "")
    if isinstance(league, dict): league = first_present(league, ["name", "title"], "")
    raw = odds_obj if odds_obj else event
    raw_bks = iter_bookmakers(raw) or [{"name": ODDSPAPI_BOOKMAKER, "markets": iter_markets(raw)}]
    bookmakers=[]
    for bk in raw_bks:
        if not is_bet365_bookmaker(bk): continue
        markets=[]
        for m in iter_markets(bk):
            mkey = normalize_market_key(first_present(m, ["key", "name", "market", "type", "id"], ""))
            outs=[]
            for out in parse_outcomes(m):
                name, price = outcome_name_and_price(out)
                if price is None: continue
                lname=name.lower()
                if "over" in lname: name="Over"
                elif "under" in lname: name="Under"
                elif lname in ["x", "draw", "tie"]: name="Draw"
                outs.append({"name": name, "price": price})
            if outs: markets.append({"key": mkey, "outcomes": outs})
        if markets: bookmakers.append({"key": ODDSPAPI_BOOKMAKER, "markets": markets})
    return {"id": eid, "home_team": home, "away_team": away, "league": league, "bookmakers": bookmakers}


def fetch_oddspapi_games():
    events = fetch_oddspapi_events()
    event_ids = [extract_event_id(e) for e in events if extract_event_id(e)]
    odds_items = fetch_oddspapi_odds_multi(event_ids)
    odds_by_event={}
    for o in odds_items:
        oid=extract_event_id(o)
        if oid: odds_by_event[str(oid)] = o
    games=[]
    for e in events:
        eid=extract_event_id(e)
        game=build_game_from_oddspapi(e, odds_by_event.get(str(eid)))
        if game["id"] and game["home_team"] and game["away_team"]: games.append(game)
    return games


def collect():
    global last_pipeline_stats
    try:
        games = fetch_oddspapi_games()
        if not games:
            last_pipeline_stats = {"live_fixtures": len(live_data), "odds_games": 0, "linked_live": 0, "untracked_live": len(live_data), "last_odds_update": datetime.now(timezone.utc).isoformat(), "linked_examples": [], "unlinked_examples": [], "provider": "OddsAPI.io"}
            log.warning("OddsAPI.io: no games/odds returned")
            return
        live_cnt=0; linked_examples=[]; unlinked_examples=[]
        conn=get_db()
        try:
            for game in games:
                mid=str(game.get("id")); home=game.get("home_team",""); away=game.get("away_team","")
                min_, score, hg, ag, league, is_live = get_live(home, away)
                league = league or game.get("league","") or "OddsAPI.io"
                if is_live:
                    live_cnt += 1
                    if len(linked_examples) < 8: linked_examples.append(f"{home} vs {away}")
                else:
                    if len(unlinked_examples) < 12: unlinked_examples.append(f"{home} vs {away}")
                over05ht=over15ht=None; opening_o05=get_opening(mid,"over05ht"); opening_o15=get_opening(mid,"over15ht")
                try:
                    conn.run("""INSERT INTO matches (match_id,league,home_team,away_team,minute,score_home,score_away,status,last_updated) VALUES (:a,:b,:c,:d,:e,:f,:g,:h,NOW()) ON CONFLICT (match_id) DO UPDATE SET league=:b,minute=:e,score_home=:f,score_away=:g,status=:h,last_updated=NOW()""", a=mid,b=league,c=home,d=away,e=min_,f=hg,g=ag,h='live' if is_live else 'odds_only')
                except Exception as e: log.warning(f"match upsert failed {home} vs {away}: {e}")
                over_odd=draw_odd=hw_odd=aw_odd=None; prev_over=None
                for bk in game.get("bookmakers", [])[:1]:
                    for mkt in bk.get("markets", []):
                        mkey=mkt.get("key","unknown")
                        for out in mkt.get("outcomes", []):
                            oname=out.get("name","")
                            try: price=float(out.get("price"))
                            except Exception: continue
                            key=f"{mid}_{mkey}_{oname}"; now=time.time(); prev=None; held=0; direction="stable"; change=0.0
                            if key in last_prices:
                                lp=last_prices[key]; prev=lp["price"]; change=round(price-prev,3)
                                if abs(change)<0.01: held=int(now-lp["since"])
                                else:
                                    last_prices[key]={"price":price,"since":now}; direction="down" if change<0 else "up"
                            else: last_prices[key]={"price":price,"since":now}
                            held=int(now-last_prices[key]["since"])
                            if mkey=="totals" and oname=="Over":
                                over_odd=price; pk=f"{mid}_over_prev"; prev_over=last_prices.get(pk,{}).get("price"); last_prices[pk]={"price":price,"since":now}; set_opening(mid,"over25",price)
                            if mkey=="h2h":
                                if oname=="Draw": draw_odd=price
                                elif normalize_team_name(oname)==normalize_team_name(home) or "home" in oname.lower(): hw_odd=price
                                elif normalize_team_name(oname)==normalize_team_name(away) or "away" in oname.lower(): aw_odd=price
                            opening_val=get_opening(mid, f"{mkey}_{oname}")
                            if not opening_val:
                                set_opening(mid, f"{mkey}_{oname}", price); opening_val=price
                            exp_val=get_expected(EXPECTED_OVER25, min_) if mkey=="totals" and oname=="Over" else None
                            conn.run("""INSERT INTO odds_snapshots (match_id,minute,score_home,score_away,market,outcome,odd_value,prev_odd,opening_odd,odd_change,direction,held_seconds,pressure,expected_odd,is_live,source) VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,:m,:n,:o,'oddspapi')""", a=mid,b=min_,c=hg,d=ag,e=mkey,f=oname,g=price,h=prev,i=opening_val,j=change,k=direction,l=held,m=pressure_score(price,opening_val,min_),n=exp_val,o=is_live)
                prev_total=last_scores.get(mid); curr_total=hg+ag
                if prev_total is not None and curr_total > prev_total and is_live:
                    log.info(f"⚽ GOAL: {home} vs {away} {score} min:{min_}")
                    goal_time=datetime.now(timezone.utc); o10=get_odds_before(conn,mid,goal_time,10); o30=get_odds_before(conn,mid,goal_time,30); o60=get_odds_before(conn,mid,goal_time,60); o120=get_odds_before(conn,mid,goal_time,120); o300=get_odds_before(conn,mid,goal_time,300)
                    conn.run("""INSERT INTO goals (match_id,minute,score_before,score_after,auto_detected,odds_10s,odds_30s,odds_60s,odds_120s,odds_300s) VALUES (:a,:b,:c,:d,TRUE,:e,:f,:g,:h,:i)""", a=mid,b=min_,c=str(prev_total),d=score,e=json.dumps(o10),f=json.dumps(o30),g=json.dumps(o60),h=json.dumps(o120),i=json.dumps(o300))
                    log.info(f"Goal odds captured: 10s={len(o10)} 30s={len(o30)} 60s={len(o60)} 120s={len(o120)} 300s={len(o300)}")
                    for t,col in [(30,"goal_30s"),(60,"goal_60s"),(120,"goal_120s"),(300,"goal_300s")]:
                        try: conn.run(f"UPDATE odds_snapshots SET {col}=TRUE WHERE match_id=:a AND captured_at>NOW()-INTERVAL '{t} seconds'", a=mid)
                        except Exception: pass
                last_scores[mid]=curr_total
                if over_odd and is_live:
                    held_over=int(time.time()-last_prices.get(f"{mid}_totals_Over",{}).get("since",time.time())); dir_over="stable"; chg_over=0.0
                    if prev_over: chg_over=round(over_odd-prev_over,3); dir_over="down" if chg_over<0 else ("up" if chg_over>0 else "stable")
                    pres=pressure_score(over_odd, get_opening(mid,"over25"), min_)
                    sigs=run_rules(mid,home,away,league,over_odd,draw_odd,hw_odd,aw_odd,over05ht,over15ht,opening_o05,opening_o15,min_,held_over,dir_over,chg_over,pres)
                    for sig in sigs:
                        inserted = conn.run("""INSERT INTO signals
                            (match_id,home_team,away_team,league,rule_num,rule_name,
                             minute,score,signal_type,verdict,confidence,reason,
                             over_odd,draw_odd,over05ht_odd,over15ht_odd,
                             opening_over05ht,opening_over15ht,pressure_score,
                             held_seconds,direction,odd_change)
                            VALUES (:a,:b,:c,:d,:e,:f,:g,:h,:i,:j,:k,:l,
                                    :m,:n,:o,:p,:q,:r,:ps,:t,:u,:v)
                            RETURNING id""",
                            a=mid,b=home,c=away,d=league,e=sig["rule_num"],f=sig["rule_name"],
                            g=min_,h=score,i=sig["signal_type"],j=sig["verdict"],
                            k=sig["confidence"],l=sig["reason"],m=over_odd,n=draw_odd,
                            o=over05ht,p=over15ht,q=opening_o05,r=opening_o15,
                            ps=pres,t=held_over,u=dir_over,v=chg_over)
                        try:
                            sig_id = inserted[0][0] if inserted else None
                            create_simulated_bet(conn, sig_id, mid, home, away, league, min_, score, sig, over_odd)
                        except Exception as e:
                            log.warning(f"simulated bet create failed: {e}")
            last_pipeline_stats={"live_fixtures":len(live_data),"odds_games":len(games),"linked_live":live_cnt,"untracked_live":max(0,len(live_data)-live_cnt),"last_odds_update":datetime.now(timezone.utc).isoformat(),"linked_examples":linked_examples,"unlinked_examples":unlinked_examples,"provider":"OddsAPI.io","bookmaker":ODDSPAPI_BOOKMAKER}
            log.info(f"✅ OddsAPI.io Saved | linked:{live_cnt}/{len(games)} | fixtures:{len(live_data)} | untracked:{max(0,len(live_data)-live_cnt)} | bookmaker:{ODDSPAPI_BOOKMAKER}")
        finally: conn.close()
    except Exception as e:
        log.exception(f"Collect error: {e}")

def collector_loop():
    time.sleep(5)
    fetch_live_football()
    if USE_BETFAIR and BETFAIR_APP_KEY:
        betfair_login()
        fetch_betfair_ht()
    while True:
        collect()
        validate_signals()
        validate_simulated_bets()
        fetch_live_football()
        if USE_BETFAIR and BETFAIR_APP_KEY:
            fetch_betfair_ht()
        time.sleep(POLL_INTERVAL)

# --- Validation + Pattern Learning Engine -----------------------------------
def bucket_minute(minute):
    try:
        m = int(minute or 0)
    except Exception:
        m = 0
    return f"m{(m // 5) * 5:02d}"

def bucket_odd(odd):
    try:
        o = float(odd or 0)
    except Exception:
        o = 0
    if o <= 0:
        return "o000"
    return f"o{int(round(o * 10) * 10):03d}"

def bucket_pressure(pressure):
    try:
        p = int(pressure or 0)
    except Exception:
        p = 0
    return f"p{(p // 10) * 10:02d}"

def bucket_duration(seconds):
    try:
        s = int(seconds or 0)
    except Exception:
        s = 0
    if s < 30:
        return "d00"
    if s < 60:
        return "d30"
    if s < 120:
        return "d60"
    if s < 180:
        return "d120"
    return "d180"

def build_pattern_id(rule_num, minute, over_odd, pressure, held_seconds):
    return "__".join([
        f"rule{int(rule_num or 0)}",
        bucket_minute(minute),
        bucket_odd(over_odd),
        bucket_pressure(pressure),
        bucket_duration(held_seconds),
    ])

def goal_in_window(conn, match_id, start_ts, seconds):
    try:
        rows = conn.run("""SELECT 1 FROM goals
            WHERE match_id=:a
            AND recorded_at >= :b
            AND recorded_at <= (:b + (:c * INTERVAL '1 second'))
            LIMIT 1""", a=match_id, b=start_ts, c=int(seconds))
        return bool(rows)
    except Exception as e:
        log.error(f"goal_in_window error match={match_id}: {e}")
        return False

def failure_reason_for_signal(signal_type, direction, held_seconds, pressure, over_odd):
    reasons = []
    if direction == "up":
        reasons.append("market reversed upward")
    if held_seconds is not None and held_seconds < 60:
        reasons.append("duration too short")
    if pressure is not None and pressure < 30:
        reasons.append("low pressure score")
    if over_odd is None or over_odd <= 0:
        reasons.append("missing over odds")
    if signal_type == "trap":
        reasons.append("trap-style signal")
    return "; ".join(reasons) if reasons else "goal did not occur within validation window"

def create_simulated_bet(conn, signal_id, match_id, home, away, league, minute, score, sig, over_odd):
    """Create a paper-trade style simulated action from a signal.
    This is NOT a real bet. It is used only to measure model accuracy.
    """
    if not signal_id:
        return
    try:
        conf = int(sig.get("confidence") or 0)
        stype = sig.get("signal_type") or ""
        if conf < 70:
            return
        if stype == "goal":
            action_type = "GOAL_WITHIN_10M"
            market = "Goal / Over"
            entry_odd = float(over_odd or 0)
        elif stype in ("no_goal", "trap"):
            action_type = "NO_GOAL_10M"
            market = "No Goal / Avoid Goal"
            entry_odd = None
        else:
            return
        conn.run("""INSERT INTO simulated_bets
            (signal_id, match_id, home_team, away_team, league, minute, score,
             action_type, market, entry_odd, virtual_stake, confidence, reason)
            VALUES (:sid,:mid,:h,:a,:lg,:m,:sc,:act,:market,:odd,100,:conf,:reason)
            ON CONFLICT (signal_id) DO NOTHING""",
            sid=signal_id, mid=match_id, h=home, a=away, lg=league, m=minute,
            sc=score, act=action_type, market=market, odd=entry_odd,
            conf=conf, reason=sig.get("reason") or sig.get("verdict") or "")
    except Exception as e:
        log.error(f"create_simulated_bet error: {e}")

def validate_simulated_bets():
    """Resolve simulated actions after 10 minutes.
    Goal actions win if a goal happened within 10m.
    No-goal/trap actions win if no goal happened within 10m.
    """
    try:
        conn = get_db()
    except Exception as e:
        log.error(f"validate_simulated_bets DB connect error: {e}")
        return
    try:
        rows = conn.run("""SELECT id, match_id, created_at, action_type, entry_odd, virtual_stake
            FROM simulated_bets
            WHERE COALESCE(validated,FALSE)=FALSE
            AND created_at < NOW() - INTERVAL '10 minutes'
            ORDER BY created_at ASC
            LIMIT 300""")
        if not rows:
            return
        resolved = 0
        for bid, match_id, created_at, action_type, entry_odd, stake in rows:
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            g10 = goal_in_window(conn, match_id, created_at, 600)
            if action_type == "GOAL_WITHIN_10M":
                win = bool(g10)
                profit = (float(stake or 100) * (float(entry_odd or 2.0) - 1.0)) if win else -float(stake or 100)
            else:
                win = not bool(g10)
                profit = float(stake or 100) * 0.80 if win else -float(stake or 100)
            conn.run("""UPDATE simulated_bets SET
                    validated=TRUE, status='closed', result=:result,
                    goal_within_10m=:g10, virtual_profit=:profit, resolved_at=NOW()
                WHERE id=:id""",
                result='win' if win else 'loss', g10=g10, profit=round(profit, 2), id=bid)
            resolved += 1
        if resolved:
            log.info(f"paper trading validation complete | resolved={resolved}")
    except Exception as e:
        log.error(f"validate_simulated_bets error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def update_pattern_stats(conn, row, goal2, goal5, goal10, false_positive, pattern_id):
    try:
        conn.run("""INSERT INTO pattern_stats
            (pattern_id, rule_num, rule_name, minute_bucket, odds_bucket, pressure_bucket, duration_bucket,
             total_cases, goals_2m, goals_5m, goals_10m, no_goal_cases, false_positive_cases,
             success_rate_2m, success_rate_5m, success_rate_10m, confidence_level, last_updated)
            VALUES
            (:pid, :rn, :rname, :mb, :ob, :pb, :db,
             1, :g2, :g5, :g10, :ng, :fp,
             :sr2, :sr5, :sr10, 'low', NOW())
            ON CONFLICT (pattern_id) DO UPDATE SET
                total_cases = pattern_stats.total_cases + 1,
                goals_2m = pattern_stats.goals_2m + EXCLUDED.goals_2m,
                goals_5m = pattern_stats.goals_5m + EXCLUDED.goals_5m,
                goals_10m = pattern_stats.goals_10m + EXCLUDED.goals_10m,
                no_goal_cases = pattern_stats.no_goal_cases + EXCLUDED.no_goal_cases,
                false_positive_cases = pattern_stats.false_positive_cases + EXCLUDED.false_positive_cases,
                success_rate_2m = ((pattern_stats.goals_2m + EXCLUDED.goals_2m)::float / (pattern_stats.total_cases + 1)) * 100,
                success_rate_5m = ((pattern_stats.goals_5m + EXCLUDED.goals_5m)::float / (pattern_stats.total_cases + 1)) * 100,
                success_rate_10m = ((pattern_stats.goals_10m + EXCLUDED.goals_10m)::float / (pattern_stats.total_cases + 1)) * 100,
                confidence_level = CASE
                    WHEN pattern_stats.total_cases + 1 >= 100 THEN 'very_high'
                    WHEN pattern_stats.total_cases + 1 >= 30 THEN 'high'
                    WHEN pattern_stats.total_cases + 1 >= 10 THEN 'medium'
                    ELSE 'low'
                END,
                last_updated = NOW()""",
            pid=pattern_id,
            rn=row.get("rule_num"), rname=row.get("rule_name") or "",
            mb=bucket_minute(row.get("minute")),
            ob=bucket_odd(row.get("over_odd")),
            pb=bucket_pressure(row.get("pressure_score")),
            db=bucket_duration(row.get("held_seconds")),
            g2=1 if goal2 else 0,
            g5=1 if goal5 else 0,
            g10=1 if goal10 else 0,
            ng=0 if goal10 else 1,
            fp=1 if false_positive else 0,
            sr2=100.0 if goal2 else 0.0,
            sr5=100.0 if goal5 else 0.0,
            sr10=100.0 if goal10 else 0.0)
        log.info(f"PatternStats updated {pattern_id} | g2={goal2} g5={goal5} g10={goal10} fp={false_positive}")
    except Exception as e:
        log.error(f"update_pattern_stats error pattern={pattern_id}: {e}")


def safe_json_loads(text, default=None):
    if default is None:
        default = {}
    try:
        return json.loads(text)
    except Exception:
        return default

def extract_json_object(text):
    """Extract first JSON object from model output."""
    if not text:
        return {}
    text = text.strip()
    if text.startswith("```"):
        text = text.replace("```json", "").replace("```", "").strip()
    start = text.find("{")
    end = text.rfind("}")
    if start >= 0 and end > start:
        return safe_json_loads(text[start:end+1], {})
    return safe_json_loads(text, {})

def save_ai_rule_candidates(conn, ai_payload):
    """Save AI suggested rules as inactive candidates. Does not activate them."""
    saved = 0
    rules = ai_payload.get("new_rules") or ai_payload.get("new_patterns_to_test") or []
    if not isinstance(rules, list):
        return 0
    for r in rules:
        if not isinstance(r, dict):
            continue
        name = str(r.get("rule_name") or r.get("name") or r.get("pattern") or "").strip()
        if not name:
            continue
        desc = str(r.get("description") or r.get("reason") or r.get("logic") or "").strip()
        expected = str(r.get("expected_outcome") or r.get("outcome") or "goal").lower()
        if expected not in ["goal", "no_goal", "trap"]:
            expected = "goal"
        conditions = r.get("conditions") or r.get("conditions_json") or {}
        if isinstance(conditions, str):
            conditions = {"text": conditions}
        try:
            conn.run("""INSERT INTO ai_rule_candidates
                (rule_name, description, conditions_json, expected_outcome, status, active, updated_at)
                VALUES (:n,:d,:c,:e,'candidate',FALSE,NOW())
                ON CONFLICT (rule_name) DO UPDATE SET
                    description=EXCLUDED.description,
                    conditions_json=EXCLUDED.conditions_json,
                    expected_outcome=EXCLUDED.expected_outcome,
                    updated_at=NOW()""",
                n=name, d=desc, c=json.dumps(conditions), e=expected)
            saved += 1
        except Exception as e:
            log.error(f"save_ai_rule_candidates error {name}: {e}")
    return saved

def promote_ai_rule_candidates(conn):
    """Promote inactive AI candidates only after enough validated evidence exists."""
    try:
        # Try to match by rule name if candidate name appears inside a PatternStats rule_name.
        rows = conn.run("""SELECT c.id, c.rule_name,
                   COALESCE(SUM(p.total_cases),0) AS cases,
                   COALESCE(SUM(p.goals_10m),0) AS g10
            FROM ai_rule_candidates c
            LEFT JOIN pattern_stats p ON LOWER(p.rule_name) LIKE '%' || LOWER(c.rule_name) || '%'
            WHERE c.active=FALSE AND c.status IN ('candidate','testing')
            GROUP BY c.id, c.rule_name""")
        promoted = 0
        for cid, name, cases, g10 in rows:
            cases = int(cases or 0)
            g10 = int(g10 or 0)
            rate = (g10 / cases * 100.0) if cases else 0.0
            level = 'very_high' if cases >= 100 else ('high' if cases >= 30 else ('medium' if cases >= 10 else 'low'))
            status = 'validated' if cases >= 10 and rate > 50 else ('testing' if cases > 0 else 'candidate')
            active = bool(cases >= 10 and rate > 50)
            reason = f"Auto-promoted: {cases} cases, success_rate_10m={rate:.1f}%" if active else None
            conn.run("""UPDATE ai_rule_candidates SET
                    total_cases=:cases, goals_10m=:g10, success_rate_10m=:rate,
                    confidence_level=:level, status=:status, active=:active,
                    promotion_reason=:reason, updated_at=NOW()
                WHERE id=:id""",
                cases=cases, g10=g10, rate=rate, level=level, status=status,
                active=active, reason=reason, id=cid)
            if active:
                promoted += 1
        if promoted:
            log.info(f"AI rule candidates promoted: {promoted}")
    except Exception as e:
        log.error(f"promote_ai_rule_candidates error: {e}")
def validate_signals():
    try:
        conn = get_db()
    except Exception as e:
        log.error(f"validate_signals DB connect error: {e}")
        return
    try:
        rows = conn.run("""SELECT id, match_id, detected_at, checked_2m, checked_5m, checked_10m,
                rule_num, rule_name, minute, signal_type, confidence, over_odd,
                pressure_score, held_seconds, direction, pattern_id
            FROM signals
            WHERE COALESCE(validated, FALSE)=FALSE
            AND match_id IS NOT NULL
            AND detected_at < NOW() - INTERVAL '2 minutes'
            ORDER BY detected_at ASC
            LIMIT 300""")
        if not rows:
            return
        cols = ["id","match_id","detected_at","checked_2m","checked_5m","checked_10m",
                "rule_num","rule_name","minute","signal_type","confidence","over_odd",
                "pressure_score","held_seconds","direction","pattern_id"]
        now = datetime.now(timezone.utc)
        staged = finalised = 0
        for raw in rows:
            row = dict(zip(cols, raw))
            sid = row["id"]
            detected_at = row["detected_at"]
            if detected_at.tzinfo is None:
                detected_at = detected_at.replace(tzinfo=timezone.utc)
            elapsed = (now - detected_at).total_seconds()
            if not row.get("checked_2m") and elapsed >= 120:
                g2 = goal_in_window(conn, row["match_id"], detected_at, 120)
                conn.run("""UPDATE signals SET checked_2m=TRUE, goal_2m=:g,
                    validation_updated_at=NOW() WHERE id=:id""", g=g2, id=sid)
                staged += 1
                log.info(f"Signal {sid} checked 2m | goal={g2}")
            if not row.get("checked_5m") and elapsed >= 300:
                g5 = goal_in_window(conn, row["match_id"], detected_at, 300)
                conn.run("""UPDATE signals SET checked_5m=TRUE, goal_5m=:g,
                    validation_updated_at=NOW() WHERE id=:id""", g=g5, id=sid)
                staged += 1
                log.info(f"Signal {sid} checked 5m | goal={g5}")
            if not row.get("checked_10m") and elapsed >= 600:
                g2 = goal_in_window(conn, row["match_id"], detected_at, 120)
                g5 = goal_in_window(conn, row["match_id"], detected_at, 300)
                g10 = goal_in_window(conn, row["match_id"], detected_at, 600)
                false_positive = bool(row.get("signal_type") == "goal" and int(row.get("confidence") or 0) >= 50 and not g10)
                reason = failure_reason_for_signal(row.get("signal_type"), row.get("direction"), row.get("held_seconds"), row.get("pressure_score"), row.get("over_odd")) if false_positive else None
                pattern_id = row.get("pattern_id") or build_pattern_id(row.get("rule_num"), row.get("minute"), row.get("over_odd"), row.get("pressure_score"), row.get("held_seconds"))
                conn.run("""UPDATE signals SET
                        checked_2m=TRUE, checked_5m=TRUE, checked_10m=TRUE,
                        goal_2m=:g2, goal_5m=:g5, goal_10m=:g10,
                        validated=TRUE, false_positive=:fp, failure_reason=:fr,
                        pattern_id=:pid, validation_updated_at=NOW()
                    WHERE id=:id""",
                    g2=g2, g5=g5, g10=g10, fp=false_positive, fr=reason, pid=pattern_id, id=sid)
                update_pattern_stats(conn, row, g2, g5, g10, false_positive, pattern_id)
                promote_ai_rule_candidates(conn)
                finalised += 1
                log.info(f"VALIDATED signal {sid} | pattern={pattern_id} | goal10={g10} | fp={false_positive}")
        if staged or finalised:
            log.info(f"validation cycle complete | staged={staged} finalised={finalised}")
    except Exception as e:
        log.error(f"validate_signals error: {e}")
    finally:
        try:
            conn.close()
        except Exception:
            pass

# ─── Dashboard HTML ───────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html lang="he" dir="rtl">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>PapaGoal — Read the Market</title>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@300;400;500;600;700;900&display=swap" rel="stylesheet">
<style>
:root{--bg:#030308;--bg2:#070710;--card:#0a0a15;--card2:#0e0e1a;--border:#141428;--border2:#1a1a32;--green:#00ff88;--red:#ff3355;--yellow:#ffcc00;--orange:#ff6b35;--blue:#4488ff;--purple:#8855ff;--text:#e8e8f8;--muted:#6666aa}
*{box-sizing:border-box;margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:'Inter',sans-serif;min-height:100vh;display:flex}
.sidebar{width:220px;min-height:100vh;background:var(--bg2);border-right:1px solid var(--border);display:flex;flex-direction:column;position:fixed;top:0;left:0;bottom:0;z-index:100}
.sidebar-logo{padding:20px 16px;border-bottom:1px solid var(--border)}
.logo-main{font-family:'JetBrains Mono',monospace;font-size:18px;font-weight:700;color:#fff;letter-spacing:2px}
.logo-main span{color:var(--green)}
.logo-sub{font-size:10px;color:var(--muted);letter-spacing:1px;margin-top:2px}
.nav{flex:1;padding:12px 8px}
.nav-item{display:flex;align-items:center;gap:10px;padding:9px 12px;border-radius:8px;font-size:13px;color:var(--muted);cursor:pointer;transition:all 0.15s;margin-bottom:2px;border:none;background:none;width:100%;text-align:right}
.nav-item:hover{background:var(--card);color:var(--text)}
.nav-item.active{background:rgba(0,255,136,0.1);color:var(--green)}
.nav-icon{font-size:14px;width:18px;text-align:center}
.main{margin-left:220px;flex:1;min-height:100vh}
.page{display:none;padding:24px;max-width:1100px}
.page.active{display:block}
.page-header{margin-bottom:20px;display:flex;justify-content:space-between;align-items:flex-start}
.page-title{font-size:22px;font-weight:700}
.page-sub{font-size:12px;color:var(--muted);font-family:'JetBrains Mono',monospace;margin-top:4px}
.stats-row{display:grid;grid-template-columns:repeat(6,1fr);gap:10px;margin-bottom:20px}
.stat-card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px}
.stat-num{font-size:26px;font-weight:900;font-family:'JetBrains Mono',monospace}
.stat-label{font-size:11px;color:var(--muted);margin-top:4px}
.section-title{font-size:11px;letter-spacing:3px;color:var(--muted);text-transform:uppercase;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid var(--border)}
.match-card{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:16px;transition:border-color 0.2s;margin-bottom:10px}
.match-card.c-goal{border-color:rgba(0,255,136,0.5);background:linear-gradient(135deg,rgba(0,255,136,0.04),var(--card))}
.match-card.c-trap{border-color:rgba(255,51,85,0.5);background:linear-gradient(135deg,rgba(255,51,85,0.04),var(--card))}
.match-card.c-warn{border-color:rgba(255,204,0,0.3)}
.match-card.c-hot{border-color:rgba(0,255,136,0.8);background:linear-gradient(135deg,rgba(0,255,136,0.08),var(--card));box-shadow:0 0 20px rgba(0,255,136,0.1)}
.match-top{display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;gap:8px}
.match-name{font-size:16px;font-weight:700}
.match-league{font-size:11px;color:var(--muted);margin-top:2px}
.badges{display:flex;gap:5px;align-items:center;flex-wrap:wrap}
.badge{padding:3px 8px;border-radius:5px;font-size:11px;font-weight:600;font-family:'JetBrains Mono',monospace}
.b-live{background:rgba(0,255,136,0.12);color:var(--green);border:1px solid rgba(0,255,136,0.3)}
.b-min{background:rgba(255,255,255,0.06);color:var(--text)}
.b-score{background:rgba(255,204,0,0.1);color:var(--yellow)}
.b-hot{background:rgba(0,255,136,0.2);color:var(--green);border:1px solid rgba(0,255,136,0.5);animation:pulse 1.5s infinite}
.b-pressure{background:rgba(255,107,53,0.15);color:var(--orange);border:1px solid rgba(255,107,53,0.3)}
.odds-row{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:10px}
.odd-tag{background:var(--card2);border:1px solid var(--border2);border-radius:6px;padding:5px 10px;font-family:'JetBrains Mono',monospace;font-size:12px;display:flex;flex-direction:column;align-items:center;gap:1px}
.odd-label{font-size:9px;color:var(--muted);letter-spacing:1px}
.odd-val{font-size:13px;font-weight:700}
.odd-tag.ht-market{border-color:rgba(68,136,255,0.3);background:rgba(68,136,255,0.05)}
.odd-tag.ht-market .odd-label{color:var(--blue)}
.pressure-bar{height:4px;background:var(--border2);border-radius:2px;margin-bottom:10px;overflow:hidden}
.pressure-fill{height:100%;border-radius:2px;transition:width 0.5s}
.verdict{padding:9px 14px;border-radius:8px;font-size:13px;font-weight:700;letter-spacing:0.5px;margin-bottom:8px}
.v-goal{background:rgba(0,255,136,0.12);color:var(--green);border:1px solid rgba(0,255,136,0.3)}
.v-trap{background:rgba(255,51,85,0.12);color:var(--red);border:1px solid rgba(255,51,85,0.3)}
.v-warn{background:rgba(255,204,0,0.1);color:var(--yellow);border:1px solid rgba(255,204,0,0.3)}
.ai-box{background:rgba(68,136,255,0.05);border:1px solid rgba(68,136,255,0.2);border-radius:8px;padding:12px;margin-top:10px;font-size:13px;line-height:1.7;color:#aaaacc}
.ai-label{font-size:10px;letter-spacing:2px;color:var(--blue);margin-bottom:6px;font-family:'JetBrains Mono',monospace}
.goal-card{background:var(--card);border:1px solid rgba(0,255,136,0.2);border-radius:12px;padding:16px;margin-bottom:10px}
.goal-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.goal-match{font-size:15px;font-weight:700}
.goal-min{font-size:14px;font-family:'JetBrains Mono',monospace;color:var(--green)}
.ot-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:6px;margin-top:10px}
.ot-cell{background:var(--card2);border-radius:6px;padding:6px;text-align:center}
.ot-label{font-size:9px;color:var(--muted);letter-spacing:1px}
.ot-val{font-size:13px;font-weight:700;font-family:'JetBrains Mono',monospace;color:var(--green);margin-top:2px}
.sig-card{background:var(--card);border:1px solid var(--border);border-radius:10px;padding:14px;margin-bottom:8px}
.sig-top{display:flex;justify-content:space-between;align-items:center;gap:8px}
.progress-bar{height:6px;background:var(--card2);border-radius:3px;overflow:hidden;margin:4px 0}
.progress-fill{height:100%;border-radius:3px;transition:width 0.5s}
.ai-run-btn{background:rgba(136,85,255,0.1);border:1px solid rgba(136,85,255,0.3);color:var(--purple);border-radius:8px;padding:10px 20px;font-size:14px;font-family:'Inter',sans-serif;font-weight:600;cursor:pointer;width:100%;margin-bottom:16px;transition:all 0.2s}
.ai-run-btn:hover{background:rgba(136,85,255,0.2)}
.insight-card{background:var(--card);border:1px solid rgba(136,85,255,0.2);border-radius:12px;padding:16px;margin-bottom:10px}
.insight-title{font-size:13px;font-weight:700;color:var(--purple);margin-bottom:8px}
.insight-text{font-size:13px;line-height:1.7;color:#aaaacc}
.empty{text-align:center;padding:60px 20px;color:var(--muted)}
.empty-icon{font-size:42px;margin-bottom:12px}
.live-dot{width:8px;height:8px;border-radius:50%;background:var(--green);animation:blink 1.2s infinite;display:inline-block}
.upd-time{font-size:11px;color:var(--muted);font-family:'JetBrains Mono',monospace}
@keyframes blink{0%,100%{opacity:1}50%{opacity:0.2}}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.6}}
::-webkit-scrollbar{width:4px}
::-webkit-scrollbar-track{background:var(--bg)}
::-webkit-scrollbar-thumb{background:var(--border2);border-radius:2px}
@media(max-width:768px){.sidebar{width:56px}.sidebar .nav-item span,.logo-sub,.logo-main{display:none}.main{margin-left:56px}.stats-row{grid-template-columns:repeat(2,1fr)}.ot-grid{grid-template-columns:repeat(3,1fr)}}
</style>
</head>
<body>
<div class="sidebar">
  <div class="sidebar-logo">
    <div class="logo-main">PAPA<span>GOAL</span></div>
    <div class="logo-sub">READ THE MARKET</div>
  </div>
  <nav class="nav">
    <button class="nav-btn nav-item active" onclick="showPage('live',this)"><span class="nav-icon">📡</span><span>Live Dashboard</span></button>
    <button class="nav-btn nav-item" onclick="showPage('goals',this)"><span class="nav-icon">⚽</span><span>Goals</span></button>
    <button class="nav-btn nav-item" onclick="showPage('signals',this)"><span class="nav-icon">🔥</span><span>Signals</span></button>
    <button class="nav-btn nav-item" onclick="showPage('paper',this)"><span class="nav-icon">🧪</span><span>Paper Trades</span></button>
    <button class="nav-btn nav-item" onclick="showPage('analytics',this)"><span class="nav-icon">📊</span><span>Analytics</span></button>
    <button class="nav-btn nav-item" onclick="showPage('ai',this)"><span class="nav-icon">🤖</span><span>AI Insights</span></button>
  </nav>
</div>
<div class="main">
  <div class="page active" id="page-live">
    <div class="page-header">
      <div><div class="page-title">Live Dashboard <span class="live-dot"></span></div><div class="page-sub">Don't predict football. Read the market.</div></div>
      <div class="upd-time" id="upd-live">מתעדכן...</div>
    </div>
    <div class="stats-row">
      <div class="stat-card"><div class="stat-num" style="color:var(--blue)" id="sl-fixtures">—</div><div class="stat-label">Live Fixtures</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--green)" id="sl-live">—</div><div class="stat-label">Tracked With Odds</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--orange)" id="sl-untracked">—</div><div class="stat-label">Live Without Odds</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--muted)" id="sl-odds-games">—</div><div class="stat-label">Odds Games</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--yellow)" id="sl-goals">—</div><div class="stat-label">Goals Today</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--purple)" id="sl-snaps">—</div><div class="stat-label">Snapshots</div></div>
    </div>
    <div id="pipeline-debug" class="match-card" style="margin-bottom:16px;display:none"></div>
    <div class="section-title">🎯 המלצות – משחקים חיים בלבד</div>
    <div id="live-cards"><div class="empty"><div class="empty-icon">📡</div><div>סורק משחקים חיים...</div></div></div>
  </div>
  <div class="page" id="page-goals">
    <div class="page-header"><div><div class="page-title">⚽ Goals Detected</div><div class="page-sub">יחסים לפני כל גול – הלמידה המרכזית</div></div></div>
    <div id="goals-list"><div class="empty"><div class="empty-icon">⚽</div><div>טוען גולים...</div></div></div>
  </div>
  <div class="page" id="page-signals">
    <div class="page-header"><div><div class="page-title">🔥 All Signals</div><div class="page-sub">כל האותות מ-3 השעות האחרונות</div></div></div>
    <div id="signals-list"><div class="empty"><div class="empty-icon">🔥</div><div>טוען...</div></div></div>
  </div>
  <div class="page" id="page-paper">
    <div class="page-header"><div><div class="page-title">🧪 Paper Trading</div><div class="page-sub">פעולות דמיוניות בלבד — בדיקת הצלחה של ההמלצות בלי כסף אמיתי</div></div></div>
    <div id="paper-stats" class="stats-row"></div>
    <div id="paper-list"><div class="empty"><div class="empty-icon">🧪</div><div>טוען פעולות דמיוניות...</div></div></div>
  </div>
  <div class="page" id="page-analytics">
    <div class="page-header"><div><div class="page-title">📊 Analytics</div><div class="page-sub">ניתוח היסטורי ולמידה מנתונים</div></div></div>
    <div id="analytics-content"><div class="empty"><div class="empty-icon">📊</div><div>טוען...</div></div></div>
  </div>
  <div class="page" id="page-ai">
    <div class="page-header"><div><div class="page-title">🤖 AI Insights</div><div class="page-sub">Claude מנתח דפוסי שוק היסטוריים</div></div></div>
    <button class="ai-run-btn" onclick="runAI()" id="ai-btn">🤖 הרץ ניתוח AI עכשיו</button>
    <div id="ai-content"><div class="empty"><div class="empty-icon">🤖</div><div>לחץ להרצת ניתוח AI</div></div></div>
  </div>
</div>
<script>
let currentPage='live';
const vc={'goal':'v-goal','no_goal':'v-trap','trap':'v-trap','warn':'v-warn'};
const mc={'goal':'c-goal','no_goal':'c-trap','trap':'c-trap','warn':'c-warn'};
const ic={'goal':'🟢','no_goal':'🔴','trap':'🔴','warn':'🟡'};

function showPage(p,btn){
  document.querySelectorAll('.page').forEach(x=>x.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(x=>x.classList.remove('active'));
  document.getElementById('page-'+p).classList.add('active');
  if(btn) btn.classList.add('active');
  currentPage=p;
  if(p==='goals') loadGoals();
  else if(p==='signals') loadSignals();
  else if(p==='paper') loadPaper();
  else if(p==='analytics') loadAnalytics();
  else if(p==='ai') loadAI();
}

async function loadLive(){
  try{
    const[st,si,ai]=await Promise.all([
      fetch('/api/stats').then(r=>r.json()),
      fetch('/api/signals').then(r=>r.json()),
      fetch('/api/ai_live').then(r=>r.json())
    ]);
    document.getElementById('sl-fixtures').textContent=st.live_fixtures||0;
    document.getElementById('sl-live').textContent=st.tracked_with_odds||st.live||0;
    document.getElementById('sl-untracked').textContent=st.untracked_live||0;
    document.getElementById('sl-odds-games').textContent=st.odds_games||0;
    document.getElementById('sl-goals').textContent=st.goals_today||0;
    document.getElementById('sl-snaps').textContent=(st.snapshots||0).toLocaleString();
    const dbg=document.getElementById('pipeline-debug');
    if(dbg){
      const linked=(st.linked_examples||[]).slice(0,3).join(' · ');
      const unlinked=(st.unlinked_examples||[]).slice(0,4).join(' · ');
      dbg.style.display='block';
      dbg.innerHTML=`<div class="section-title">🔎 Data Pipeline Status</div>
        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:8px;font-size:12px">
          <div><span style="color:var(--muted)">API-Football Live</span><br><b style="color:var(--blue)">${st.live_fixtures||0}</b></div>
          <div><span style="color:var(--muted)">Odds API Games</span><br><b>${st.odds_games||0}</b></div>
          <div><span style="color:var(--muted)">Linked Live Odds</span><br><b style="color:var(--green)">${st.tracked_with_odds||0}</b></div>
          <div><span style="color:var(--muted)">Untracked Live</span><br><b style="color:var(--orange)">${st.untracked_live||0}</b></div>
        </div>
        ${linked?`<div style="margin-top:10px;font-size:11px;color:var(--green)">Linked: ${linked}</div>`:''}
        ${unlinked?`<div style="margin-top:6px;font-size:11px;color:var(--muted)">Odds not live/linked: ${unlinked}</div>`:''}
        ${(st.live_fixtures||0)>0 && (st.tracked_with_odds||0)===0?`<div style="margin-top:10px;font-size:12px;color:var(--orange)">⚠️ יש משחקים חיים, אבל אין להם odds מחוברים כרגע.</div>`:''}`;
    }
    document.getElementById('upd-live').textContent='עדכון: '+new Date().toLocaleTimeString('he-IL');
    const aiMap={};
    ai.forEach(a=>aiMap[a.match_id]=a.analysis);
    const el=document.getElementById('live-cards');
    if(!si.length){
      el.innerHTML='<div class="empty"><div class="empty-icon">✅</div><div style="font-size:15px;font-weight:700;margin-bottom:6px">אין אותות פעילים כרגע</div><div style="font-size:12px">עוקב אחרי '+(st.live_fixtures||0)+' משחקים חיים · '+(st.tracked_with_odds||st.live||0)+' עם odds</div></div>';
      return;
    }
    const byMatch={};
    si.forEach(s=>{
      if(!byMatch[s.match_id]) byMatch[s.match_id]={...s,rules:[]};
      if(!byMatch[s.match_id].rules.find(r=>r.rule_num===s.rule_num))
        byMatch[s.match_id].rules.push(s);
    });
    el.innerHTML=Object.values(byMatch).map(m=>{
      const c=m.signal_type||'warn';
      const isHot=(m.pressure_score||0)>=60 || m.rule_num>=100;
      const cardClass=isHot?'c-hot':mc[c]||'';
      const rules=m.rules.map(r=>`R${r.rule_num} ${r.rule_name}`).join(' · ');
      const pres=m.pressure_score||0;
      const presColor=pres>=70?'var(--green)':pres>=40?'var(--orange)':'var(--muted)';
      const ht05=m.over05ht_odd?`<div class="odd-tag ht-market"><div class="odd-label">0.5 HT</div><div class="odd-val">${m.over05ht_odd.toFixed(2)}</div></div>`:'';
      const ht15=m.over15ht_odd?`<div class="odd-tag ht-market"><div class="odd-label">1.5 HT</div><div class="odd-val">${m.over15ht_odd.toFixed(2)}</div></div>`:'';
      const ai=aiMap[m.match_id]?`<div class="ai-box"><div class="ai-label">🤖 CLAUDE AI</div>${aiMap[m.match_id]}</div>`:'';
      return `<div class="match-card ${cardClass}">
        <div class="match-top">
          <div>
            <div class="match-name">${m.home_team} vs ${m.away_team}</div>
            <div class="match-league">${m.league||''}</div>
          </div>
          <div class="badges">
            ${m.minute>0?`<span class="badge b-min">⏱ ${m.minute}'`:''}
            ${m.score&&m.score!='0-0'?`<span class="badge b-score">${m.score}</span>`:''}
            ${isHot?'<span class="badge b-hot">🔥 HOT</span>':''}
            <span class="badge b-live">LIVE</span>
            ${pres>0?`<span class="badge b-pressure">${pres}% לחץ</span>`:''}
          </div>
        </div>
        ${pres>0?`<div class="pressure-bar"><div class="pressure-fill" style="width:${pres}%;background:${presColor}"></div></div>`:''}
        <div class="odds-row">
          ${m.over_odd?`<div class="odd-tag"><div class="odd-label">OVER 2.5</div><div class="odd-val">${m.over_odd}</div></div>`:''}
          ${m.draw_odd?`<div class="odd-tag"><div class="odd-label">DRAW</div><div class="odd-val">${m.draw_odd}</div></div>`:''}
          ${ht05}${ht15}
          ${m.held_seconds>0?`<div class="odd-tag"><div class="odd-label">HELD</div><div class="odd-val">${m.held_seconds}s</div></div>`:''}
        </div>
        <div class="verdict ${vc[c]||'v-warn'}">${ic[c]||'🟡'} ${m.verdict} · ${rules}</div>
        ${ai}
      </div>`;
    }).join('');
  }catch(e){console.error(e);}
}

async function loadGoals(){
  try{
    const goals=await fetch('/api/goals').then(r=>r.json());
    const el=document.getElementById('goals-list');
    if(!goals.length){el.innerHTML='<div class="empty"><div class="empty-icon">⚽</div><div>עדיין אין גולים מוקלטים</div></div>';return;}
    el.innerHTML=goals.map(g=>{
      const getOdd=(obj,key)=>{
        if(!obj) return '—';
        const k=Object.keys(obj).find(k=>k.toLowerCase().includes(key.toLowerCase()));
        return k?(+obj[k]).toFixed(2):'—';
      };
      return `<div class="goal-card">
        <div class="goal-header">
          <div class="goal-match">${g.home_team||''} vs ${g.away_team||''}</div>
          <div class="goal-min">⚽ דקה ${g.minute}</div>
        </div>
        <div style="font-size:12px;color:var(--muted);margin-bottom:4px">${g.score_before||'?'} → ${g.score_after||'?'} | ${g.league||''}</div>
        <div class="ot-grid">
          <div class="ot-cell"><div class="ot-label">10s</div><div class="ot-val">${getOdd(g.odds_10s,'over')}</div></div>
          <div class="ot-cell"><div class="ot-label">30s</div><div class="ot-val">${getOdd(g.odds_30s,'over')}</div></div>
          <div class="ot-cell"><div class="ot-label">60s</div><div class="ot-val">${getOdd(g.odds_60s,'over')}</div></div>
          <div class="ot-cell"><div class="ot-label">2m</div><div class="ot-val">${getOdd(g.odds_120s,'over')}</div></div>
          <div class="ot-cell"><div class="ot-label">5m</div><div class="ot-val">${getOdd(g.odds_300s,'over')}</div></div>
        </div>
      </div>`;
    }).join('');
  }catch(e){console.error(e);}
}

async function loadSignals(){
  try{
    const sigs=await fetch('/api/all_signals').then(r=>r.json());
    const el=document.getElementById('signals-list');
    if(!sigs.length){el.innerHTML='<div class="empty"><div class="empty-icon">🔥</div><div>אין אותות</div></div>';return;}
    el.innerHTML=sigs.map(s=>{
      const c=s.signal_type||'warn';
      const pres=s.pressure_score||0;
      return `<div class="sig-card">
        <div class="sig-top">
          <div>
            <div style="font-size:14px;font-weight:600">${s.home_team} vs ${s.away_team}</div>
            <div style="font-size:11px;color:var(--muted)">R${s.rule_num} · ${s.rule_name} · ${s.league||''}</div>
          </div>
          <div style="display:flex;gap:6px;align-items:center">
            ${s.minute>0?`<span class="badge b-min">⏱ ${s.minute}'</span>`:''}
            <span class="verdict ${vc[c]||'v-warn'}" style="padding:4px 10px;font-size:12px">${ic[c]||'🟡'} ${s.verdict}</span>
          </div>
        </div>
        <div style="font-size:12px;color:var(--muted);margin-top:6px">${s.reason}</div>
        ${pres>0?`<div class="progress-bar" style="margin-top:8px"><div class="progress-fill" style="width:${pres}%;background:var(--green)"></div></div><div style="font-size:10px;color:var(--muted);font-family:'JetBrains Mono',monospace">${pres}% לחץ שוק</div>`:''}
      </div>`;
    }).join('');
  }catch(e){console.error(e);}
}

async function loadPaper(){
  try{
    const[stats,trades]=await Promise.all([
      fetch('/api/paper_stats').then(r=>r.json()),
      fetch('/api/paper_trades').then(r=>r.json())
    ]);
    const st=document.getElementById('paper-stats');
    st.innerHTML=`
      <div class="stat-card"><div class="stat-num" style="color:var(--blue)">${stats.total||0}</div><div class="stat-label">Total Paper Actions</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--green)">${stats.wins||0}</div><div class="stat-label">Wins</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--red)">${stats.losses||0}</div><div class="stat-label">Losses</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--yellow)">${(stats.success_rate||0).toFixed(1)}%</div><div class="stat-label">Success Rate</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--purple)">${(stats.virtual_profit||0).toFixed(0)}</div><div class="stat-label">Virtual Profit</div></div>
      <div class="stat-card"><div class="stat-num" style="color:var(--muted)">${stats.open||0}</div><div class="stat-label">Open / Waiting</div></div>`;
    const el=document.getElementById('paper-list');
    if(!trades.length){el.innerHTML='<div class="empty"><div class="empty-icon">🧪</div><div>אין עדיין פעולות דמיוניות. הן ייווצרו אוטומטית מסיגנלים עם confidence 70%+</div></div>';return;}
    el.innerHTML=trades.map(t=>{
      const color=t.result==='win'?'var(--green)':(t.result==='loss'?'var(--red)':'var(--yellow)');
      const label=t.validated?(t.result==='win'?'✅ נתפס':'❌ נכשל'):'⏳ ממתין לבדיקה';
      return `<div class="sig-card">
        <div class="sig-top">
          <div>
            <div style="font-size:14px;font-weight:700">${t.home_team||''} vs ${t.away_team||''}</div>
            <div style="font-size:11px;color:var(--muted)">${t.league||''} · דקה ${t.minute||0} · ${t.score||''}</div>
          </div>
          <span class="badge" style="background:rgba(255,255,255,0.06);color:${color};border:1px solid ${color}">${label}</span>
        </div>
        <div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px">
          <div class="odd-tag"><div class="odd-label">ACTION</div><div class="odd-val">${t.action_type}</div></div>
          ${t.entry_odd?`<div class="odd-tag"><div class="odd-label">ODD</div><div class="odd-val">${Number(t.entry_odd).toFixed(2)}</div></div>`:''}
          <div class="odd-tag"><div class="odd-label">CONF</div><div class="odd-val">${t.confidence||0}%</div></div>
          <div class="odd-tag"><div class="odd-label">P/L</div><div class="odd-val" style="color:${(t.virtual_profit||0)>=0?'var(--green)':'var(--red)'}">${(t.virtual_profit||0).toFixed(0)}</div></div>
        </div>
        <div style="font-size:12px;color:var(--muted);margin-top:8px">${t.reason||''}</div>
      </div>`;
    }).join('');
  }catch(e){console.error(e);}
}


async function loadAnalytics(){
  try{
    const data=await fetch('/api/analytics').then(r=>r.json());
    const el=document.getElementById('analytics-content');
    const targets=[
      {l:"Goals collected",v:data.total_goals,t:500,c:"var(--green)"},
      {l:"Signals collected",v:data.total_signals,t:2000,c:"var(--blue)"},
      {l:"Snapshots saved",v:data.total_snapshots,t:50000,c:"var(--yellow)"}
    ];
    el.innerHTML=`
      <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;margin-bottom:20px">
        <div class="stat-card"><div class="stat-num" style="color:var(--green)">${data.total_goals||0}</div><div class="stat-label">גולים זוהו</div></div>
        <div class="stat-card"><div class="stat-num" style="color:var(--blue)">${data.total_signals||0}</div><div class="stat-label">אותות סה"כ</div></div>
        <div class="stat-card"><div class="stat-num" style="color:var(--yellow)">${(data.total_snapshots||0).toLocaleString()}</div><div class="stat-label">דגימות</div></div>
      </div>
      <div class="match-card" style="margin-bottom:16px">
        <div class="section-title">התקדמות איסוף נתונים</div>
        ${targets.map(t=>`
          <div style="display:flex;justify-content:space-between;margin-top:12px;font-size:12px">
            <span style="color:var(--muted)">${t.l}</span>
            <span style="color:${t.c};font-family:'JetBrains Mono',monospace">${t.v||0} / ${t.t}</span>
          </div>
          <div class="progress-bar"><div class="progress-fill" style="width:${Math.min(100,(t.v||0)/t.t*100)}%;background:${t.c}"></div></div>
        `).join('')}
      </div>
      ${data.top_rules&&data.top_rules.length?`
      <div class="match-card">
        <div class="section-title">החוקים הפעילים ביותר</div>
        ${data.top_rules.map(r=>`
          <div style="display:flex;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--border);font-size:13px">
            <span>R${r.rule_num} ${r.rule_name}</span>
            <span style="color:var(--green);font-family:'JetBrains Mono',monospace">${r.cnt}</span>
          </div>`).join('')}
      </div>`:''}
    `;
  }catch(e){console.error(e);}
}

async function loadAI(){
  try{
    const ins=await fetch('/api/insights').then(r=>r.json());
    const el=document.getElementById('ai-content');
    if(!ins.length){el.innerHTML='<div class="empty"><div class="empty-icon">🤖</div><div>לחץ להרצת ניתוח AI</div></div>';return;}
    el.innerHTML=ins.map(i=>`
      <div class="insight-card">
        <div class="insight-title">🧠 ניתוח שוק</div>
        <div style="font-size:11px;color:var(--muted);margin-bottom:8px;font-family:'JetBrains Mono',monospace">${new Date(i.created_at).toLocaleString('he-IL')} · ${i.goals_analyzed||0} גולים נותחו</div>
        <div class="insight-text">${i.content}</div>
      </div>`).join('');
  }catch(e){console.error(e);}
}

async function runAI(){
  const btn=document.getElementById('ai-btn');
  btn.disabled=true;btn.textContent='⏳ מנתח...';
  try{await fetch('/api/run_ai',{method:'POST'});await loadAI();}catch(e){console.error(e);}
  btn.disabled=false;btn.textContent='🤖 הרץ ניתוח AI עכשיו';
}

async function autoRefresh(){
  if(currentPage==='live') await loadLive();
  else if(currentPage==='paper') await loadPaper();
}
loadLive();
setInterval(autoRefresh,20000);
</script>
</body>
</html>"""

# ─── API Routes ───────────────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/stats")
def api_stats():
    try:
        conn = get_db()
        try:
            r1 = conn.run("SELECT COUNT(DISTINCT match_id) FROM odds_snapshots WHERE captured_at>NOW()-INTERVAL '1 hour' AND is_live=TRUE")
            r2 = conn.run("SELECT COUNT(*) FROM signals WHERE detected_at>NOW()-INTERVAL '30 minutes' AND signal_type='goal' AND confidence>=75")
            r3 = conn.run("SELECT COUNT(*) FROM goals WHERE recorded_at>NOW()-INTERVAL '24 hours'")
            r4 = conn.run("SELECT COUNT(*) FROM odds_snapshots")
            tracked = int(r1[0][0] or 0)
            live_fixtures = int(last_pipeline_stats.get("live_fixtures") or len(live_data) or 0)
            odds_games = int(last_pipeline_stats.get("odds_games") or 0)
            linked_live = int(last_pipeline_stats.get("linked_live") or tracked or 0)
            untracked_live = max(0, live_fixtures - linked_live)
            return jsonify({
                "live": tracked,
                "live_fixtures": live_fixtures,
                "odds_games": odds_games,
                "tracked_with_odds": tracked,
                "linked_live": linked_live,
                "untracked_live": untracked_live,
                "hot_signals": r2[0][0],
                "goals_today": r3[0][0],
                "snapshots": r4[0][0],
                "last_odds_update": last_pipeline_stats.get("last_odds_update"),
                "linked_examples": last_pipeline_stats.get("linked_examples", []),
                "unlinked_examples": last_pipeline_stats.get("unlinked_examples", []),
            })
        finally: conn.close()
    except Exception as e:
        log.error(f"api_stats error: {e}")
        return jsonify({"live":0,"live_fixtures":len(live_data),"odds_games":0,"tracked_with_odds":0,"untracked_live":len(live_data),"hot_signals":0,"goals_today":0,"snapshots":0})

@app.route("/api/signals")
def api_signals():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT DISTINCT ON (match_id, rule_num)
                match_id,home_team,away_team,league,rule_num,rule_name,
                minute,score,signal_type,verdict,confidence,reason,
                over_odd,draw_odd,over05ht_odd,over15ht_odd,
                opening_over05ht,opening_over15ht,
                pressure_score,held_seconds,direction,odd_change,detected_at
                FROM signals WHERE detected_at>NOW()-INTERVAL '30 minutes'
                ORDER BY match_id,rule_num,detected_at DESC LIMIT 40""")
            cols=["match_id","home_team","away_team","league","rule_num","rule_name",
                  "minute","score","signal_type","verdict","confidence","reason",
                  "over_odd","draw_odd","over05ht_odd","over15ht_odd",
                  "opening_over05ht","opening_over15ht",
                  "pressure_score","held_seconds","direction","odd_change","detected_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["detected_at"]=str(r["detected_at"])
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/all_signals")
def api_all_signals():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT match_id,home_team,away_team,league,rule_num,rule_name,
                minute,score,signal_type,verdict,confidence,reason,
                over_odd,draw_odd,pressure_score,held_seconds,direction,odd_change,detected_at
                FROM signals WHERE detected_at>NOW()-INTERVAL '3 hours'
                ORDER BY detected_at DESC LIMIT 100""")
            cols=["match_id","home_team","away_team","league","rule_num","rule_name",
                  "minute","score","signal_type","verdict","confidence","reason",
                  "over_odd","draw_odd","pressure_score","held_seconds","direction","odd_change","detected_at"]
            result=[dict(zip(cols,r)) for r in rows]
            for r in result: r["detected_at"]=str(r["detected_at"])
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/goals")
def api_goals():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT g.match_id,g.minute,g.score_before,g.score_after,
                g.odds_10s,g.odds_30s,g.odds_60s,g.odds_120s,g.odds_300s,
                g.recorded_at,m.home_team,m.away_team,m.league
                FROM goals g LEFT JOIN matches m ON g.match_id=m.match_id
                ORDER BY g.recorded_at DESC LIMIT 50""")
            result=[]
            for r in rows:
                result.append({"match_id":r[0],"minute":r[1],"score_before":r[2],
                               "score_after":r[3],"odds_10s":r[4]or{},"odds_30s":r[5]or{},
                               "odds_60s":r[6]or{},"odds_120s":r[7]or{},"odds_300s":r[8]or{},
                               "recorded_at":str(r[9]),"home_team":r[10]or"",
                               "away_team":r[11]or"","league":r[12]or""})
            return jsonify(result)
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/ai_live")
def api_ai_live():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT content,created_at FROM ai_insights
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
        conn = get_db()
        try:
            r1=conn.run("SELECT COUNT(*) FROM goals")[0][0]
            r2=conn.run("SELECT COUNT(*) FROM signals")[0][0]
            r3=conn.run("SELECT COUNT(*) FROM odds_snapshots")[0][0]
            top=conn.run("""SELECT rule_num,rule_name,COUNT(*) as cnt
                FROM signals GROUP BY rule_num,rule_name ORDER BY cnt DESC LIMIT 10""")
            return jsonify({"total_goals":r1,"total_signals":r2,"total_snapshots":r3,
                           "top_rules":[{"rule_num":r[0],"rule_name":r[1],"cnt":r[2]} for r in top]})
        finally: conn.close()
    except: return jsonify({"total_goals":0,"total_signals":0,"total_snapshots":0})

@app.route("/api/insights")
def api_insights():
    try:
        conn = get_db()
        try:
            rows=conn.run("""SELECT insight_type,content,goals_analyzed,created_at
                FROM ai_insights WHERE insight_type='market_analysis'
                ORDER BY created_at DESC LIMIT 10""")
            return jsonify([{"insight_type":r[0],"content":r[1],"goals_analyzed":r[2],"created_at":str(r[3])} for r in rows])
        finally: conn.close()
    except: return jsonify([])

@app.route("/api/run_ai", methods=["POST"])
def api_run_ai():
    """Run OpenAI analysis, save insights, and store suggested rules as inactive candidates."""
    if not OPENAI_API_KEY:
        msg = "חסר OPENAI_API_KEY ב-Railway. הוסף אותו ב-Variables בשם OPENAI_API_KEY."
        try:
            conn = get_db()
            try:
                conn.run("""INSERT INTO ai_insights (insight_type,content,goals_analyzed)
                    VALUES ('market_analysis',:a,0)""", a=msg)
            finally:
                conn.close()
        except Exception:
            pass
        return jsonify({"error": "Missing OPENAI_API_KEY", "message": msg}), 400

    try:
        conn = get_db()
        try:
            goals = conn.run("""SELECT g.minute,g.score_before,g.score_after,g.odds_30s,g.odds_60s,
                g.recorded_at,m.league,m.home_team,m.away_team
                FROM goals g LEFT JOIN matches m ON g.match_id=m.match_id
                ORDER BY g.recorded_at DESC LIMIT 100""")

            signals = conn.run("""SELECT rule_num,rule_name,signal_type,confidence,pressure_score,minute,
                over_odd,draw_odd,reason,detected_at, validated, goal_10m, false_positive
                FROM signals
                ORDER BY detected_at DESC LIMIT 150""")

            total_snapshots = conn.run("SELECT COUNT(*) FROM odds_snapshots")[0][0]
            total_signals = conn.run("SELECT COUNT(*) FROM signals")[0][0]
            total_goals = conn.run("SELECT COUNT(*) FROM goals")[0][0]
            ht_snapshots = conn.run("SELECT COUNT(*) FROM odds_snapshots WHERE market IN ('over05ht','over15ht')")[0][0]

            validation_summary = "Validation data not available yet."
            pattern_summary = "No pattern_stats rows yet."

            try:
                v = conn.run("""SELECT
                    COUNT(*) FILTER (WHERE validated=TRUE) AS validated,
                    COUNT(*) FILTER (WHERE checked_2m=TRUE) AS checked_2m,
                    COUNT(*) FILTER (WHERE checked_5m=TRUE) AS checked_5m,
                    COUNT(*) FILTER (WHERE checked_10m=TRUE) AS checked_10m,
                    COUNT(*) FILTER (WHERE false_positive=TRUE) AS false_positive
                    FROM signals""")[0]
                validation_summary = (
                    f"validated={v[0] or 0}, checked_2m={v[1] or 0}, "
                    f"checked_5m={v[2] or 0}, checked_10m={v[3] or 0}, "
                    f"false_positive={v[4] or 0}"
                )
            except Exception as e:
                log.warning(f"AI validation summary unavailable: {e}")

            try:
                p = conn.run("""SELECT pattern_id,total_cases,success_rate_5m,success_rate_10m,confidence_level
                    FROM pattern_stats ORDER BY total_cases DESC LIMIT 20""")
                if p:
                    pattern_summary = "\n".join([
                        f"{row[0]} | cases={row[1]} | 5m={round((row[2] or 0),1)}% | 10m={round((row[3] or 0),1)}% | confidence={row[4]}"
                        for row in p
                    ])
            except Exception as e:
                log.warning(f"AI pattern summary unavailable: {e}")

            if total_goals == 0 and total_signals == 0:
                content = (
                    "עדיין אין מספיק נתונים לניתוח AI.\n\n"
                    "המערכת נמצאת במצב Learning Mode. כרגע צריך להמשיך לאסוף:\n"
                    "1. משחקים חיים עם odds\n"
                    "2. snapshots לפני ואחרי שינויי יחס\n"
                    "3. goals מזוהים אוטומטית\n"
                    "4. signals שעוברים validation אחרי 2/5/10 דקות\n\n"
                    "ברגע שיצטברו signals וגולים, OpenAI יוכל להתחיל לזהות דפוסים אמיתיים."
                )
                conn.run("""INSERT INTO ai_insights (insight_type,content,goals_analyzed)
                    VALUES ('market_analysis',:a,0)""", a=content)
                return jsonify({"status":"ok","message":"Not enough data yet","analysis":content,"rules_saved":0})

            goals_lines = []
            for g in goals[:30]:
                o30 = g[3] or {}
                over30 = None
                try:
                    over30 = next((v for k, v in o30.items() if 'over' in str(k).lower()), None)
                except Exception:
                    over30 = None
                goals_lines.append(
                    f"minute={g[0]} | score_before={g[1]} | score_after={g[2]} | over_30s_before={over30 or '?'} | league={g[6] or ''} | match={g[7] or ''} vs {g[8] or ''}"
                )

            signal_lines = []
            for s in signals[:40]:
                signal_lines.append(
                    f"R{s[0]} {s[1]} | type={s[2]} | conf={s[3]} | pressure={s[4]} | minute={s[5]} | over={s[6]} | draw={s[7]} | validated={s[10]} | goal10={s[11]} | fp={s[12]} | reason={s[8]}"
                )

            prompt = f"""אתה PapaGoal AI — מנתח שוק הימורי כדורגל ולמידת דפוסים.

חשוב מאוד:
- המערכת במצב Learning Mode.
- אל תיתן הוראות הימור ישירות.
- אל תגיד ENTER / BET / EXIT.
- תן ניתוח הסתברותי בלבד.
- אם אתה מציע חוקים חדשים, הם רק candidates לבדיקה, לא חוקים פעילים.

מצב הדאטה:
- snapshots: {total_snapshots}
- HT snapshots: {ht_snapshots}
- total signals: {total_signals}
- total goals: {total_goals}
- validation summary: {validation_summary}

PatternStats:
{pattern_summary}

גולים אחרונים:
{chr(10).join(goals_lines) if goals_lines else 'No goals yet'}

Signals אחרונים:
{chr(10).join(signal_lines) if signal_lines else 'No signals yet'}

החזר JSON בלבד במבנה הבא:
{{
  "summary": "סיכום בעברית",
  "strong_patterns": [{{"pattern":"...","reason":"...","confidence":0}}],
  "weak_patterns": [{{"pattern":"...","reason":"..."}}],
  "trap_patterns": [{{"pattern":"...","reason":"..."}}],
  "recommended_adjustments": ["..."],
  "new_rules": [
    {{
      "rule_name": "שם חוק קצר",
      "description": "הסבר בעברית",
      "expected_outcome": "goal",
      "conditions": {{
        "minute_range": "30-40",
        "odds_range": "2.10-2.40",
        "pressure_min": 50,
        "movement": "steady_or_pressure"
      }}
    }}
  ]
}}
"""

            resp = requests.post(
                "https://api.openai.com/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": OPENAI_MODEL,
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 1500,
                    "temperature": 0.2,
                },
                timeout=45,
            )

            if resp.status_code != 200:
                error_text = resp.text[:1000]
                log.error(f"OpenAI API error status={resp.status_code}: {error_text}")
                content = (
                    "OpenAI לא הצליח להריץ ניתוח כרגע.\n\n"
                    f"Status: {resp.status_code}\n"
                    f"Details: {error_text}\n\n"
                    "המערכת ממשיכה לאסוף נתונים כרגיל."
                )
                conn.run("""INSERT INTO ai_insights (insight_type,content,goals_analyzed)
                    VALUES ('market_analysis',:a,:b)""", a=content, b=total_goals)
                return jsonify({"error":"OpenAI API failed","status_code":resp.status_code,"details":error_text,"analysis":content}), 500

            data = resp.json()
            raw = data["choices"][0]["message"]["content"]
            payload = extract_json_object(raw)
            summary = payload.get("summary") if isinstance(payload, dict) else None
            if not summary:
                summary = raw

            rules_saved = save_ai_rule_candidates(conn, payload if isinstance(payload, dict) else {})
            promote_ai_rule_candidates(conn)

            content = summary
            if isinstance(payload, dict):
                extras = []
                for key, title in [
                    ("strong_patterns", "דפוסים חזקים"),
                    ("weak_patterns", "דפוסים חלשים"),
                    ("trap_patterns", "מלכודות"),
                    ("recommended_adjustments", "שיפורים מומלצים"),
                ]:
                    val = payload.get(key) or []
                    if val:
                        extras.append(f"\n{title}:\n{json.dumps(val, ensure_ascii=False, indent=2)}")
                if extras:
                    content += "\n" + "\n".join(extras)

            conn.run("""INSERT INTO ai_insights (insight_type,content,goals_analyzed)
                VALUES ('market_analysis',:a,:b)""", a=content, b=total_goals)

            return jsonify({"status":"ok","analysis":content,"rules_saved":rules_saved,"raw":payload})

        finally:
            conn.close()

    except Exception as e:
        log.exception("run_ai failed")
        return jsonify({"error":"run_ai crashed","details":str(e)}), 500

@app.route("/api/ai_rules")
def api_ai_rules():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT id, created_at, updated_at, rule_name, description,
                    conditions_json, expected_outcome, status, active, total_cases,
                    goals_10m, success_rate_10m, confidence_level, promotion_reason
                FROM ai_rule_candidates
                ORDER BY active DESC, total_cases DESC, updated_at DESC
                LIMIT 200""")
            cols = ["id","created_at","updated_at","rule_name","description","conditions_json",
                    "expected_outcome","status","active","total_cases","goals_10m",
                    "success_rate_10m","confidence_level","promotion_reason"]
            out = []
            for r in rows:
                d = dict(zip(cols, r))
                d["created_at"] = str(d["created_at"])
                d["updated_at"] = str(d["updated_at"])
                out.append(d)
            return jsonify(out)
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e), "rules": []}), 500

@app.route("/api/paper_trades")
def api_paper_trades():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT id, signal_id, match_id, created_at, resolved_at,
                    home_team, away_team, league, minute, score, action_type, market,
                    entry_odd, virtual_stake, virtual_profit, confidence, reason,
                    status, result, goal_within_10m, validated
                FROM simulated_bets
                ORDER BY created_at DESC
                LIMIT 200""")
            cols = ["id","signal_id","match_id","created_at","resolved_at","home_team","away_team","league",
                    "minute","score","action_type","market","entry_odd","virtual_stake","virtual_profit",
                    "confidence","reason","status","result","goal_within_10m","validated"]
            out=[]
            for r in rows:
                d=dict(zip(cols,r))
                d["created_at"]=str(d["created_at"])
                d["resolved_at"]=str(d["resolved_at"]) if d.get("resolved_at") else None
                out.append(d)
            return jsonify(out)
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e), "trades": []}), 500

@app.route("/api/paper_stats")
def api_paper_stats():
    try:
        conn = get_db()
        try:
            total = conn.run("SELECT COUNT(*) FROM simulated_bets")[0][0]
            open_cnt = conn.run("SELECT COUNT(*) FROM simulated_bets WHERE COALESCE(validated,FALSE)=FALSE")[0][0]
            wins = conn.run("SELECT COUNT(*) FROM simulated_bets WHERE result='win'")[0][0]
            losses = conn.run("SELECT COUNT(*) FROM simulated_bets WHERE result='loss'")[0][0]
            profit = conn.run("SELECT COALESCE(SUM(virtual_profit),0) FROM simulated_bets WHERE COALESCE(validated,FALSE)=TRUE")[0][0]
            closed = int(wins or 0) + int(losses or 0)
            rate = (float(wins or 0) / closed * 100.0) if closed else 0.0
            by_action_rows = conn.run("""SELECT action_type, COUNT(*),
                    SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) AS wins
                FROM simulated_bets
                WHERE COALESCE(validated,FALSE)=TRUE
                GROUP BY action_type
                ORDER BY COUNT(*) DESC""")
            by_action=[]
            for a,c,w in by_action_rows:
                c=int(c or 0); w=int(w or 0)
                by_action.append({"action_type":a,"total":c,"wins":w,"success_rate":(w/c*100.0 if c else 0)})
            return jsonify({"total":total,"open":open_cnt,"wins":wins,"losses":losses,
                            "success_rate":rate,"virtual_profit":float(profit or 0),"by_action":by_action})
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e), "total": 0, "open": 0, "wins": 0, "losses": 0, "success_rate": 0, "virtual_profit": 0}), 500

@app.route("/api/validation_stats")
def api_validation_stats():
    try:
        conn = get_db()
        try:
            pending = conn.run("SELECT COUNT(*) FROM signals WHERE COALESCE(validated,FALSE)=FALSE")[0][0]
            partial = conn.run("""SELECT COUNT(*) FROM signals
                WHERE COALESCE(validated,FALSE)=FALSE
                AND (COALESCE(checked_2m,FALSE)=TRUE OR COALESCE(checked_5m,FALSE)=TRUE)""")[0][0]
            full = conn.run("SELECT COUNT(*) FROM signals WHERE COALESCE(validated,FALSE)=TRUE")[0][0]
            patterns = conn.run("SELECT COUNT(*) FROM pattern_stats")[0][0]
            fp = conn.run("SELECT COUNT(*) FROM signals WHERE COALESCE(false_positive,FALSE)=TRUE")[0][0]
            last = conn.run("SELECT MAX(last_updated) FROM pattern_stats")[0][0]
            return jsonify({
                "pending": pending,
                "partial": partial,
                "fully_validated": full,
                "pattern_stats_rows": patterns,
                "false_positives": fp,
                "last_pattern_update": str(last) if last else None
            })
        finally:
            conn.close()
    except Exception as e:
        return jsonify({"error": str(e), "pending": 0, "partial": 0, "fully_validated": 0, "pattern_stats_rows": 0})

@app.route("/api/patterns")
def api_patterns():
    try:
        conn = get_db()
        try:
            rows = conn.run("""SELECT pattern_id, rule_num, rule_name, minute_bucket, odds_bucket,
                    pressure_bucket, duration_bucket, total_cases, goals_2m, goals_5m, goals_10m,
                    no_goal_cases, false_positive_cases, success_rate_2m, success_rate_5m,
                    success_rate_10m, confidence_level, last_updated
                FROM pattern_stats
                ORDER BY total_cases DESC, success_rate_5m DESC
                LIMIT 100""")
            cols = ["pattern_id","rule_num","rule_name","minute_bucket","odds_bucket","pressure_bucket",
                    "duration_bucket","total_cases","goals_2m","goals_5m","goals_10m","no_goal_cases",
                    "false_positive_cases","success_rate_2m","success_rate_5m","success_rate_10m",
                    "confidence_level","last_updated"]
            result = []
            for r in rows:
                d = dict(zip(cols, r))
                d["last_updated"] = str(d["last_updated"])
                result.append(d)
            return jsonify(result)
        finally:
            conn.close()
    except Exception:
        return jsonify([])

@app.route("/health")
def health():
    return jsonify({"status":"ok","version":"v2-betfair",
                   "betfair":bool(betfair_session["token"]),
                   "time":datetime.now(timezone.utc).isoformat()})

# ─── Start ────────────────────────────────────────────────────────────────────
init_db()
_t = threading.Thread(target=collector_loop, daemon=True)
_t.start()
log.info(f"🚀 PapaGoal v2 started | Betfair enabled={USE_BETFAIR}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT, debug=False)
