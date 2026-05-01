# PapaGoal Market Recorder · v5

> Don't predict football. Read the market.

Single-file Flask app that records Bet365 odds via OddsAPI.io every 30s, runs a 19-rule
engine + expected-curve pressure model, captures goal-window odds, paper-trades signals,
and lets Claude propose new rules.

## What's new in v5

- **DB migrations run on boot** — fixes the "goal_time column missing" error from v4 by
  using `ALTER TABLE … ADD COLUMN IF NOT EXISTS` via `_ensure_column()`. Safe to redeploy
  on top of the old DB.
- **Fixed OddsAPI parser** — handles the real v3 shape:
  `bookmakers.Bet365 = [{name:"ML", odds:[…]}, {name:"Over/Under", odds:[{max:2.5,…}, …]}]`.
  Loops the Over/Under entries by `max` (2.5, 3.5) instead of assuming an order. HT lines
  are read from any market whose name contains `1st half` / `first half` / `half time`
  / `1h` / `ht` (case-insensitive).
- **Betfair removed** — OddsAPI provides everything we need (FT odds + HT 0.5 / HT 1.5).
- **All 19 rules live** in one `evaluate_rules()` function with a clean `add()` helper.
- **Expected curves + pressure** computed every snapshot, persisted, and used by rules
  101/102/103/200.
- **Paper trades auto-settle** 5 minutes after entry by checking the `goals` table.
- **7 dashboard pages** with a coherent dark trading-terminal look (Bricolage Grotesque +
  JetBrains Mono).

## Required environment variables

| Var                 | Notes                                              |
|---------------------|----------------------------------------------------|
| `ODDSPAPI_KEY`      | from odds-api.io                                   |
| `FOOTBALL_API_KEY`  | already set: `f3979dd5d8c7d1b4efd239c2b9a8e2a1`    |
| `ANTHROPIC_API_KEY` | optional, but enables /insights and Claude review  |
| `DATABASE_URL`      | `${{Postgres.DATABASE_URL}}` — Railway-provided    |
| `PORT`              | Railway sets it; defaults to 8080                  |
| `SCAN_INTERVAL_SEC` | optional, defaults to 30                           |

The old `BETFAIR_*` vars are no longer used — feel free to delete them from Railway.

## Health check

`GET /health` returns scanner stats + which env vars are wired + every OddsAPI market
name we've encountered so far:

```json
{
  "ok": true,
  "scanner": {"loops": 12, "matches_seen": 47, "snapshots_saved": 188, ...},
  "anthropic": true, "db": true, "oddsapi": true,
  "markets_seen": ["1st Half Over/Under", "ML", "Over/Under"],
  "now": "2026-04-30T..."
}
```

`markets_seen` is the diagnostic you check first if HT odds look empty in the dashboard:
if you don't see anything HT-like in that list, then OddsAPI isn't shipping HT under any
of the names we recognise. The Railway logs also show one line per unique market name,
either `[captured]` or `[SKIPPED]`, the first time it's encountered.

Use `/health` for Railway's healthcheck (already configured in `railway.json`).

## Rules engine

Each rule lives in `evaluate_rules()`:

| #   | Trigger | Verdict |
|-----|---------|---------|
| 1   | Draw 1.57-1.66 + Over 1.83-2.10, min 21-25 | DRAW_UNDER |
| 2   | Over stuck 1.80-1.86, min 26-30 | NO_ENTRY |
| 3   | Over already 1.66-1.75 | TRAP |
| 4   | Over ≥ 2.10, min 30-34 | GOAL |
| 5   | Over ≈ 1.66 | TRAP |
| 6   | Draw 1.61 + Over 1.90 | GOAL |
| 7   | Over ≥ 2.15, min 65-70 | GOAL |
| 8   | Over ≥ 2.80, min 82+ | NO_GOAL |
| 11  | Over ≤ 1.55, min 17-20 | GOAL |
| 12  | Opening Over ≈ 1.30 | GOAL |
| 13  | Over 1.54-1.60 | GOAL |
| 14  | Over 2.30-2.70 held ≥ 2 min | GOAL |
| 15  | Over jumped within 30s | NO_GOAL |
| 16  | Over dropped 0.15+ | GOAL |
| 101 | Over 0.5 HT below curve, min 15-45 | GOAL |
| 102 | Over 1.5 HT below curve | GOAL |
| 103 | Over 2.5 below curve by ≥ 0.80, min 80+ | GOAL |
| 104 | Over 2.7-3.5, min 85-93 | GOAL |
| 200 | Composite pressure ≥ 60% | GOAL |

## Pages

- `/`           Live + hot signals + Claude review entry point
- `/goals`      Odds at -30s/-60s/-2m/-5m before each goal
- `/simulation` Paper trades · pending/hit/miss · P/L
- `/signals`    Rolling 3-hour signal log + "Claude" button per signal
- `/rules`      Toggle on/off, hit rates, "Ask Claude for new rules"
- `/analytics`  Totals + top rules by win rate
- `/insights`   Claude's saved analyses
