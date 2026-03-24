"""
SIS — Signal Intelligence System
Background Market Scanner
Runs every 30 minutes on Railway
"""

import os
import time
import json
import logging
import requests
import schedule
from datetime import datetime, timedelta
from anthropic import Anthropic

# ── Logging ──────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

# ── Config from environment variables ────────────────────────────
ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
POLYGON_KEY    = os.environ.get("POLYGON_API_KEY", "")
FINNHUB_KEY    = os.environ.get("FINNHUB_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
VOLUME_MULT    = float(os.environ.get("VOLUME_MULTIPLIER", "2.0"))
MIN_PRICE      = float(os.environ.get("MIN_PRICE", "5.0"))
MAX_SIGNALS    = int(os.environ.get("MAX_SIGNALS_PER_SCAN", "5"))

# ── Your 9 investment themes ──────────────────────────────────────
THEMES = [
    "AI & Infrastructure",
    "AI Picks & Shovels",
    "Space & Satellite Economy",
    "Nuclear & Next-Gen Energy",
    "Biotech & Longevity",
    "Robotics & Automation",
    "Cyber & Defence Tech",
    "Oil, Gas & Energy Geopolitics",
    "Geopolitical Momentum & Critical Minerals",
]

# ── Known theme mappings (fast local check before AI) ────────────
KNOWN_THEMES = {
    "NVDA": "AI & Infrastructure",
    "PLTR": "AI & Infrastructure",
    "MSFT": "AI & Infrastructure",
    "AMZN": "AI & Infrastructure",
    "GOOGL": "AI & Infrastructure",
    "VRT":  "AI Picks & Shovels",
    "ARST": "AI Picks & Shovels",
    "ARM":  "AI Picks & Shovels",
    "RKLB": "Space & Satellite Economy",
    "SPCE": "Space & Satellite Economy",
    "ASTS": "Space & Satellite Economy",
    "CCJ":  "Nuclear & Next-Gen Energy",
    "OKLO": "Nuclear & Next-Gen Energy",
    "CEG":  "Nuclear & Next-Gen Energy",
    "SMR":  "Nuclear & Next-Gen Energy",
    "EQT":  "Oil, Gas & Energy Geopolitics",
    "LNG":  "Oil, Gas & Energy Geopolitics",
    "CQP":  "Oil, Gas & Energy Geopolitics",
    "RBRK": "Cyber & Defence Tech",
    "CRWD": "Cyber & Defence Tech",
    "LMT":  "Cyber & Defence Tech",
    "RTX":  "Cyber & Defence Tech",
    "GLD":  "Geopolitical Momentum & Critical Minerals",
    "MP":   "Geopolitical Momentum & Critical Minerals",
    "TSLA": "Robotics & Automation",
    "ISRG": "Robotics & Automation",
    "MRNA": "Biotech & Longevity",
    "ILMN": "Biotech & Longevity",
}

# ── Anthropic client ──────────────────────────────────────────────
client = Anthropic(api_key=ANTHROPIC_KEY)

# ── Strategy system prompt ────────────────────────────────────────
STRATEGY_PROMPT = """You are the AI analysis engine of a personal trading system called SIS (Signal Intelligence System).

The trader uses a Signal-Driven Momentum Strategy with these rules:

PORTFOLIO:
- Account A: ~£8,000 (own capital) | Account B: ~£5,000 (brother's capital)
- Total: ~£13,000 | UK-based retail investor
- Position sizing: Speculative £150-300 (A only), Medium £300-500, High £500-900
- Max 12% single trade. Always keep 20% cash.

9 INVESTMENT THEMES:
1. AI & Infrastructure
2. AI Picks & Shovels
3. Space & Satellite Economy
4. Nuclear & Next-Gen Energy
5. Biotech & Longevity
6. Robotics & Automation
7. Cyber & Defence Tech
8. Oil, Gas & Energy Geopolitics
9. Geopolitical Momentum & Critical Minerals

STRATEGY RULES:
- Read the SECOND ORDER — not the obvious trade, who supplies/enables the winner?
- Set stop loss before entry. State maximum £ loss.
- Thesis over price. Know the thesis exit trigger.
- Account B = higher standard. Only medium-to-high conviction.
- Three questions: Why higher? What proves me wrong? Am I early/on-time/late?

MAGIC FORMULA (Greenblatt):
- Return on Capital = EBIT / (Net Working Capital + Net Fixed Assets)
- Earnings Yield = EBIT / Enterprise Value
- High RoC + High EY = ideal combination

CURRENT MACRO CONTEXT (March 2026):
- Hormuz crisis: Strait closed to Western shipping. Qatar LNG offline. Europe buying US LNG.
- Agentic AI: OpenClaw confirmed by Jensen Huang. 1000-1M× more compute than chatbots.
- Petrodollar stress: Yuan settlement growing. Gold primary beneficiary.
- Existing positions: PLTR, RKLB, NVDA (all HOLD strong), RBRK (weak), EQT (active trade)

You will be given a stock ticker, its volume spike data, recent news, and theme classification.
Respond ONLY with a JSON object in this exact format:
{
  "verdict": "STRONG BUY" | "BUY" | "WATCHLIST" | "PASS",
  "conviction": "High" | "Medium" | "Low",
  "theme": "which of the 9 themes",
  "signal_type": "1 - Breaking News" | "2 - Supply Chain" | "3 - AI Prediction",
  "second_order": "one sentence — the non-obvious trade here",
  "why_higher": "one sentence — specific fundamental reason",
  "prove_wrong": "one sentence — specific thing that kills this thesis",
  "timing": "Early" | "On Time" | "Late",
  "entry_price": 123.45,
  "stop_loss": 110.00,
  "stop_loss_gbp": 65,
  "position_size_gbp": 500,
  "account": "A" | "B" | "A only",
  "target_1": 140.00,
  "target_2": 160.00,
  "summary": "2-3 sentence analyst verdict"
}
No other text. Just the JSON."""


# ────────────────────────────────────────────────────────────────
# POLYGON — Market-wide volume scanner
# ────────────────────────────────────────────────────────────────

def get_market_movers():
    """
    Pull top gainers + most active tickers from Polygon.
    Free tier: uses snapshot/gainers and snapshot/losers endpoints.
    Paid tier: uses full market snapshot for comprehensive scan.
    """
    tickers = set()

    # Most active by volume
    urls = [
        f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/gainers?apiKey={POLYGON_KEY}",
        f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/losers?apiKey={POLYGON_KEY}",
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=15)
            data = r.json()
            for item in data.get("tickers", []):
                t = item.get("ticker", "")
                price = item.get("lastTrade", {}).get("p", 0) or item.get("day", {}).get("c", 0)
                if t and price >= MIN_PRICE and len(t) <= 5 and "." not in t:
                    tickers.add(t)
        except Exception as e:
            log.warning(f"Polygon movers fetch error: {e}")

    # Also check full snapshot if on paid tier
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers"
            f"?apiKey={POLYGON_KEY}&include_otc=false",
            timeout=30
        )
        data = r.json()
        if data.get("status") != "ERROR":
            for item in data.get("tickers", []):
                t = item.get("ticker", "")
                day = item.get("day", {})
                prev = item.get("prevDay", {})
                price = day.get("c", 0)
                vol = day.get("v", 0)
                prev_vol = prev.get("v", 1)
                ratio = vol / prev_vol if prev_vol > 0 else 0

                if (t and price >= MIN_PRICE and len(t) <= 5
                        and "." not in t and ratio >= VOLUME_MULT):
                    tickers.add(t)
            log.info(f"Full market scan: {len(tickers)} candidates found")
    except Exception as e:
        log.info(f"Full snapshot not available (free tier): {e}")

    return list(tickers)


def get_ticker_snapshot(ticker):
    """Get detailed snapshot for a specific ticker."""
    try:
        r = requests.get(
            f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}"
            f"?apiKey={POLYGON_KEY}",
            timeout=10
        )
        data = r.json()
        snap = data.get("ticker", {})
        if not snap:
            return None

        day  = snap.get("day", {})
        prev = snap.get("prevDay", {})
        last = snap.get("lastTrade", {})

        vol      = day.get("v", 0)
        prev_vol = prev.get("v", 1)
        price    = last.get("p") or day.get("c", 0)
        prev_c   = prev.get("c", price)

        if not price or price < MIN_PRICE:
            return None

        vol_ratio  = vol / prev_vol if prev_vol > 0 else 0
        price_chg  = ((price - prev_c) / prev_c * 100) if prev_c > 0 else 0

        return {
            "ticker":    ticker,
            "price":     round(price, 2),
            "price_chg": round(price_chg, 2),
            "volume":    int(vol),
            "prev_vol":  int(prev_vol),
            "vol_ratio": round(vol_ratio, 2),
        }
    except Exception as e:
        log.warning(f"Snapshot error {ticker}: {e}")
        return None


# ────────────────────────────────────────────────────────────────
# FINNHUB — News
# ────────────────────────────────────────────────────────────────

def get_news(ticker):
    """Fetch last 48h news for a ticker from Finnhub."""
    try:
        to_date   = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        r = requests.get(
            f"https://finnhub.io/api/v1/company-news"
            f"?symbol={ticker}&from={from_date}&to={to_date}&token={FINNHUB_KEY}",
            timeout=10
        )
        items = r.json()
        if not isinstance(items, list):
            return []
        # Return top 3 headlines
        return [
            {"headline": i.get("headline", ""), "source": i.get("source", "")}
            for i in items[:3]
        ]
    except Exception as e:
        log.warning(f"Finnhub news error {ticker}: {e}")
        return []


# ────────────────────────────────────────────────────────────────
# CLAUDE — Full strategy analysis
# ────────────────────────────────────────────────────────────────

def analyse_with_claude(ticker, snapshot, news, theme):
    """Run full strategy analysis via Claude."""
    news_text = "\n".join([f"- {n['headline']} ({n['source']})" for n in news]) or "No recent news found."

    user_msg = f"""Analyse this signal:

TICKER: {ticker}
CURRENT PRICE: ${snapshot['price']}
PRICE CHANGE TODAY: {snapshot['price_chg']}%
VOLUME: {snapshot['volume']:,} ({snapshot['vol_ratio']}× previous day average)
THEME CLASSIFICATION: {theme}

RECENT NEWS (last 48h):
{news_text}

Run the full strategy analysis and return the JSON verdict."""

    try:
        response = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1000,
            system=STRATEGY_PROMPT,
            messages=[{"role": "user", "content": user_msg}]
        )
        text = response.content[0].text.strip()
        # Strip markdown fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text.strip())
    except json.JSONDecodeError as e:
        log.error(f"Claude JSON parse error for {ticker}: {e}")
        return None
    except Exception as e:
        log.error(f"Claude API error for {ticker}: {e}")
        return None


# ────────────────────────────────────────────────────────────────
# THEME CLASSIFICATION
# ────────────────────────────────────────────────────────────────

def classify_theme(ticker, news):
    """Quick theme classification — local first, then Claude if unknown."""
    if ticker in KNOWN_THEMES:
        return KNOWN_THEMES[ticker]

    # Ask Claude to classify based on news
    if not news:
        return None

    news_text = " | ".join([n["headline"] for n in news[:2]])
    try:
        r = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=100,
            messages=[{
                "role": "user",
                "content": f"""Does this stock fit any of these 9 investment themes?
Themes: {", ".join(THEMES)}
Ticker: {ticker}
News: {news_text}

Reply with ONLY the matching theme name exactly as written, or "NONE" if it doesn't fit any theme."""
            }]
        )
        result = r.content[0].text.strip()
        if result in THEMES:
            return result
        return None
    except Exception:
        return None


# ────────────────────────────────────────────────────────────────
# TELEGRAM — Alert formatting and sending
# ────────────────────────────────────────────────────────────────

def format_telegram_message(ticker, snapshot, analysis):
    """Format a rich Telegram alert message."""
    verdict = analysis.get("verdict", "WATCHLIST")

    emoji = {
        "STRONG BUY": "🟢",
        "BUY":        "🟢",
        "WATCHLIST":  "🟡",
        "PASS":       "🔴",
    }.get(verdict, "⚪")

    chg_sign = "+" if snapshot["price_chg"] >= 0 else ""
    vol_ratio = snapshot["vol_ratio"]

    msg = f"""{emoji} *{verdict} — ${ticker}*
━━━━━━━━━━━━━━━━━━
*Price:* ${snapshot['price']} ({chg_sign}{snapshot['price_chg']}%)
*Volume:* {vol_ratio}× average
*Theme:* {analysis.get('theme', '—')}
*Signal Type:* {analysis.get('signal_type', '—')}
*Conviction:* {analysis.get('conviction', '—')}

*Second Order:*
↳ {analysis.get('second_order', '—')}

*The Three Questions:*
✓ Why higher: {analysis.get('why_higher', '—')}
✓ Prove wrong: {analysis.get('prove_wrong', '—')}
✓ Timing: {analysis.get('timing', '—')}

*Trade Plan:*
Entry: ${analysis.get('entry_price', '—')}
Stop: ${analysis.get('stop_loss', '—')} (max loss ~£{analysis.get('stop_loss_gbp', '—')})
Target 1: ${analysis.get('target_1', '—')}
Target 2: ${analysis.get('target_2', '—')}
Size: £{analysis.get('position_size_gbp', '—')} · Account {analysis.get('account', 'A')}

*Verdict:*
{analysis.get('summary', '—')}

_SIS Scanner · {datetime.now().strftime('%d %b %Y %H:%M')} UTC_"""

    return msg


def send_telegram(message):
    """Send a message via Telegram Bot API."""
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        log.warning("Telegram not configured — skipping alert")
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id":    TELEGRAM_CHAT,
                "text":       message,
                "parse_mode": "Markdown",
            },
            timeout=10
        )
        if r.status_code == 200:
            log.info("Telegram alert sent successfully")
            return True
        else:
            log.error(f"Telegram error: {r.status_code} — {r.text}")
            return False
    except Exception as e:
        log.error(f"Telegram send failed: {e}")
        return False


def send_scan_summary(signals_found, total_scanned):
    """Send a brief summary message even when no signals found."""
    now = datetime.now().strftime("%H:%M UTC")
    if signals_found == 0:
        msg = f"📡 *SIS Scan Complete* — {now}\nScanned {total_scanned} tickers · No signals above threshold"
    else:
        msg = f"📡 *SIS Scan Complete* — {now}\nScanned {total_scanned} tickers · {signals_found} signal(s) sent above"
    send_telegram(msg)


# ────────────────────────────────────────────────────────────────
# MAIN SCAN LOOP
# ────────────────────────────────────────────────────────────────

def run_scan():
    """Main scan function — called every 30 minutes."""
    now = datetime.now()

    # Only scan during market-relevant hours (Mon-Fri, 07:00-23:00 UTC)
    # Covers pre-market, market hours, and after-hours
    if now.weekday() >= 5:  # Saturday=5, Sunday=6
        log.info("Weekend — skipping scan")
        return
    if now.hour < 7 or now.hour >= 23:
        log.info(f"Outside scan hours ({now.hour}:00 UTC) — skipping")
        return

    log.info("=" * 50)
    log.info(f"SIS SCAN STARTING — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 50)

    if not all([ANTHROPIC_KEY, POLYGON_KEY, FINNHUB_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT]):
        log.error("Missing required environment variables — check Railway config")
        return

    # Step 1: Get market movers
    log.info("Fetching market movers from Polygon...")
    candidates = get_market_movers()
    log.info(f"Found {len(candidates)} initial candidates")

    if not candidates:
        log.warning("No candidates returned — check Polygon API key")
        send_telegram("⚠️ *SIS Scanner* — No candidates returned from Polygon. Check API key.")
        return

    # Step 2: Get detailed snapshots and filter by volume
    signals = []
    checked = 0

    for ticker in candidates:
        snapshot = get_ticker_snapshot(ticker)
        if not snapshot:
            continue
        checked += 1

        if snapshot["vol_ratio"] >= VOLUME_MULT:
            log.info(f"Signal candidate: {ticker} — {snapshot['vol_ratio']}× volume, ${snapshot['price']}")
            signals.append(snapshot)

        # Respect rate limits
        time.sleep(0.3)

    log.info(f"Checked {checked} tickers — {len(signals)} volume signals found")

    # Step 3: Sort by volume ratio, take top N
    signals.sort(key=lambda x: x["vol_ratio"], reverse=True)
    signals = signals[:MAX_SIGNALS]

    # Step 4: For each signal — news, theme, analysis, alert
    alerts_sent = 0

    for snap in signals:
        ticker = snap["ticker"]
        log.info(f"Analysing {ticker}...")

        # Get news
        news = get_news(ticker)
        time.sleep(0.2)

        # Classify theme
        theme = classify_theme(ticker, news)
        if not theme:
            log.info(f"{ticker} — no theme match, skipping")
            continue

        log.info(f"{ticker} — theme: {theme}, news items: {len(news)}")

        # Run full AI analysis
        analysis = analyse_with_claude(ticker, snap, news, theme)
        if not analysis:
            log.warning(f"{ticker} — analysis failed")
            continue

        verdict = analysis.get("verdict", "PASS")
        log.info(f"{ticker} — verdict: {verdict} ({analysis.get('conviction', '—')} conviction)")

        # Send alert for BUY and WATCHLIST (not PASS)
        if verdict in ("STRONG BUY", "BUY", "WATCHLIST"):
            msg = format_telegram_message(ticker, snap, analysis)
            if send_telegram(msg):
                alerts_sent += 1
            time.sleep(1)  # Small delay between messages

    # Step 5: Send summary
    send_scan_summary(alerts_sent, checked)

    log.info(f"Scan complete — {alerts_sent} alerts sent")
    log.info("=" * 50)


# ────────────────────────────────────────────────────────────────
# SCHEDULER
# ────────────────────────────────────────────────────────────────

def main():
    log.info("SIS Background Scanner starting...")
    log.info(f"Volume threshold: {VOLUME_MULT}×")
    log.info(f"Min price filter: ${MIN_PRICE}")
    log.info(f"Max signals per scan: {MAX_SIGNALS}")

    # Validate keys on startup
    missing = []
    if not ANTHROPIC_KEY:  missing.append("ANTHROPIC_API_KEY")
    if not POLYGON_KEY:    missing.append("POLYGON_API_KEY")
    if not FINNHUB_KEY:    missing.append("FINNHUB_API_KEY")
    if not TELEGRAM_TOKEN: missing.append("TELEGRAM_BOT_TOKEN")
    if not TELEGRAM_CHAT:  missing.append("TELEGRAM_CHAT_ID")

    if missing:
        log.error(f"Missing environment variables: {', '.join(missing)}")
        log.error("Add these in Railway → Variables before deploying")
        return

    log.info("All keys loaded ✓")
    log.info("Running initial scan on startup...")
    run_scan()

    # Schedule every 30 minutes
    schedule.every(30).minutes.do(run_scan)
    log.info("Scheduler active — scanning every 30 minutes")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
