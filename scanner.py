"""
SIS — Signal Intelligence System
Background Market Scanner v3
- Yahoo Finance volume scanning
- Watchlist price alerts (2%+ moves)
- The Economist RSS signal detection
- Yahoo Finance RSS news signals
- Finnhub company news
- Full Claude strategy analysis + Telegram alerts
"""

import os
import time
import json
import logging
import requests
import schedule
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from anthropic import Anthropic

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

# ── Config ────────────────────────────────────────────────────────
ANTHROPIC_KEY   = os.environ.get("ANTHROPIC_API_KEY", "")
FINNHUB_KEY     = os.environ.get("FINNHUB_API_KEY", "")
TELEGRAM_TOKEN  = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT   = os.environ.get("TELEGRAM_CHAT_ID", "")
VOLUME_MULT     = float(os.environ.get("VOLUME_MULTIPLIER", "1.5"))
MIN_PRICE       = float(os.environ.get("MIN_PRICE", "5.0"))
MAX_SIGNALS     = int(os.environ.get("MAX_SIGNALS_PER_SCAN", "5"))
PRICE_ALERT_PCT = float(os.environ.get("PRICE_ALERT_PCT", "2.0"))

# ── 9 Themes ──────────────────────────────────────────────────────
THEMES = [
    "AI & Infrastructure", "AI Picks & Shovels", "Space & Satellite Economy",
    "Nuclear & Next-Gen Energy", "Biotech & Longevity", "Robotics & Automation",
    "Cyber & Defence Tech", "Oil, Gas & Energy Geopolitics",
    "Geopolitical Momentum & Critical Minerals",
]

KNOWN_THEMES = {
    "NVDA":"AI & Infrastructure","PLTR":"AI & Infrastructure","MSFT":"AI & Infrastructure",
    "AMZN":"AI & Infrastructure","GOOGL":"AI & Infrastructure","META":"AI & Infrastructure",
    "VRT":"AI Picks & Shovels","ARM":"AI Picks & Shovels","ANET":"AI Picks & Shovels",
    "SMCI":"AI Picks & Shovels","AVGO":"AI Picks & Shovels",
    "RKLB":"Space & Satellite Economy","ASTS":"Space & Satellite Economy",
    "CCJ":"Nuclear & Next-Gen Energy","OKLO":"Nuclear & Next-Gen Energy",
    "CEG":"Nuclear & Next-Gen Energy","SMR":"Nuclear & Next-Gen Energy",
    "NNE":"Nuclear & Next-Gen Energy","VST":"Nuclear & Next-Gen Energy",
    "EQT":"Oil, Gas & Energy Geopolitics","LNG":"Oil, Gas & Energy Geopolitics",
    "CQP":"Oil, Gas & Energy Geopolitics","AR":"Oil, Gas & Energy Geopolitics",
    "RBRK":"Cyber & Defence Tech","CRWD":"Cyber & Defence Tech",
    "LMT":"Cyber & Defence Tech","RTX":"Cyber & Defence Tech",
    "AXON":"Cyber & Defence Tech","PANW":"Cyber & Defence Tech",
    "GLD":"Geopolitical Momentum & Critical Minerals",
    "MP":"Geopolitical Momentum & Critical Minerals",
    "GDX":"Geopolitical Momentum & Critical Minerals",
    "TSLA":"Robotics & Automation","ISRG":"Robotics & Automation",
    "MRNA":"Biotech & Longevity","ILMN":"Biotech & Longevity","RXRX":"Biotech & Longevity",
}

# ── Your personal watchlist (price alerts fired on these) ─────────
WATCHLIST = [
    "NVDA","PLTR","RKLB","EQT","RBRK","VRT","CCJ","OKLO","LMT","GLD","CRWD",
]

# ── Full scan list ────────────────────────────────────────────────
SCAN_TICKERS = list(set(WATCHLIST + [
    "MSFT","AMZN","META","GOOGL","AMD","INTC","NFLX","CRM","ORCL","SNOW","DDOG",
    "PANW","ZS","NET","FTNT","NOC","GD","BA","AXON","CEG","SMR","NNE","TALEN",
    "LNG","CQP","AR","RRC","GDX","GDXJ","UUUU","NXE","SMCI","AVGO","QCOM",
    "MU","MRVL","ISRG","ROK","ILMN","RXRX","BEAM","CRSP","COIN","MSTR","HOOD",
    "ARM","ASTS","SPCE","VST","SQ","PYPL","OKTA","S","RTX",
]))

# ── RSS feeds to monitor ──────────────────────────────────────────
RSS_FEEDS = [
    {
        "name": "The Economist",
        "url": "https://www.economist.com/finance-and-economics/rss.xml",
        "emoji": "📰"
    },
    {
        "name": "The Economist — Business",
        "url": "https://www.economist.com/business/rss.xml",
        "emoji": "📰"
    },
    {
        "name": "Yahoo Finance",
        "url": "https://finance.yahoo.com/news/rssindex",
        "emoji": "📈"
    },
    {
        "name": "Yahoo Finance — Markets",
        "url": "https://finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US",
        "emoji": "📈"
    },
]

# Track seen RSS items to avoid duplicate alerts
seen_rss_ids = set()

client = Anthropic(api_key=ANTHROPIC_KEY)

# ── Claude prompts ────────────────────────────────────────────────
STRATEGY_PROMPT = """You are the AI analysis engine of SIS (Signal Intelligence System).

PORTFOLIO: Account A ~£8,000 (own) | Account B ~£5,000 (brother's, higher standard) | Total ~£13,000
UK retail investor. Position sizing: Speculative £150-300 (A only), Medium £300-500, High £500-900. Max 12% per trade. Min 20% cash.

9 THEMES: AI & Infrastructure | AI Picks & Shovels | Space & Satellite Economy | Nuclear & Next-Gen Energy | Biotech & Longevity | Robotics & Automation | Cyber & Defence Tech | Oil Gas & Energy Geopolitics | Geopolitical Momentum & Critical Minerals

STRATEGY: Read SECOND ORDER — not obvious trade, who supplies/enables? Set stop before entry. Thesis over price.
Three questions: Why higher? Prove wrong? Early/On-time/Late?
MAGIC FORMULA: RoC = EBIT/(NWC+Fixed Assets). EY = EBIT/EV.

MACRO (March 2026): Hormuz crisis — Qatar LNG offline, Europe buying US LNG. Agentic AI (OpenClaw) — 1000x more compute. Petrodollar stress — gold beneficiary. Holdings: PLTR/RKLB/NVDA (HOLD), RBRK (weak), EQT (active trade).

Return ONLY this JSON — no other text:
{"verdict":"STRONG BUY"|"BUY"|"WATCHLIST"|"PASS","conviction":"High"|"Medium"|"Low","theme":"theme name","signal_type":"1 - Breaking News"|"2 - Supply Chain"|"3 - AI Prediction","second_order":"one sentence","why_higher":"one sentence","prove_wrong":"one sentence","timing":"Early"|"On Time"|"Late","entry_price":0.00,"stop_loss":0.00,"stop_loss_gbp":0,"position_size_gbp":0,"account":"A"|"B"|"A only","target_1":0.00,"target_2":0.00,"summary":"2-3 sentences"}"""

RSS_PROMPT = """You are a signal-detection engine for a momentum trader.

The trader's 9 investment themes are:
1. AI & Infrastructure
2. AI Picks & Shovels
3. Space & Satellite Economy
4. Nuclear & Next-Gen Energy
5. Biotech & Longevity
6. Robotics & Automation
7. Cyber & Defence Tech
8. Oil, Gas & Energy Geopolitics
9. Geopolitical Momentum & Critical Minerals

Current macro context (March 2026):
- Hormuz crisis: Strait closed, Qatar LNG offline, Europe sourcing US LNG
- Agentic AI (OpenClaw): 1000x more compute demand
- Petrodollar stress: yuan settlement growing, gold beneficiary

For each headline, identify:
1. Does it match any of the 9 themes? If not, ignore it.
2. What is the SECOND ORDER implication — not the obvious trade, but who supplies/enables/benefits indirectly?
3. Signal strength: HIGH (actionable now), MEDIUM (worth watching), LOW (background context)
4. Which specific stock tickers might be affected (if any)

Return ONLY a JSON array of matching items (skip non-matching ones entirely):
[{"headline":"...","source":"...","theme":"exact theme name","second_order":"one sentence","signal_strength":"HIGH"|"MEDIUM"|"LOW","tickers":["TICK1","TICK2"],"action":"one sentence on what to do"}]

If nothing matches any theme, return an empty array: []"""

YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# ────────────────────────────────────────────────────────────────
# TELEGRAM
# ────────────────────────────────────────────────────────────────

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        if r.status_code == 200:
            log.info("Telegram sent ✓")
            return True
        else:
            log.error(f"Telegram error {r.status_code}: {r.text[:200]}")
            return False
    except Exception as e:
        log.error(f"Telegram failed: {e}")
        return False

# ────────────────────────────────────────────────────────────────
# YAHOO FINANCE — Price + Volume data
# ────────────────────────────────────────────────────────────────

def get_snapshot(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=5d&interval=1d&includePrePost=false"
        r = requests.get(url, headers=YAHOO_HEADERS, timeout=10)
        j = r.json()
        result = j.get("chart", {}).get("result", [])
        if not result:
            return None
        quotes = result[0].get("indicators", {}).get("quote", [{}])[0]
        volumes = [v for v in quotes.get("volume", []) if v is not None]
        closes  = [c for c in quotes.get("close",  []) if c is not None]
        if len(volumes) < 2 or len(closes) < 2:
            return None
        latest_vol = volumes[-1]
        avg_vol    = sum(volumes[:-1]) / len(volumes[:-1])
        vol_ratio  = latest_vol / avg_vol if avg_vol > 0 else 0
        price      = closes[-1]
        prev       = closes[-2]
        chg        = ((price - prev) / prev * 100) if prev > 0 else 0
        if price < MIN_PRICE:
            return None
        return {
            "ticker":    ticker,
            "price":     round(price, 2),
            "price_chg": round(chg, 2),
            "volume":    int(latest_vol),
            "avg_vol":   int(avg_vol),
            "vol_ratio": round(vol_ratio, 2),
        }
    except Exception as e:
        log.warning(f"Yahoo snapshot error {ticker}: {e}")
        return None

# ────────────────────────────────────────────────────────────────
# PRICE ALERTS — Watchlist moves 2%+
# ────────────────────────────────────────────────────────────────

def check_price_alerts(snapshots):
    """Fire Telegram alert for any watchlist stock moving 2%+"""
    for snap in snapshots:
        if snap["ticker"] not in WATCHLIST:
            continue
        chg = snap["price_chg"]
        if abs(chg) >= PRICE_ALERT_PCT:
            direction = "📈" if chg >= 0 else "📉"
            sign = "+" if chg >= 0 else ""
            theme = KNOWN_THEMES.get(snap["ticker"], "—")
            msg = f"""{direction} *WATCHLIST ALERT — ${snap['ticker']}*
━━━━━━━━━━━━━━━━━━
*Move:* {sign}{chg}% today
*Price:* ${snap['price']}
*Volume:* {snap['vol_ratio']}x average
*Theme:* {theme}

_Open SIS to analyse →_
_SIS · {datetime.now().strftime('%d %b %H:%M UTC')}_"""
            send_telegram(msg)
            log.info(f"Price alert sent: {snap['ticker']} {sign}{chg}%")
            time.sleep(0.5)

# ────────────────────────────────────────────────────────────────
# RSS SCANNER — Economist + Yahoo Finance
# ────────────────────────────────────────────────────────────────

def fetch_rss(feed):
    """Fetch and parse an RSS feed, return list of items."""
    try:
        r = requests.get(feed["url"], headers=YAHOO_HEADERS, timeout=15)
        root = ET.fromstring(r.content)
        items = []
        # Handle both RSS and Atom formats
        for item in root.findall(".//item")[:15]:
            title = item.findtext("title", "").strip()
            desc  = item.findtext("description", "").strip()
            link  = item.findtext("link", "").strip()
            guid  = item.findtext("guid", link).strip()
            if title and guid not in seen_rss_ids:
                items.append({
                    "headline": title,
                    "summary":  desc[:200] if desc else "",
                    "source":   feed["name"],
                    "guid":     guid,
                    "emoji":    feed["emoji"],
                })
        return items
    except Exception as e:
        log.warning(f"RSS error {feed['name']}: {e}")
        return []

def scan_rss_feeds():
    """Scan all RSS feeds, run Claude signal detection, fire alerts."""
    global seen_rss_ids
    all_items = []

    for feed in RSS_FEEDS:
        items = fetch_rss(feed)
        log.info(f"RSS {feed['name']}: {len(items)} new items")
        all_items.extend(items)
        time.sleep(0.5)

    if not all_items:
        log.info("No new RSS items this scan")
        return

    # Send to Claude for theme matching + second order analysis
    headlines_text = "\n".join([
        f"{i+1}. [{item['source']}] {item['headline']}"
        for i, item in enumerate(all_items)
    ])

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2000,
            messages=[{
                "role": "user",
                "content": f"{RSS_PROMPT}\n\nHeadlines to analyse:\n{headlines_text}"
            }]
        )
        text = resp.content[0].text.strip().replace("```json","").replace("```","").strip()
        signals = json.loads(text)

        if not signals:
            log.info("RSS scan: no theme matches found")
            # Mark all as seen
            for item in all_items:
                seen_rss_ids.add(item["guid"])
            return

        log.info(f"RSS scan: {len(signals)} theme matches found")

        for sig in signals:
            strength = sig.get("signal_strength", "LOW")
            if strength == "LOW":
                # Mark as seen but don't alert
                continue

            emoji = "🔴" if strength == "HIGH" else "🟡"
            tickers = sig.get("tickers", [])
            ticker_str = " · ".join([f"${t}" for t in tickers]) if tickers else "No specific ticker"

            msg = f"""{emoji} *{strength} SIGNAL — {sig.get('theme','—')}*
━━━━━━━━━━━━━━━━━━
*Source:* {sig.get('source','—')}
*Headline:* {sig.get('headline','—')}

*Second Order:*
↳ {sig.get('second_order','—')}

*Action:* {sig.get('action','—')}
*Tickers to watch:* {ticker_str}

_SIS RSS · {datetime.now().strftime('%d %b %H:%M UTC')}_"""

            send_telegram(msg)
            log.info(f"RSS alert: {sig.get('theme')} — {strength}")
            time.sleep(0.5)

        # Mark all items as seen
        for item in all_items:
            seen_rss_ids.add(item["guid"])

        # Keep seen set from growing too large
        if len(seen_rss_ids) > 500:
            seen_rss_ids = set(list(seen_rss_ids)[-250:])

    except json.JSONDecodeError as e:
        log.error(f"RSS Claude JSON error: {e}")
    except Exception as e:
        log.error(f"RSS Claude error: {e}")

# ────────────────────────────────────────────────────────────────
# FINNHUB — Company news
# ────────────────────────────────────────────────────────────────

def get_news(ticker):
    try:
        to  = datetime.now().strftime("%Y-%m-%d")
        frm = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        r = requests.get(
            f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={frm}&to={to}&token={FINNHUB_KEY}",
            timeout=10
        )
        items = r.json()
        if not isinstance(items, list):
            return []
        return [{"headline": i.get("headline",""), "source": i.get("source","")} for i in items[:3]]
    except Exception as e:
        log.warning(f"Finnhub error {ticker}: {e}")
        return []

# ────────────────────────────────────────────────────────────────
# THEME CLASSIFICATION
# ────────────────────────────────────────────────────────────────

def classify_theme(ticker, news):
    if ticker in KNOWN_THEMES:
        return KNOWN_THEMES[ticker]
    if not news:
        return None
    try:
        news_text = " | ".join([n["headline"] for n in news[:2]])
        r = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=60,
            messages=[{"role":"user","content":
                f"Does {ticker} with this news fit any theme: {', '.join(THEMES)}?\nNews: {news_text}\nReply with ONLY the exact theme name or NONE."}]
        )
        result = r.content[0].text.strip()
        return result if result in THEMES else None
    except:
        return None

# ────────────────────────────────────────────────────────────────
# CLAUDE — Full strategy analysis
# ────────────────────────────────────────────────────────────────

def analyse(ticker, snap, news, theme):
    news_text = "\n".join([f"- {n['headline']} ({n['source']})" for n in news]) or "No recent news."
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=1000,
            system=STRATEGY_PROMPT,
            messages=[{"role":"user","content":
                f"TICKER: {ticker}\nPRICE: ${snap['price']} ({snap['price_chg']:+.1f}%)\nVOLUME: {snap['volume']:,} ({snap['vol_ratio']}x avg)\nTHEME: {theme}\nNEWS:\n{news_text}\n\nReturn JSON verdict."}]
        )
        text = resp.content[0].text.strip().replace("```json","").replace("```","").strip()
        return json.loads(text)
    except Exception as e:
        log.error(f"Claude error {ticker}: {e}")
        return None

def format_alert(ticker, snap, a):
    v = a.get("verdict","WATCHLIST")
    e = {"STRONG BUY":"🟢","BUY":"🟢","WATCHLIST":"🟡","PASS":"🔴"}.get(v,"⚪")
    s = "+" if snap["price_chg"] >= 0 else ""
    return f"""{e} *{v} — ${ticker}*
━━━━━━━━━━━━━━━━━━
*Price:* ${snap['price']} ({s}{snap['price_chg']}%)
*Volume:* {snap['vol_ratio']}x average
*Theme:* {a.get('theme','—')}
*Conviction:* {a.get('conviction','—')}

↳ {a.get('second_order','—')}

*Three Questions:*
✓ {a.get('why_higher','—')}
✗ {a.get('prove_wrong','—')}
⏱ {a.get('timing','—')}

*Trade:* Entry ${a.get('entry_price','—')} · Stop ${a.get('stop_loss','—')} \(~£{a.get('stop_loss_gbp','—')}\)
*Targets:* T1 ${a.get('target_1','—')} · T2 ${a.get('target_2','—')}
*Size:* £{a.get('position_size_gbp','—')} · Acct {a.get('account','A')}

_{a.get('summary','—')}_
_SIS · {datetime.now().strftime('%d %b %H:%M UTC')}_"""

# ────────────────────────────────────────────────────────────────
# MAIN SCAN
# ────────────────────────────────────────────────────────────────

def run_scan():
    now = datetime.now()
    if now.weekday() >= 5:
        log.info("Weekend — skip"); return
    if now.hour < 7 or now.hour >= 23:
        log.info(f"Outside hours ({now.hour}:00 UTC) — skip"); return

    log.info("="*55)
    log.info(f"SIS SCAN v3 — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("="*55)

    # ── Part 1: RSS feeds (Economist + Yahoo Finance news) ────────
    log.info("--- RSS SCAN ---")
    scan_rss_feeds()

    # ── Part 2: Volume + price scan ───────────────────────────────
    log.info("--- VOLUME + PRICE SCAN ---")
    all_snapshots = []
    vol_signals = []

    for i, ticker in enumerate(SCAN_TICKERS):
        snap = get_snapshot(ticker)
        if snap:
            all_snapshots.append(snap)
            if snap["vol_ratio"] >= VOLUME_MULT:
                log.info(f"VOL SIGNAL: {ticker} {snap['vol_ratio']}x · ${snap['price']} ({snap['price_chg']:+.1f}%)")
                vol_signals.append(snap)
        time.sleep(0.15)
        if (i+1) % 20 == 0:
            log.info(f"Progress: {i+1}/{len(SCAN_TICKERS)}, {len(vol_signals)} vol signals")

    log.info(f"Volume scan: {len(vol_signals)} signals from {len(all_snapshots)} tickers")

    # ── Part 3: Price alerts for watchlist ────────────────────────
    log.info("--- PRICE ALERTS ---")
    check_price_alerts(all_snapshots)

    # ── Part 4: Full AI analysis on top volume signals ────────────
    log.info("--- AI ANALYSIS ---")
    if not vol_signals:
        log.info("No volume signals to analyse")
        send_telegram(
            f"📡 *SIS Scan v3* — {now.strftime('%H:%M UTC')}\n"
            f"No volume signals above {VOLUME_MULT}x\n"
            f"{len(SCAN_TICKERS)} tickers · RSS feeds checked ✓"
        )
        return

    vol_signals.sort(key=lambda x: x["vol_ratio"], reverse=True)
    alerts = 0

    for snap in vol_signals[:MAX_SIGNALS]:
        ticker = snap["ticker"]
        news   = get_news(ticker); time.sleep(0.2)
        theme  = classify_theme(ticker, news)
        if not theme:
            log.info(f"{ticker} — no theme, skip"); continue
        a = analyse(ticker, snap, news, theme)
        if not a: continue
        verdict = a.get("verdict","PASS")
        log.info(f"{ticker} — {verdict} ({a.get('conviction','—')})")
        if verdict in ("STRONG BUY","BUY","WATCHLIST"):
            if send_telegram(format_alert(ticker, snap, a)):
                alerts += 1
            time.sleep(1)

    send_telegram(
        f"📡 *SIS Complete* — {now.strftime('%H:%M UTC')}\n"
        f"Vol signals: {len(vol_signals)} · Alerts: {alerts}\n"
        f"Tickers scanned: {len(SCAN_TICKERS)} · RSS: ✓"
    )
    log.info(f"Scan done — {alerts} alerts sent")

# ────────────────────────────────────────────────────────────────
# ENTRY POINT
# ────────────────────────────────────────────────────────────────

def main():
    log.info("SIS Scanner v3 starting...")
    log.info(f"Tickers: {len(SCAN_TICKERS)} | Vol threshold: {VOLUME_MULT}x | Price alert: {PRICE_ALERT_PCT}%")
    log.info(f"RSS feeds: {len(RSS_FEEDS)} ({', '.join([f['name'] for f in RSS_FEEDS])})")

    missing = [k for k,v in {
        "ANTHROPIC_API_KEY":  ANTHROPIC_KEY,
        "FINNHUB_API_KEY":    FINNHUB_KEY,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT,
    }.items() if not v]

    if missing:
        log.error(f"Missing env vars: {missing}"); return

    log.info("All keys loaded ✓")
    log.info("Running initial scan on startup...")
    run_scan()

    schedule.every(30).minutes.do(run_scan)
    log.info("Scheduler active — every 30 minutes, Mon-Fri 07:00-23:00 UTC")

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
