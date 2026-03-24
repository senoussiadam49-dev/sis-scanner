"""
SIS — Signal Intelligence System
Background Market Scanner v2
Runs every 30 minutes on Railway
Data: Yahoo Finance (free, no API key needed) + Finnhub news
"""

import os
import time
import json
import logging
import requests
import schedule
from datetime import datetime, timedelta
from anthropic import Anthropic

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
log = logging.getLogger(__name__)

ANTHROPIC_KEY  = os.environ.get("ANTHROPIC_API_KEY", "")
FINNHUB_KEY    = os.environ.get("FINNHUB_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT  = os.environ.get("TELEGRAM_CHAT_ID", "")
VOLUME_MULT    = float(os.environ.get("VOLUME_MULTIPLIER", "2.0"))
MIN_PRICE      = float(os.environ.get("MIN_PRICE", "5.0"))
MAX_SIGNALS    = int(os.environ.get("MAX_SIGNALS_PER_SCAN", "5"))

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
    "RKLB":"Space & Satellite Economy","ASTS":"Space & Satellite Economy","SPCE":"Space & Satellite Economy",
    "CCJ":"Nuclear & Next-Gen Energy","OKLO":"Nuclear & Next-Gen Energy","CEG":"Nuclear & Next-Gen Energy",
    "SMR":"Nuclear & Next-Gen Energy","NNE":"Nuclear & Next-Gen Energy","VST":"Nuclear & Next-Gen Energy",
    "EQT":"Oil, Gas & Energy Geopolitics","LNG":"Oil, Gas & Energy Geopolitics",
    "CQP":"Oil, Gas & Energy Geopolitics","AR":"Oil, Gas & Energy Geopolitics",
    "RBRK":"Cyber & Defence Tech","CRWD":"Cyber & Defence Tech","LMT":"Cyber & Defence Tech",
    "RTX":"Cyber & Defence Tech","AXON":"Cyber & Defence Tech","PANW":"Cyber & Defence Tech",
    "GLD":"Geopolitical Momentum & Critical Minerals","MP":"Geopolitical Momentum & Critical Minerals",
    "GDX":"Geopolitical Momentum & Critical Minerals",
    "TSLA":"Robotics & Automation","ISRG":"Robotics & Automation",
    "MRNA":"Biotech & Longevity","ILMN":"Biotech & Longevity","RXRX":"Biotech & Longevity",
}

SCAN_TICKERS = list(set([
    "NVDA","PLTR","RKLB","EQT","RBRK","VRT","MSFT","CCJ","OKLO","LMT","GLD","CRWD",
    "CEG","ARM","ASTS","RTX","AMZN","META","GOOGL","TSLA","MRNA","MP","SMR","ANET",
    "AMD","INTC","NFLX","CRM","ORCL","SNOW","DDOG","PANW","ZS","NET","FTNT",
    "NOC","GD","BA","AXON","SPCE","NNE","VST","TALEN","LNG","CQP","AR","RRC",
    "GDX","GDXJ","UUUU","NXE","SMCI","AVGO","QCOM","MU","MRVL","ISRG","ROK",
    "ILMN","RXRX","BEAM","CRSP","COIN","MSTR","HOOD","SQ","PYPL","OKTA","S",
]))

client = Anthropic(api_key=ANTHROPIC_KEY)

STRATEGY_PROMPT = """You are the AI analysis engine of SIS (Signal Intelligence System).

PORTFOLIO: Account A ~£8,000 (own) | Account B ~£5,000 (brother's, higher standard) | Total ~£13,000
UK retail investor. Position sizing: Speculative £150-300 (A only), Medium £300-500, High £500-900. Max 12% per trade. Min 20% cash.

9 THEMES: AI & Infrastructure | AI Picks & Shovels | Space & Satellite Economy | Nuclear & Next-Gen Energy | Biotech & Longevity | Robotics & Automation | Cyber & Defence Tech | Oil Gas & Energy Geopolitics | Geopolitical Momentum & Critical Minerals

STRATEGY: Read SECOND ORDER — not obvious trade, who supplies/enables? Set stop before entry. Thesis over price. Three questions: Why higher? Prove wrong? Early/On-time/Late?

MAGIC FORMULA: RoC = EBIT/(NWC+Fixed Assets). EY = EBIT/EV. High RoC + High EY = ideal.

MACRO (March 2026): Hormuz crisis — Qatar LNG offline, Europe buying US LNG. Agentic AI (OpenClaw) — 1000x more compute. Petrodollar stress — gold beneficiary. Holdings: PLTR/RKLB/NVDA (HOLD), RBRK (weak), EQT (active trade).

Return ONLY this JSON:
{"verdict":"STRONG BUY"|"BUY"|"WATCHLIST"|"PASS","conviction":"High"|"Medium"|"Low","theme":"theme name","signal_type":"1 - Breaking News"|"2 - Supply Chain"|"3 - AI Prediction","second_order":"one sentence","why_higher":"one sentence","prove_wrong":"one sentence","timing":"Early"|"On Time"|"Late","entry_price":0.00,"stop_loss":0.00,"stop_loss_gbp":0,"position_size_gbp":0,"account":"A"|"B"|"A only","target_1":0.00,"target_2":0.00,"summary":"2-3 sentences"}"""

YAHOO_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

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
        closes  = [c for c in quotes.get("close", []) if c is not None]
        if len(volumes) < 2 or len(closes) < 2:
            return None
        latest_vol = volumes[-1]
        avg_vol = sum(volumes[:-1]) / len(volumes[:-1])
        vol_ratio = latest_vol / avg_vol if avg_vol > 0 else 0
        price = closes[-1]
        prev  = closes[-2]
        chg   = ((price - prev) / prev * 100) if prev > 0 else 0
        if price < MIN_PRICE:
            return None
        return {"ticker": ticker, "price": round(price,2), "price_chg": round(chg,2),
                "volume": int(latest_vol), "avg_vol": int(avg_vol), "vol_ratio": round(vol_ratio,2)}
    except Exception as e:
        log.warning(f"Yahoo error {ticker}: {e}")
        return None

def get_news(ticker):
    try:
        to   = datetime.now().strftime("%Y-%m-%d")
        frm  = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        r = requests.get(f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={frm}&to={to}&token={FINNHUB_KEY}", timeout=10)
        items = r.json()
        if not isinstance(items, list):
            return []
        return [{"headline": i.get("headline",""), "source": i.get("source","")} for i in items[:3]]
    except Exception as e:
        log.warning(f"Finnhub error {ticker}: {e}")
        return []

def classify_theme(ticker, news):
    if ticker in KNOWN_THEMES:
        return KNOWN_THEMES[ticker]
    if not news:
        return None
    try:
        news_text = " | ".join([n["headline"] for n in news[:2]])
        r = client.messages.create(model="claude-sonnet-4-20250514", max_tokens=60,
            messages=[{"role":"user","content":f"Does {ticker} with this news fit any theme: {', '.join(THEMES)}?\nNews: {news_text}\nReply with ONLY the exact theme name or NONE."}])
        result = r.content[0].text.strip()
        return result if result in THEMES else None
    except:
        return None

def analyse(ticker, snap, news, theme):
    news_text = "\n".join([f"- {n['headline']} ({n['source']})" for n in news]) or "No recent news."
    try:
        resp = client.messages.create(model="claude-sonnet-4-20250514", max_tokens=1000,
            system=STRATEGY_PROMPT,
            messages=[{"role":"user","content":f"TICKER: {ticker}\nPRICE: ${snap['price']} ({snap['price_chg']:+.1f}%)\nVOLUME: {snap['volume']:,} ({snap['vol_ratio']}x avg)\nTHEME: {theme}\nNEWS:\n{news_text}\n\nReturn JSON verdict."}])
        text = resp.content[0].text.strip().replace("```json","").replace("```","").strip()
        return json.loads(text)
    except Exception as e:
        log.error(f"Claude error {ticker}: {e}")
        return None

def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown"}, timeout=10)
        return r.status_code == 200
    except:
        return False

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
⏱ Timing: {a.get('timing','—')}

*Trade:* Entry ${a.get('entry_price','—')} · Stop ${a.get('stop_loss','—')} \(~£{a.get('stop_loss_gbp','—')}\)
*Targets:* T1 ${a.get('target_1','—')} · T2 ${a.get('target_2','—')}
*Size:* £{a.get('position_size_gbp','—')} · Acct {a.get('account','A')}

_{a.get('summary','—')}_
_SIS · {datetime.now().strftime('%d %b %H:%M UTC')}_"""

def run_scan():
    now = datetime.now()
    if now.weekday() >= 5:
        log.info("Weekend — skip"); return
    if now.hour < 7 or now.hour >= 23:
        log.info(f"Outside hours — skip"); return

    log.info("="*55)
    log.info(f"SIS SCAN — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("="*55)

    signals = []
    for i, ticker in enumerate(SCAN_TICKERS):
        snap = get_snapshot(ticker)
        if snap and snap["vol_ratio"] >= VOLUME_MULT:
            log.info(f"SIGNAL: {ticker} {snap['vol_ratio']}x vol ${snap['price']} ({snap['price_chg']:+.1f}%)")
            signals.append(snap)
        time.sleep(0.15)
        if (i+1) % 20 == 0:
            log.info(f"Progress: {i+1}/{len(SCAN_TICKERS)}, {len(signals)} signals")

    log.info(f"Scan done: {len(signals)} signals from {len(SCAN_TICKERS)} tickers")

    if not signals:
        send_telegram(f"📡 *SIS Scan* — {now.strftime('%H:%M UTC')}\nNo signals above {VOLUME_MULT}x · {len(SCAN_TICKERS)} tickers scanned")
        return

    signals.sort(key=lambda x: x["vol_ratio"], reverse=True)
    alerts = 0

    for snap in signals[:MAX_SIGNALS]:
        ticker = snap["ticker"]
        news  = get_news(ticker); time.sleep(0.2)
        theme = classify_theme(ticker, news)
        if not theme:
            log.info(f"{ticker} — no theme match, skip"); continue
        a = analyse(ticker, snap, news, theme)
        if not a: continue
        verdict = a.get("verdict","PASS")
        log.info(f"{ticker} — {verdict} ({a.get('conviction','—')})")
        if verdict in ("STRONG BUY","BUY","WATCHLIST"):
            if send_telegram(format_alert(ticker, snap, a)):
                alerts += 1
            time.sleep(1)

    send_telegram(f"📡 *SIS Complete* — {now.strftime('%H:%M UTC')}\n{len(signals)} signals · {alerts} alerts · {len(SCAN_TICKERS)} tickers")
    log.info(f"Done — {alerts} alerts sent")

def main():
    log.info("SIS Scanner v2 starting — Yahoo Finance + Finnhub")
    log.info(f"Scanning {len(SCAN_TICKERS)} tickers, threshold {VOLUME_MULT}x")
    missing = [k for k,v in {"ANTHROPIC_API_KEY":ANTHROPIC_KEY,"FINNHUB_API_KEY":FINNHUB_KEY,"TELEGRAM_BOT_TOKEN":TELEGRAM_TOKEN,"TELEGRAM_CHAT_ID":TELEGRAM_CHAT}.items() if not v]
    if missing:
        log.error(f"Missing env vars: {missing}"); return
    log.info("All keys loaded ✓")
    run_scan()
    schedule.every(30).minutes.do(run_scan)
    log.info("Scheduler active — every 30 minutes")
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
