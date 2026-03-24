"""
SIS — Signal Intelligence System
Background Market Scanner v5
- ~300 tickers across all 9 themes
- Auto-watchlist: AI adds tickers from news signals
- Watchlist price alerts (2%+)
- The Economist + Yahoo Finance RSS
- Finnhub company news
- Full Claude strategy analysis
- Telegram alerts
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

# ── Theme map ─────────────────────────────────────────────────────
KNOWN_THEMES = {
    # AI & Infrastructure
    "NVDA":"AI & Infrastructure","MSFT":"AI & Infrastructure","GOOGL":"AI & Infrastructure",
    "GOOG":"AI & Infrastructure","AMZN":"AI & Infrastructure","META":"AI & Infrastructure",
    "ORCL":"AI & Infrastructure","IBM":"AI & Infrastructure","CRM":"AI & Infrastructure",
    "NOW":"AI & Infrastructure","ADBE":"AI & Infrastructure","INTU":"AI & Infrastructure",
    "PLTR":"AI & Infrastructure","SNOW":"AI & Infrastructure","DDOG":"AI & Infrastructure",
    "MDB":"AI & Infrastructure","WDAY":"AI & Infrastructure","AI":"AI & Infrastructure",
    "BBAI":"AI & Infrastructure","SOUN":"AI & Infrastructure","PSTG":"AI & Infrastructure",
    "GTLB":"AI & Infrastructure","CFLT":"AI & Infrastructure","ESTC":"AI & Infrastructure",
    "DOCN":"AI & Infrastructure","FSLY":"AI & Infrastructure","NET":"AI & Infrastructure",
    # AI Picks & Shovels — chips
    "AMD":"AI Picks & Shovels","INTC":"AI Picks & Shovels","AVGO":"AI Picks & Shovels",
    "QCOM":"AI Picks & Shovels","MU":"AI Picks & Shovels","MRVL":"AI Picks & Shovels",
    "ARM":"AI Picks & Shovels","ANET":"AI Picks & Shovels","SMCI":"AI Picks & Shovels",
    "VRT":"AI Picks & Shovels","LRCX":"AI Picks & Shovels","AMAT":"AI Picks & Shovels",
    "KLAC":"AI Picks & Shovels","ASML":"AI Picks & Shovels","TSM":"AI Picks & Shovels",
    "ONTO":"AI Picks & Shovels","ACMR":"AI Picks & Shovels","COHU":"AI Picks & Shovels",
    "LITE":"AI Picks & Shovels","VIAV":"AI Picks & Shovels","WOLF":"AI Picks & Shovels",
    "NXPI":"AI Picks & Shovels","SWKS":"AI Picks & Shovels","QRVO":"AI Picks & Shovels",
    "MPWR":"AI Picks & Shovels","FORM":"AI Picks & Shovels","ACLS":"AI Picks & Shovels",
    "CAMT":"AI Picks & Shovels","AMBA":"AI Picks & Shovels","SITM":"AI Picks & Shovels",
    # Space & Satellite
    "RKLB":"Space & Satellite Economy","ASTS":"Space & Satellite Economy",
    "SPCE":"Space & Satellite Economy","LUNR":"Space & Satellite Economy",
    "RDW":"Space & Satellite Economy","KTOS":"Space & Satellite Economy",
    "IRDM":"Space & Satellite Economy","VSAT":"Space & Satellite Economy",
    "GSAT":"Space & Satellite Economy","MNTS":"Space & Satellite Economy",
    "SATL":"Space & Satellite Economy","AJRD":"Space & Satellite Economy",
    "VORB":"Space & Satellite Economy","BKSY":"Space & Satellite Economy",
    "PL":"Space & Satellite Economy","SPIR":"Space & Satellite Economy",
    # Nuclear & Next-Gen Energy
    "CCJ":"Nuclear & Next-Gen Energy","OKLO":"Nuclear & Next-Gen Energy",
    "CEG":"Nuclear & Next-Gen Energy","SMR":"Nuclear & Next-Gen Energy",
    "NNE":"Nuclear & Next-Gen Energy","VST":"Nuclear & Next-Gen Energy",
    "TALEN":"Nuclear & Next-Gen Energy","BWX":"Nuclear & Next-Gen Energy",
    "UUUU":"Nuclear & Next-Gen Energy","NXE":"Nuclear & Next-Gen Energy",
    "DNN":"Nuclear & Next-Gen Energy","UEC":"Nuclear & Next-Gen Energy",
    "URG":"Nuclear & Next-Gen Energy","LEU":"Nuclear & Next-Gen Energy",
    "BWXT":"Nuclear & Next-Gen Energy","GEV":"Nuclear & Next-Gen Energy",
    "ETR":"Nuclear & Next-Gen Energy","PCG":"Nuclear & Next-Gen Energy",
    "EXC":"Nuclear & Next-Gen Energy","PPL":"Nuclear & Next-Gen Energy",
    "AEE":"Nuclear & Next-Gen Energy","PALAF":"Nuclear & Next-Gen Energy",
    # Robotics & Automation
    "TSLA":"Robotics & Automation","ISRG":"Robotics & Automation",
    "ROK":"Robotics & Automation","EMR":"Robotics & Automation",
    "HON":"Robotics & Automation","PATH":"Robotics & Automation",
    "BRZE":"Robotics & Automation","AZTA":"Robotics & Automation",
    "ACHR":"Robotics & Automation","JOBY":"Robotics & Automation",
    "IRBT":"Robotics & Automation","NNDM":"Robotics & Automation",
    "XMTR":"Robotics & Automation","TNDM":"Robotics & Automation",
    "IIPR":"Robotics & Automation","BLDE":"Robotics & Automation",
    "RBOT":"Robotics & Automation","MBOT":"Robotics & Automation",
    # Cyber & Defence
    "LMT":"Cyber & Defence Tech","RTX":"Cyber & Defence Tech",
    "NOC":"Cyber & Defence Tech","GD":"Cyber & Defence Tech",
    "BA":"Cyber & Defence Tech","AXON":"Cyber & Defence Tech",
    "CRWD":"Cyber & Defence Tech","PANW":"Cyber & Defence Tech",
    "ZS":"Cyber & Defence Tech","FTNT":"Cyber & Defence Tech",
    "OKTA":"Cyber & Defence Tech","S":"Cyber & Defence Tech",
    "RBRK":"Cyber & Defence Tech","TENB":"Cyber & Defence Tech",
    "CYBR":"Cyber & Defence Tech","RPD":"Cyber & Defence Tech",
    "QLYS":"Cyber & Defence Tech","HXL":"Cyber & Defence Tech",
    "TXT":"Cyber & Defence Tech","HII":"Cyber & Defence Tech",
    "LDOS":"Cyber & Defence Tech","SAIC":"Cyber & Defence Tech",
    "BAH":"Cyber & Defence Tech","CACI":"Cyber & Defence Tech",
    "MANT":"Cyber & Defence Tech","DRS":"Cyber & Defence Tech",
    "KTOS":"Cyber & Defence Tech","HIMS":"Cyber & Defence Tech",
    "DFEN":"Cyber & Defence Tech","VEC":"Cyber & Defence Tech",
    # Oil Gas & Geopolitics
    "EQT":"Oil, Gas & Energy Geopolitics","LNG":"Oil, Gas & Energy Geopolitics",
    "CQP":"Oil, Gas & Energy Geopolitics","AR":"Oil, Gas & Energy Geopolitics",
    "RRC":"Oil, Gas & Energy Geopolitics","SWN":"Oil, Gas & Energy Geopolitics",
    "CNX":"Oil, Gas & Energy Geopolitics","CTRA":"Oil, Gas & Energy Geopolitics",
    "DVN":"Oil, Gas & Energy Geopolitics","EOG":"Oil, Gas & Energy Geopolitics",
    "COP":"Oil, Gas & Energy Geopolitics","XOM":"Oil, Gas & Energy Geopolitics",
    "CVX":"Oil, Gas & Energy Geopolitics","OXY":"Oil, Gas & Energy Geopolitics",
    "HAL":"Oil, Gas & Energy Geopolitics","SLB":"Oil, Gas & Energy Geopolitics",
    "BKR":"Oil, Gas & Energy Geopolitics","MPC":"Oil, Gas & Energy Geopolitics",
    "VLO":"Oil, Gas & Energy Geopolitics","PSX":"Oil, Gas & Energy Geopolitics",
    "KMI":"Oil, Gas & Energy Geopolitics","WMB":"Oil, Gas & Energy Geopolitics",
    "ET":"Oil, Gas & Energy Geopolitics","TRGP":"Oil, Gas & Energy Geopolitics",
    "OKE":"Oil, Gas & Energy Geopolitics","MMP":"Oil, Gas & Energy Geopolitics",
    # Geopolitical & Critical Minerals
    "GLD":"Geopolitical Momentum & Critical Minerals",
    "GDX":"Geopolitical Momentum & Critical Minerals",
    "GDXJ":"Geopolitical Momentum & Critical Minerals",
    "SLV":"Geopolitical Momentum & Critical Minerals",
    "MP":"Geopolitical Momentum & Critical Minerals",
    "FCX":"Geopolitical Momentum & Critical Minerals",
    "TECK":"Geopolitical Momentum & Critical Minerals",
    "RIO":"Geopolitical Momentum & Critical Minerals",
    "BHP":"Geopolitical Momentum & Critical Minerals",
    "VALE":"Geopolitical Momentum & Critical Minerals",
    "AA":"Geopolitical Momentum & Critical Minerals",
    "ALB":"Geopolitical Momentum & Critical Minerals",
    "LAC":"Geopolitical Momentum & Critical Minerals",
    "SQM":"Geopolitical Momentum & Critical Minerals",
    "LTHM":"Geopolitical Momentum & Critical Minerals",
    "PLL":"Geopolitical Momentum & Critical Minerals",
    "NEM":"Geopolitical Momentum & Critical Minerals",
    "AEM":"Geopolitical Momentum & Critical Minerals",
    "WPM":"Geopolitical Momentum & Critical Minerals",
    "GOLD":"Geopolitical Momentum & Critical Minerals",
    "KGC":"Geopolitical Momentum & Critical Minerals",
    "AG":"Geopolitical Momentum & Critical Minerals",
    "PAAS":"Geopolitical Momentum & Critical Minerals",
    "CDE":"Geopolitical Momentum & Critical Minerals",
}

# ── Personal watchlist — price alerts + core holdings ─────────────
CORE_WATCHLIST = [
    "NVDA","PLTR","RKLB","EQT","RBRK","VRT","CCJ","OKLO","LMT","GLD","CRWD","RTX","CEG",
]

# ── Base scan list (~300 tickers) ─────────────────────────────────
BASE_SCAN_TICKERS = list(set([
    # AI & Infrastructure
    "NVDA","MSFT","GOOGL","GOOG","AMZN","META","ORCL","IBM","CRM","NOW",
    "ADBE","INTU","PLTR","SNOW","DDOG","MDB","WDAY","AI","BBAI","SOUN",
    "PSTG","GTLB","CFLT","ESTC","DOCN","NET","FSLY",
    # AI Picks & Shovels
    "AMD","INTC","AVGO","QCOM","MU","MRVL","ARM","ANET","SMCI","VRT",
    "LRCX","AMAT","KLAC","ASML","TSM","ONTO","ACMR","COHU","LITE","VIAV",
    "WOLF","NXPI","SWKS","MPWR","FORM","ACLS","CAMT","AMBA","SITM",
    # Space
    "RKLB","ASTS","SPCE","LUNR","RDW","KTOS","IRDM","VSAT","GSAT",
    "MNTS","VORB","BKSY","PL","SPIR",
    # Nuclear & Energy
    "CCJ","OKLO","CEG","SMR","NNE","VST","TALEN","BWX","UUUU","NXE",
    "DNN","UEC","URG","LEU","BWXT","GEV","ETR","PCG","EXC","PPL","AEE",
    # Robotics
    "TSLA","ISRG","ROK","EMR","HON","PATH","BRZE","AZTA","ACHR","JOBY",
    "IRBT","NNDM","BLDE","RBOT","XMTR",
    # Cyber & Defence
    "LMT","RTX","NOC","GD","BA","AXON","CRWD","PANW","ZS","FTNT",
    "OKTA","S","RBRK","TENB","CYBR","RPD","QLYS","HXL","TXT","HII",
    "LDOS","SAIC","BAH","CACI","MANT","DRS","VEC",
    # Oil & Gas
    "EQT","LNG","CQP","AR","RRC","SWN","CNX","CTRA","DVN","EOG",
    "COP","XOM","CVX","OXY","HAL","SLB","BKR","MPC","VLO","PSX",
    "KMI","WMB","ET","TRGP","OKE",
    # Critical Minerals & Macro
    "GLD","GDX","GDXJ","SLV","MP","FCX","TECK","RIO","BHP","VALE",
    "AA","ALB","LAC","SQM","LTHM","PLL","NEM","AEM","WPM","GOLD",
    "KGC","AG","PAAS","CDE",
    # Theme ETFs
    "AIQ","BOTZ","ROBO","IRBO","UFO","ARKX","URA","URNM","NLR",
    "XOP","XLE","OIH","FCG","HACK","BUG","CIBR","IHAK",
    "ITA","PPA","XAR","RING","COPX","LIT","REMX",
    "ARKK","ARKG","ARKQ","ARKW",
    # Broader market & macro
    "SPY","QQQ","IWM","DIA","TLT","GLD","SLV",
    "NFLX","UBER","ABNB","COIN","MSTR","HOOD","SQ",
    "MRNA","ILMN","RXRX","BEAM","CRSP","EDIT","NTLA","PACB",
    "JPM","GS","MS","BAC","WFC","BRK-B",
]))

# ── Dynamic watchlist — auto-populated by AI from news ────────────
# Stored in memory during session, persists via simple file on Railway
DYNAMIC_WATCHLIST_FILE = "/tmp/sis_dynamic_watchlist.json"

def load_dynamic_watchlist():
    try:
        with open(DYNAMIC_WATCHLIST_FILE, "r") as f:
            return json.load(f)
    except:
        return {}

def save_dynamic_watchlist(wl):
    try:
        with open(DYNAMIC_WATCHLIST_FILE, "w") as f:
            json.dump(wl, f)
    except Exception as e:
        log.error(f"Save dynamic watchlist error: {e}")

def add_to_dynamic_watchlist(ticker, reason, theme):
    wl = load_dynamic_watchlist()
    if ticker not in wl:
        wl[ticker] = {
            "reason": reason,
            "theme": theme,
            "added": datetime.now().strftime("%Y-%m-%d %H:%M UTC")
        }
        save_dynamic_watchlist(wl)
        log.info(f"Auto-added to watchlist: {ticker} — {reason}")
        send_telegram(
            f"🔍 *AUTO-ADDED TO WATCHLIST*\n"
            f"━━━━━━━━━━━━━━━━━━\n"
            f"*Ticker:* ${ticker}\n"
            f"*Theme:* {theme}\n"
            f"*Reason:* {reason}\n"
            f"*Added:* {datetime.now().strftime('%d %b %H:%M UTC')}\n\n"
            f"_Will be monitored from next scan onwards_\n"
            f"_SIS Auto-Watchlist_"
        )
        return True
    return False

def get_full_scan_list():
    """Combine base list + dynamic watchlist tickers."""
    dynamic = load_dynamic_watchlist()
    dynamic_tickers = list(dynamic.keys())
    combined = list(set(BASE_SCAN_TICKERS + CORE_WATCHLIST + dynamic_tickers))
    return combined

def get_full_watchlist():
    """Core watchlist + dynamic watchlist for price alerts."""
    dynamic = load_dynamic_watchlist()
    return list(set(CORE_WATCHLIST + list(dynamic.keys())))

# ── RSS feeds ─────────────────────────────────────────────────────
RSS_FEEDS = [
    {"name":"The Economist — Finance","url":"https://www.economist.com/finance-and-economics/rss.xml","emoji":"📰"},
    {"name":"The Economist — Business","url":"https://www.economist.com/business/rss.xml","emoji":"📰"},
    {"name":"Yahoo Finance","url":"https://finance.yahoo.com/news/rssindex","emoji":"📈"},
    {"name":"Yahoo Finance Markets","url":"https://finance.yahoo.com/rss/2.0/headline?s=^GSPC&region=US&lang=en-US","emoji":"📈"},
]

seen_rss_ids = set()
client = Anthropic(api_key=ANTHROPIC_KEY)

# ── Prompts ───────────────────────────────────────────────────────
STRATEGY_PROMPT = """You are the AI analysis engine of SIS (Signal Intelligence System).
PORTFOLIO: Account A ~£8,000 (own) | Account B ~£5,000 (brother's) | Total ~£13,000
UK retail investor. Position sizing: Speculative £150-300 (A only), Medium £300-500, High £500-900. Max 12% per trade. Min 20% cash.
9 THEMES: AI & Infrastructure | AI Picks & Shovels | Space & Satellite Economy | Nuclear & Next-Gen Energy | Biotech & Longevity | Robotics & Automation | Cyber & Defence Tech | Oil Gas & Energy Geopolitics | Geopolitical Momentum & Critical Minerals
STRATEGY: Read SECOND ORDER. Set stop before entry. Thesis over price. Three questions: Why higher? Prove wrong? Early/On-time/Late?
MACRO (March 2026): Hormuz crisis — Qatar LNG offline, Europe buying US LNG. Agentic AI (OpenClaw) — 1000x more compute. Petrodollar stress — gold beneficiary. Holdings: PLTR/RKLB/NVDA (HOLD), RBRK (weak), EQT (active).
Return ONLY JSON: {"verdict":"STRONG BUY"|"BUY"|"WATCHLIST"|"PASS","conviction":"High"|"Medium"|"Low","theme":"name","signal_type":"1 - Breaking News"|"2 - Supply Chain"|"3 - AI Prediction","second_order":"one sentence","why_higher":"one sentence","prove_wrong":"one sentence","timing":"Early"|"On Time"|"Late","entry_price":0.00,"stop_loss":0.00,"stop_loss_gbp":0,"position_size_gbp":0,"account":"A"|"B"|"A only","target_1":0.00,"target_2":0.00,"summary":"2-3 sentences"}"""

RSS_PROMPT = """You are a signal-detection engine for a momentum trader.
9 investment themes:
1. AI & Infrastructure  2. AI Picks & Shovels  3. Space & Satellite Economy
4. Nuclear & Next-Gen Energy  5. Biotech & Longevity  6. Robotics & Automation
7. Cyber & Defence Tech  8. Oil, Gas & Energy Geopolitics  9. Geopolitical Momentum & Critical Minerals

Macro (March 2026): Hormuz crisis, Agentic AI compute boom, petrodollar stress, gold beneficiary.

For each headline: does it match a theme? Identify the SECOND ORDER implication (not the obvious trade).
For HIGH conviction signals, identify specific stock tickers that should be added to a watchlist.

Return ONLY a JSON array of theme matches:
[{
  "headline": "...",
  "source": "...",
  "theme": "exact theme name",
  "second_order": "one sentence — non-obvious implication",
  "signal_strength": "HIGH"|"MEDIUM"|"LOW",
  "tickers": ["TICK1","TICK2"],
  "watchlist_add": [{"ticker":"TICK","reason":"one sentence why"}],
  "action": "one sentence on what to do"
}]
If nothing matches return []"""

YAHOO_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# ────────────────────────────────────────────────────────────────
# TELEGRAM
# ────────────────────────────────────────────────────────────────
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHAT,"text":msg,"parse_mode":"Markdown"},
            timeout=10
        )
        if r.status_code == 200:
            return True
        log.error(f"Telegram {r.status_code}: {r.text[:100]}")
        return False
    except Exception as e:
        log.error(f"Telegram error: {e}"); return False

# ────────────────────────────────────────────────────────────────
# YAHOO FINANCE — Snapshots
# ────────────────────────────────────────────────────────────────
def get_snapshot(ticker):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=5d&interval=1d&includePrePost=false"
        r = requests.get(url, headers=YAHOO_HEADERS, timeout=10)
        j = r.json()
        result = j.get("chart",{}).get("result",[])
        if not result: return None
        quotes  = result[0].get("indicators",{}).get("quote",[{}])[0]
        volumes = [v for v in quotes.get("volume",[]) if v is not None]
        closes  = [c for c in quotes.get("close", []) if c is not None]
        if len(volumes) < 2 or len(closes) < 2: return None
        latest_vol = volumes[-1]
        avg_vol    = sum(volumes[:-1]) / len(volumes[:-1])
        vol_ratio  = latest_vol / avg_vol if avg_vol > 0 else 0
        price = closes[-1]; prev = closes[-2]
        chg   = ((price - prev) / prev * 100) if prev > 0 else 0
        if price < MIN_PRICE: return None
        return {
            "ticker":    ticker,
            "price":     round(price, 2),
            "price_chg": round(chg, 2),
            "volume":    int(latest_vol),
            "avg_vol":   int(avg_vol),
            "vol_ratio": round(vol_ratio, 2),
        }
    except Exception as e:
        log.warning(f"Yahoo error {ticker}: {e}"); return None

# ────────────────────────────────────────────────────────────────
# PRICE ALERTS
# ────────────────────────────────────────────────────────────────
def check_price_alerts(snapshots):
    watchlist = get_full_watchlist()
    for snap in snapshots:
        if snap["ticker"] not in watchlist: continue
        chg = snap["price_chg"]
        if abs(chg) >= PRICE_ALERT_PCT:
            d = "📈" if chg >= 0 else "📉"
            s = "+" if chg >= 0 else ""
            theme = KNOWN_THEMES.get(snap["ticker"], "—")
            dynamic = load_dynamic_watchlist()
            source = "Core watchlist"
            if snap["ticker"] in dynamic:
                source = f"Auto-added: {dynamic[snap['ticker']]['reason'][:50]}"
            send_telegram(
                f"{d} *WATCHLIST ALERT — ${snap['ticker']}*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"*Move:* {s}{chg}% today\n"
                f"*Price:* ${snap['price']}\n"
                f"*Volume:* {snap['vol_ratio']}x average\n"
                f"*Theme:* {theme}\n"
                f"*Source:* {source}\n\n"
                f"_Open SIS to analyse →_\n"
                f"_SIS · {datetime.now().strftime('%d %b %H:%M UTC')}_"
            )
            log.info(f"Price alert: {snap['ticker']} {s}{chg}%")
            time.sleep(0.5)

# ────────────────────────────────────────────────────────────────
# RSS SCANNER
# ────────────────────────────────────────────────────────────────
def fetch_rss(feed):
    try:
        r = requests.get(feed["url"], headers=YAHOO_HEADERS, timeout=15)
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:20]:
            title = item.findtext("title","").strip()
            desc  = item.findtext("description","").strip()
            link  = item.findtext("link","").strip()
            guid  = item.findtext("guid", link).strip()
            if title and guid not in seen_rss_ids:
                items.append({
                    "headline": title,
                    "summary":  desc[:200],
                    "source":   feed["name"],
                    "guid":     guid,
                })
        return items
    except Exception as e:
        log.warning(f"RSS error {feed['name']}: {e}"); return []

def scan_rss_feeds():
    global seen_rss_ids
    all_items = []
    for feed in RSS_FEEDS:
        items = fetch_rss(feed)
        log.info(f"RSS {feed['name']}: {len(items)} new items")
        all_items.extend(items)
        time.sleep(0.5)

    if not all_items:
        log.info("No new RSS items"); return

    headlines = "\n".join([f"{i+1}. [{x['source']}] {x['headline']}" for i,x in enumerate(all_items)])

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=2500,
            messages=[{"role":"user","content":f"{RSS_PROMPT}\n\nHeadlines:\n{headlines}"}]
        )
        text = resp.content[0].text.strip().replace("```json","").replace("```","").strip()
        signals = json.loads(text)

        for sig in signals:
            strength = sig.get("signal_strength","LOW")

            # Auto-add HIGH conviction tickers to watchlist
            if strength == "HIGH":
                for entry in sig.get("watchlist_add", []):
                    t = entry.get("ticker","").upper().strip()
                    reason = entry.get("reason","High conviction signal from news")
                    theme = sig.get("theme","—")
                    if t and len(t) <= 6 and t.isalpha():
                        add_to_dynamic_watchlist(t, reason, theme)

            # Send alert for HIGH and MEDIUM
            if strength == "LOW":
                continue

            e = "🔴" if strength == "HIGH" else "🟡"
            tickers = " · ".join([f"${t}" for t in sig.get("tickers",[])]) or "—"
            send_telegram(
                f"{e} *{strength} NEWS SIGNAL*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"*Source:* {sig.get('source','—')}\n"
                f"*Theme:* {sig.get('theme','—')}\n"
                f"*Headline:* {sig.get('headline','—')}\n\n"
                f"*Second Order:*\n↳ {sig.get('second_order','—')}\n\n"
                f"*Action:* {sig.get('action','—')}\n"
                f"*Tickers:* {tickers}\n\n"
                f"_SIS RSS · {datetime.now().strftime('%d %b %H:%M UTC')}_"
            )
            log.info(f"RSS alert: {sig.get('theme')} — {strength}")
            time.sleep(0.5)

        for item in all_items: seen_rss_ids.add(item["guid"])
        if len(seen_rss_ids) > 600: seen_rss_ids = set(list(seen_rss_ids)[-300:])

    except json.JSONDecodeError as e:
        log.error(f"RSS JSON error: {e}")
        for item in all_items: seen_rss_ids.add(item["guid"])
    except Exception as e:
        log.error(f"RSS error: {e}")
        for item in all_items: seen_rss_ids.add(item["guid"])

# ────────────────────────────────────────────────────────────────
# FINNHUB NEWS
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
        if not isinstance(items, list): return []
        return [{"headline":i.get("headline",""),"source":i.get("source","")} for i in items[:3]]
    except: return []

# ────────────────────────────────────────────────────────────────
# THEME CLASSIFICATION
# ────────────────────────────────────────────────────────────────
def classify_theme(ticker, news):
    if ticker in KNOWN_THEMES: return KNOWN_THEMES[ticker]
    dynamic = load_dynamic_watchlist()
    if ticker in dynamic: return dynamic[ticker].get("theme")
    if not news: return None
    try:
        news_text = " | ".join([n["headline"] for n in news[:2]])
        r = client.messages.create(model="claude-sonnet-4-20250514", max_tokens=60,
            messages=[{"role":"user","content":f"Does {ticker} fit any theme: {', '.join(THEMES)}?\nNews: {news_text}\nReply ONLY with exact theme name or NONE."}])
        result = r.content[0].text.strip()
        return result if result in THEMES else None
    except: return None

# ────────────────────────────────────────────────────────────────
# CLAUDE ANALYSIS
# ────────────────────────────────────────────────────────────────
def analyse(ticker, snap, news, theme):
    news_text = "\n".join([f"- {n['headline']} ({n['source']})" for n in news]) or "No recent news."
    try:
        resp = client.messages.create(
            model="claude-sonnet-4-20250514", max_tokens=1000,
            system=STRATEGY_PROMPT,
            messages=[{"role":"user","content":
                f"TICKER: {ticker}\nPRICE: ${snap['price']} ({snap['price_chg']:+.1f}%)\n"
                f"VOLUME: {snap['volume']:,} ({snap['vol_ratio']}x avg)\nTHEME: {theme}\n"
                f"NEWS:\n{news_text}\n\nReturn JSON verdict."}]
        )
        text = resp.content[0].text.strip().replace("```json","").replace("```","").strip()
        return json.loads(text)
    except Exception as e:
        log.error(f"Claude error {ticker}: {e}"); return None

def format_alert(ticker, snap, a):
    v = a.get("verdict","WATCHLIST")
    e = {"STRONG BUY":"🟢","BUY":"🟢","WATCHLIST":"🟡","PASS":"🔴"}.get(v,"⚪")
    s = "+" if snap["price_chg"] >= 0 else ""
    return (
        f"{e} *{v} — ${ticker}*\n━━━━━━━━━━━━━━━━━━\n"
        f"*Price:* ${snap['price']} ({s}{snap['price_chg']}%)\n"
        f"*Volume:* {snap['vol_ratio']}x average\n"
        f"*Theme:* {a.get('theme','—')}\n"
        f"*Conviction:* {a.get('conviction','—')}\n\n"
        f"↳ {a.get('second_order','—')}\n\n"
        f"*Three Questions:*\n"
        f"✓ {a.get('why_higher','—')}\n"
        f"✗ {a.get('prove_wrong','—')}\n"
        f"⏱ {a.get('timing','—')}\n\n"
        f"*Trade:* Entry ${a.get('entry_price','—')} · Stop ${a.get('stop_loss','—')} \\(~£{a.get('stop_loss_gbp','—')}\\)\n"
        f"*Targets:* T1 ${a.get('target_1','—')} · T2 ${a.get('target_2','—')}\n"
        f"*Size:* £{a.get('position_size_gbp','—')} · Acct {a.get('account','A')}\n\n"
        f"_{a.get('summary','—')}_\n"
        f"_SIS · {datetime.now().strftime('%d %b %H:%M UTC')}_"
    )

# ────────────────────────────────────────────────────────────────
# MAIN SCAN
# ────────────────────────────────────────────────────────────────
def run_scan():
    now = datetime.now()
    if now.weekday() >= 5: log.info("Weekend — skip"); return
    if now.hour < 7 or now.hour >= 23: log.info("Outside hours — skip"); return

    scan_list = get_full_scan_list()
    dynamic_count = len(load_dynamic_watchlist())

    log.info("="*55)
    log.info(f"SIS SCAN v5 — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info(f"Tickers: {len(scan_list)} ({len(BASE_SCAN_TICKERS)} base + {dynamic_count} auto-added)")
    log.info("="*55)

    # 1. RSS feeds
    log.info("--- RSS ---")
    scan_rss_feeds()

    # 2. Volume + price scan
    log.info("--- VOLUME SCAN ---")
    all_snaps = []
    vol_signals = []

    for i, ticker in enumerate(scan_list):
        snap = get_snapshot(ticker)
        if snap:
            all_snaps.append(snap)
            if snap["vol_ratio"] >= VOLUME_MULT:
                log.info(f"SIGNAL: {ticker} {snap['vol_ratio']}x · ${snap['price']} ({snap['price_chg']:+.1f}%)")
                vol_signals.append(snap)
        time.sleep(0.15)
        if (i+1) % 30 == 0:
            log.info(f"Progress: {i+1}/{len(scan_list)} · {len(vol_signals)} signals")

    log.info(f"Scan complete: {len(vol_signals)} signals from {len(all_snaps)} tickers")

    # 3. Price alerts
    log.info("--- PRICE ALERTS ---")
    check_price_alerts(all_snaps)

    # 4. AI analysis on top vol signals
    log.info("--- AI ANALYSIS ---")
    if not vol_signals:
        send_telegram(
            f"📡 *SIS Scan v5* — {now.strftime('%H:%M UTC')}\n"
            f"No vol signals above {VOLUME_MULT}x\n"
            f"{len(scan_list)} tickers scanned · RSS ✓\n"
            f"Auto-watchlist: {dynamic_count} tickers"
        )
        return

    vol_signals.sort(key=lambda x: x["vol_ratio"], reverse=True)
    alerts = 0

    for snap in vol_signals[:MAX_SIGNALS]:
        ticker = snap["ticker"]
        news   = get_news(ticker); time.sleep(0.2)
        theme  = classify_theme(ticker, news)
        if not theme: log.info(f"{ticker} — no theme, skip"); continue
        a = analyse(ticker, snap, news, theme)
        if not a: continue
        verdict = a.get("verdict","PASS")
        log.info(f"{ticker} — {verdict} ({a.get('conviction','—')})")
        if verdict in ("STRONG BUY","BUY","WATCHLIST"):
            if send_telegram(format_alert(ticker, snap, a)): alerts += 1
            time.sleep(1)

    send_telegram(
        f"📡 *SIS Complete* — {now.strftime('%H:%M UTC')}\n"
        f"Vol signals: {len(vol_signals)} · Alerts: {alerts}\n"
        f"Tickers: {len(scan_list)} · RSS ✓ · Auto-WL: {dynamic_count}"
    )
    log.info(f"Done — {alerts} alerts sent")

# ────────────────────────────────────────────────────────────────
# ENTRY POINT
# ────────────────────────────────────────────────────────────────
def main():
    log.info("SIS Scanner v5 starting...")
    log.info(f"Base tickers: {len(BASE_SCAN_TICKERS)}")
    log.info(f"Vol threshold: {VOLUME_MULT}x | Price alert: {PRICE_ALERT_PCT}%")
    log.info(f"RSS feeds: {', '.join([f['name'] for f in RSS_FEEDS])}")

    missing = [k for k,v in {
        "ANTHROPIC_API_KEY":  ANTHROPIC_KEY,
        "FINNHUB_API_KEY":    FINNHUB_KEY,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN,
        "TELEGRAM_CHAT_ID":   TELEGRAM_CHAT,
    }.items() if not v]

    if missing:
        log.error(f"Missing env vars: {missing}"); return

    log.info("All keys loaded ✓")

    dynamic = load_dynamic_watchlist()
    if dynamic:
        log.info(f"Auto-watchlist loaded: {len(dynamic)} tickers — {', '.join(dynamic.keys())}")

    log.info("Running initial scan...")
    run_scan()

    schedule.every(30).minutes.do(run_scan)
    log.info("Scheduler active — every 30 min, Mon-Fri 07:00-23:00 UTC")

    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
