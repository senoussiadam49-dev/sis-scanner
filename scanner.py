"""
SIS — Signal Intelligence System
Background Market Scanner v6
- Supabase integration: writes signals + watchlist to shared DB
- Dashboard reads same DB in real time
- Yahoo Finance volume scanning (~236 tickers)
- Watchlist price alerts (2%+)
- The Economist + Yahoo Finance RSS
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
SUPABASE_URL    = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY    = os.environ.get("SUPABASE_KEY", "")
VOLUME_MULT     = float(os.environ.get("VOLUME_MULTIPLIER", "1.5"))
MIN_PRICE       = float(os.environ.get("MIN_PRICE", "5.0"))
MAX_SIGNALS     = int(os.environ.get("MAX_SIGNALS_PER_SCAN", "5"))
PRICE_ALERT_PCT = float(os.environ.get("PRICE_ALERT_PCT", "2.0"))

THEMES = [
    "AI & Infrastructure", "AI Picks & Shovels", "Space & Satellite Economy",
    "Nuclear & Next-Gen Energy", "Biotech & Longevity",
    "Humanoid Robotics & Physical AI", "Robotics & Automation",
    "Cyber & Defence Tech", "Oil, Gas & Energy Geopolitics",
    "Geopolitical Momentum & Critical Minerals",
    "Water & Food Security",
    "Financial Infrastructure & De-dollarisation",
    "Longevity & Demographic Shift",
]

KNOWN_THEMES = {
    "NVDA":"AI & Infrastructure","PLTR":"AI & Infrastructure","MSFT":"AI & Infrastructure",
    "AMZN":"AI & Infrastructure","GOOGL":"AI & Infrastructure","META":"AI & Infrastructure",
    "GOOG":"AI & Infrastructure","ORCL":"AI & Infrastructure","IBM":"AI & Infrastructure",
    "CRM":"AI & Infrastructure","NOW":"AI & Infrastructure","SNOW":"AI & Infrastructure",
    "DDOG":"AI & Infrastructure","MDB":"AI & Infrastructure","NET":"AI & Infrastructure",
    "AI":"AI & Infrastructure","BBAI":"AI & Infrastructure","SOUN":"AI & Infrastructure",
    "VRT":"AI Picks & Shovels","ARM":"AI Picks & Shovels","ANET":"AI Picks & Shovels",
    "SMCI":"AI Picks & Shovels","AVGO":"AI Picks & Shovels","AMD":"AI Picks & Shovels",
    "INTC":"AI Picks & Shovels","LRCX":"AI Picks & Shovels","AMAT":"AI Picks & Shovels",
    "KLAC":"AI Picks & Shovels","MU":"AI Picks & Shovels","MRVL":"AI Picks & Shovels",
    "QCOM":"AI Picks & Shovels","MPWR":"AI Picks & Shovels","ONTO":"AI Picks & Shovels",
    "RKLB":"Space & Satellite Economy","ASTS":"Space & Satellite Economy",
    "SPCE":"Space & Satellite Economy","LUNR":"Space & Satellite Economy",
    "KTOS":"Space & Satellite Economy","IRDM":"Space & Satellite Economy",
    "VSAT":"Space & Satellite Economy","GSAT":"Space & Satellite Economy",
    "CCJ":"Nuclear & Next-Gen Energy","OKLO":"Nuclear & Next-Gen Energy",
    "CEG":"Nuclear & Next-Gen Energy","SMR":"Nuclear & Next-Gen Energy",
    "NNE":"Nuclear & Next-Gen Energy","VST":"Nuclear & Next-Gen Energy",
    "TALEN":"Nuclear & Next-Gen Energy","UUUU":"Nuclear & Next-Gen Energy",
    "NXE":"Nuclear & Next-Gen Energy","DNN":"Nuclear & Next-Gen Energy",
    "UEC":"Nuclear & Next-Gen Energy","BWXT":"Nuclear & Next-Gen Energy",
    "EQT":"Oil, Gas & Energy Geopolitics","LNG":"Oil, Gas & Energy Geopolitics",
    "CQP":"Oil, Gas & Energy Geopolitics","AR":"Oil, Gas & Energy Geopolitics",
    "RRC":"Oil, Gas & Energy Geopolitics","XOM":"Oil, Gas & Energy Geopolitics",
    "CVX":"Oil, Gas & Energy Geopolitics","COP":"Oil, Gas & Energy Geopolitics",
    "OXY":"Oil, Gas & Energy Geopolitics","HAL":"Oil, Gas & Energy Geopolitics",
    "SLB":"Oil, Gas & Energy Geopolitics","DVN":"Oil, Gas & Energy Geopolitics",
    "RBRK":"Cyber & Defence Tech","CRWD":"Cyber & Defence Tech",
    "LMT":"Cyber & Defence Tech","RTX":"Cyber & Defence Tech",
    "NOC":"Cyber & Defence Tech","GD":"Cyber & Defence Tech",
    "AXON":"Cyber & Defence Tech","PANW":"Cyber & Defence Tech",
    "ZS":"Cyber & Defence Tech","FTNT":"Cyber & Defence Tech",
    "OKTA":"Cyber & Defence Tech","S":"Cyber & Defence Tech",
    "BAH":"Cyber & Defence Tech","SAIC":"Cyber & Defence Tech",
    "GLD":"Geopolitical Momentum & Critical Minerals",
    "GDX":"Geopolitical Momentum & Critical Minerals",
    "GDXJ":"Geopolitical Momentum & Critical Minerals",
    "SLV":"Geopolitical Momentum & Critical Minerals",
    "MP":"Geopolitical Momentum & Critical Minerals",
    "FCX":"Geopolitical Momentum & Critical Minerals",
    "ALB":"Geopolitical Momentum & Critical Minerals",
    "LAC":"Geopolitical Momentum & Critical Minerals",
    "NEM":"Geopolitical Momentum & Critical Minerals",
    "GOLD":"Geopolitical Momentum & Critical Minerals",
    "TSLA":"Robotics & Automation","ISRG":"Robotics & Automation",
    "ROK":"Robotics & Automation","PATH":"Robotics & Automation",
    "ACHR":"Robotics & Automation","JOBY":"Robotics & Automation",
    "MRNA":"Biotech & Longevity","ILMN":"Biotech & Longevity",
    "RXRX":"Biotech & Longevity","BEAM":"Biotech & Longevity",
    "CRSP":"Biotech & Longevity",
}

CORE_WATCHLIST = [
    "NVDA","PLTR","RKLB","EQT","RBRK","VRT","CCJ","OKLO","LMT","GLD","CRWD","RTX","CEG",
]

SCAN_TICKERS = list(set([
    "NVDA","MSFT","GOOGL","GOOG","AMZN","META","ORCL","IBM","CRM","NOW","SNOW","DDOG","MDB",
    "NET","AI","BBAI","SOUN","PLTR","ADBE","INTU","WDAY","GTLB","CFLT",
    "AMD","INTC","AVGO","QCOM","MU","MRVL","ARM","ANET","SMCI","VRT",
    "LRCX","AMAT","KLAC","MPWR","ONTO","ACMR","NXPI","SWKS","AMBA",
    "RKLB","ASTS","SPCE","LUNR","KTOS","IRDM","VSAT","GSAT","MNTS","PL",
    "CCJ","OKLO","CEG","SMR","NNE","VST","TALEN","UUUU","NXE","DNN","UEC","BWXT","GEV",
    "TSLA","ISRG","ROK","PATH","ACHR","JOBY","IRBT","NNDM","BLDE",
    "LMT","RTX","NOC","GD","BA","AXON","CRWD","PANW","ZS","FTNT",
    "OKTA","S","RBRK","TENB","CYBR","LDOS","SAIC","BAH","CACI","DRS",
    "EQT","LNG","CQP","AR","RRC","SWN","XOM","CVX","COP","OXY",
    "HAL","SLB","DVN","EOG","MPC","VLO","KMI","WMB","ET",
    "GLD","GDX","GDXJ","SLV","MP","FCX","ALB","LAC","NEM","GOLD","WPM","AEM",
    "AIQ","BOTZ","ROBO","UFO","ARKX","URA","URNM","XOP","XLE","HACK","ITA","PPA",
    "ARKK","ARKG","ARKQ","LIT","REMX","COPX",
    "NFLX","COIN","MSTR","MRNA","ILMN","RXRX","BEAM","CRSP",
    "SPY","QQQ","IWM",
]))

RSS_FEEDS = [
    # Core macro & finance
    {"name":"The Economist — Finance","url":"https://www.economist.com/finance-and-economics/rss.xml"},
    {"name":"The Economist — Business","url":"https://www.economist.com/business/rss.xml"},
    {"name":"The Economist — International","url":"https://www.economist.com/international/rss.xml"},
    {"name":"The Economist — Middle East","url":"https://www.economist.com/middle-east-and-africa/rss.xml"},
    {"name":"Yahoo Finance","url":"https://finance.yahoo.com/news/rssindex"},
    {"name":"Reuters Business","url":"https://feeds.reuters.com/reuters/businessNews"},
    {"name":"Reuters World","url":"https://feeds.reuters.com/reuters/worldNews"},
    {"name":"BBC Business","url":"https://feeds.bbci.co.uk/news/business/rss.xml"},
    {"name":"BBC World","url":"https://feeds.bbci.co.uk/news/world/rss.xml"},
    {"name":"CNBC Top News","url":"https://www.cnbc.com/id/100003114/device/rss/rss.html"},
    # Geopolitics & policy — direct market movers
    {"name":"Politico Energy","url":"https://rss.politico.com/energy.xml"},
    {"name":"Politico Defence","url":"https://rss.politico.com/defense.xml"},
    {"name":"Foreign Policy","url":"https://foreignpolicy.com/feed/"},
    {"name":"War on the Rocks","url":"https://warontherocks.com/feed/"},
    # Theme-specific
    {"name":"Defense News","url":"https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml"},
    {"name":"Space News","url":"https://spacenews.com/feed/"},
    {"name":"Mining.com","url":"https://www.mining.com/feed/"},
    {"name":"World Nuclear News","url":"https://www.world-nuclear-news.org/rss"},
]

seen_rss_ids = set()  # populated from Supabase on startup

def load_seen_ids():
    """Load recently seen RSS IDs from Supabase to survive restarts."""
    global seen_rss_ids
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/seen_rss_ids",
            headers={**sb_headers(), "Prefer": "return=representation"},
            params={"select": "guid", "order": "created_at.desc", "limit": "500"},
            timeout=10
        )
        if r.ok:
            data = r.json()
            seen_rss_ids = {row["guid"] for row in data}
            log.info(f"Loaded {len(seen_rss_ids)} seen IDs from Supabase")
    except Exception as e:
        log.warning(f"Could not load seen IDs: {e}")

def save_seen_id(guid):
    """Persist a seen RSS ID to Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/seen_rss_ids",
            headers=sb_headers(),
            json={"guid": guid},
            timeout=5
        )
    except Exception as e:
        log.warning(f"Could not save seen ID: {e}")
client = Anthropic(api_key=ANTHROPIC_KEY)

STRATEGY_PROMPT = """You are the AI analysis engine of SIS (Signal Intelligence System).
PORTFOLIO: Account A ~£8,000 (own) | Account B ~£5,000 (brother's) | Total ~£13,000
UK retail investor. Position sizing: Speculative £150-300 (A only), Medium £300-500, High £500-900. Max 12% per trade. Min 20% cash.
13 THEMES: AI & Infrastructure | AI Picks & Shovels | Space & Satellite Economy | Nuclear & Next-Gen Energy | Biotech & Longevity | Humanoid Robotics & Physical AI | Robotics & Automation | Cyber & Defence Tech | Oil Gas & Energy Geopolitics | Geopolitical Momentum & Critical Minerals | Water & Food Security | Financial Infrastructure & De-dollarisation | Longevity & Demographic Shift
STRATEGY: Read SECOND ORDER. Set stop before entry. Thesis over price. Three questions: Why higher? Prove wrong? Early/On-time/Late?
MACRO (March 2026): Hormuz crisis — Qatar LNG offline, Europe buying US LNG. Agentic AI (OpenClaw) — 1000x more compute. Petrodollar stress — gold beneficiary. Holdings: PLTR/RKLB/NVDA (HOLD), RBRK (weak), EQT (active).
Return ONLY JSON: {"verdict":"STRONG BUY"|"BUY"|"WATCHLIST"|"PASS","conviction":"High"|"Medium"|"Low","theme":"name","signal_type":"1 - Breaking News"|"2 - Supply Chain"|"3 - AI Prediction","second_order":"one sentence","why_higher":"one sentence","prove_wrong":"one sentence","timing":"Early"|"On Time"|"Late","entry_price":0.00,"stop_loss":0.00,"stop_loss_gbp":0,"position_size_gbp":0,"account":"A"|"B"|"A only","target_1":0.00,"target_2":0.00,"summary":"2-3 sentences"}"""

RSS_PROMPT = """You are a signal-detection engine for a momentum trader. Your job is to identify headlines with a GENUINE, SPECIFIC investment angle. Reject noise ruthlessly.

13 investment themes:
1. AI & Infrastructure — models, data centres, chips, cooling, power, networking
2. AI Picks & Shovels — companies supplying AI regardless of who wins the AI race
3. Space & Satellite Economy — launch, LEO satellites, Earth observation, hypersonics
4. Nuclear & Next-Gen Energy — SMRs, uranium supply chain, nuclear services, grid
5. Biotech & Longevity — AI drug discovery, gene editing, GLP-1 ecosystem
6. Humanoid Robotics & Physical AI — humanoid robots, physical AI, embodied intelligence
7. Robotics & Automation — industrial automation, drones, autonomous systems
8. Cyber & Defence Tech — cybersecurity, defence contracts, autonomous weapons
9. Oil, Gas & Energy Geopolitics — LNG, energy security, multipolar world energy
10. Geopolitical Momentum & Critical Minerals — wartime picks & shovels, reshoring, rare earths
11. Water & Food Security — water infrastructure, fertilizers, agricultural supply chains
12. Financial Infrastructure & De-dollarisation — gold, SWIFT alternatives, commodity-backed currencies, payment rails outside dollar system
13. Longevity & Demographic Shift — ageing population plays, elder care, GLP-1, pension infrastructure

Macro context (March 2026):
- Hormuz crisis: US-Israel strikes on Iran, strait effectively closed, Qatar LNG offline, Marines deploying, mine-clearing operations beginning
- Agentic AI (OpenClaw): 1000x compute demand, fastest-growing open source project in history
- Petrodollar stress: Iran demanding yuan settlement, CIPS processing $245T, gold as primary beneficiary
- European energy crisis: scramble for US LNG, TTF-Henry Hub spread wide

SIGNAL QUALITY RULES — apply these strictly:
1. REJECT general market commentary, opinion pieces, predictions without specific catalysts
2. REJECT "stocks fell today", "markets worry about X", "analyst says Y" — no specific investment angle
3. REJECT duplicate signals — if same story already flagged, skip it
4. ACCEPT geopolitical and military events that directly affect commodity flows, defence spending, or supply chains
5. ACCEPT earnings surprises, contract wins, supply disruptions, policy changes with named companies or sectors
6. ACCEPT second-order plays — who benefits from the obvious winner? That is often the better trade
7. Only flag HIGH if: (a) immediate price catalyst exists, (b) specific tickers benefit, (c) information cycle is early — before consensus
8. Flag MEDIUM if: developing story, clear theme fit, but timing unclear
9. Flag LOW only if genuinely notable but no immediate trade — otherwise skip entirely
10. ACCEPT major macro news with clear sector impact: tariffs, trade wars, sanctions, central bank decisions with named sector winners/losers
11. ACCEPT any news naming a specific company with a clear positive or negative catalyst — earnings beats, contract wins, regulatory approvals, leadership changes
12. Be LESS strict than you think you should be — it is better to flag a borderline story as MEDIUM than to miss a real signal. The trader will filter

Think: "Would a sharp momentum trader read this and immediately know what to buy?" If no — reject it.

For matching headlines return ONLY a JSON array:
[{"headline":"...","source":"...","theme":"exact theme name from list above","second_order":"one sentence — the non-obvious investment implication","signal_strength":"HIGH"|"MEDIUM"|"LOW","tickers":["TICK1","TICK2"],"watchlist_add":[{"ticker":"TICK","reason":"one sentence"}],"action":"one specific actionable sentence"}]

If nothing meets the quality bar return []"""

YAHOO_HEADERS = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

# ── Supabase helpers ──────────────────────────────────────────────
def sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

def sb_insert(table, data):
    """Insert a row into Supabase."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=sb_headers(),
            json=data,
            timeout=10
        )
        if r.status_code in (200, 201):
            log.info(f"Supabase {table}: inserted ✓")
            return True
        else:
            log.error(f"Supabase error {r.status_code}: {r.text[:200]}")
            return False
    except Exception as e:
        log.error(f"Supabase insert error: {e}")
        return False

def sb_upsert_watchlist(ticker, reason, theme, source="auto"):
    """Add ticker to Supabase watchlist (upsert — no duplicates)."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return False
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/watchlist",
            headers={**sb_headers(), "Prefer": "resolution=merge-duplicates,return=minimal"},
            json={"ticker": ticker, "reason": reason, "theme": theme, "source": source},
            timeout=10
        )
        return r.status_code in (200, 201)
    except Exception as e:
        log.error(f"Supabase watchlist error: {e}")
        return False

def sb_cleanup_old_signals():
    """Keep only last 200 signals to avoid DB bloat."""
    if not SUPABASE_URL or not SUPABASE_KEY:
        return
    try:
        requests.delete(
            f"{SUPABASE_URL}/rest/v1/signals?created_at=lt.{(datetime.now()-timedelta(days=7)).isoformat()}",
            headers=sb_headers(),
            timeout=10
        )
    except:
        pass

# ── Telegram ──────────────────────────────────────────────────────
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": TELEGRAM_CHAT, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        return r.status_code == 200
    except Exception as e:
        log.error(f"Telegram error: {e}"); return False

# ── Yahoo Finance ─────────────────────────────────────────────────
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
            "ticker": ticker, "price": round(price,2), "price_chg": round(chg,2),
            "volume": int(latest_vol), "avg_vol": int(avg_vol), "vol_ratio": round(vol_ratio,2),
        }
    except Exception as e:
        log.warning(f"Yahoo error {ticker}: {e}"); return None

# ── Price alerts ──────────────────────────────────────────────────
def check_price_alerts(snapshots):
    for snap in snapshots:
        if snap["ticker"] not in CORE_WATCHLIST: continue
        chg = snap["price_chg"]
        if abs(chg) >= PRICE_ALERT_PCT:
            d = "📈" if chg >= 0 else "📉"
            s = "+" if chg >= 0 else ""
            theme = KNOWN_THEMES.get(snap["ticker"],"—")
            msg = (f"{d} *WATCHLIST ALERT — ${snap['ticker']}*\n"
                   f"━━━━━━━━━━━━━━━━━━\n"
                   f"*Move:* {s}{chg}% today\n"
                   f"*Price:* ${snap['price']}\n"
                   f"*Volume:* {snap['vol_ratio']}x average\n"
                   f"*Theme:* {theme}\n\n"
                   f"_Open SIS terminal to analyse →_\n"
                   f"_SIS · {datetime.now().strftime('%d %b %H:%M UTC')}_")
            send_telegram(msg)
            # Write to Supabase signals table
            sb_insert("signals", {
                "ticker": snap["ticker"],
                "headline": f"${snap['ticker']} moved {s}{chg}% today",
                "source": "Price Alert",
                "theme": theme,
                "second_order": f"Price movement of {s}{chg}% on {snap['vol_ratio']}x volume",
                "signal_strength": "HIGH" if abs(chg) >= 5 else "MEDIUM",
                "action": "Check thesis — is this news-driven or technical?",
                "tickers": [snap["ticker"]],
                "verdict": "WATCHLIST",
                "conviction": "Medium",
                "is_watchlist": True
            })
            log.info(f"Price alert: {snap['ticker']} {s}{chg}%")
            time.sleep(0.5)

# ── RSS scanner ───────────────────────────────────────────────────
def fetch_rss(feed):
    try:
        r = requests.get(feed["url"], headers=YAHOO_HEADERS, timeout=15)
        root = ET.fromstring(r.content)
        items = []
        for item in root.findall(".//item")[:20]:
            title = item.findtext("title","").strip()
            link  = item.findtext("link","").strip()
            guid  = item.findtext("guid", link).strip()
            if title and guid not in seen_rss_ids:
                items.append({"headline":title,"source":feed["name"],"guid":guid})
                seen_rss_ids.add(guid)
                save_seen_id(guid)
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
            model="claude-sonnet-4-20250514", max_tokens=2500,
            messages=[{"role":"user","content":f"{RSS_PROMPT}\n\nHeadlines:\n{headlines}"}]
        )
        text = resp.content[0].text.strip().replace("```json","").replace("```","").strip()
        signals = json.loads(text)

        for sig in signals:
            strength = sig.get("signal_strength","LOW")
            tickers  = sig.get("tickers",[])
            theme    = sig.get("theme","—")

            # Write ALL signals (HIGH + MEDIUM) to Supabase for dashboard
            if strength in ("HIGH","MEDIUM"):
                sb_insert("signals", {
                    "ticker": tickers[0] if tickers else None,
                    "headline": sig.get("headline",""),
                    "source": sig.get("source",""),
                    "theme": theme,
                    "second_order": sig.get("second_order",""),
                    "signal_strength": strength,
                    "action": sig.get("action",""),
                    "tickers": tickers,
                    "verdict": "HIGH" if strength == "HIGH" else "WATCHLIST",
                    "conviction": "High" if strength == "HIGH" else "Medium",
                    "is_watchlist": strength == "HIGH"
                })

            # Auto-add HIGH conviction tickers to Supabase watchlist
            if strength == "HIGH":
                for entry in sig.get("watchlist_add",[]):
                    t = entry.get("ticker","").upper().strip()
                    reason = entry.get("reason","High conviction signal")
                    if t and len(t) <= 6 and t.isalpha():
                        if sb_upsert_watchlist(t, reason, theme, "auto-rss"):
                            log.info(f"Auto-watchlist: {t} added")
                            send_telegram(
                                f"🔍 *AUTO-ADDED TO WATCHLIST*\n"
                                f"━━━━━━━━━━━━━━━━━━\n"
                                f"*Ticker:* ${t}\n"
                                f"*Theme:* {theme}\n"
                                f"*Reason:* {reason}\n"
                                f"_SIS Auto-Watchlist · {datetime.now().strftime('%d %b %H:%M UTC')}_"
                            )
                            time.sleep(0.5)

            # Send Telegram for HIGH/MEDIUM
            if strength == "LOW":
                continue
            e = "🔴" if strength == "HIGH" else "🟡"
            tk_str = " · ".join([f"${t}" for t in tickers]) or "—"
            send_telegram(
                f"{e} *{strength} NEWS SIGNAL*\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"*Source:* {sig.get('source','—')}\n"
                f"*Theme:* {theme}\n"
                f"*Headline:* {sig.get('headline','—')}\n\n"
                f"*Second Order:*\n↳ {sig.get('second_order','—')}\n\n"
                f"*Action:* {sig.get('action','—')}\n"
                f"*Tickers:* {tk_str}\n\n"
                f"_SIS RSS · {datetime.now().strftime('%d %b %H:%M UTC')}_"
            )
            log.info(f"RSS alert: {theme} — {strength}")
            time.sleep(0.5)

        for item in all_items: seen_rss_ids.add(item["guid"])
        if len(seen_rss_ids) > 600: seen_rss_ids = set(list(seen_rss_ids)[-300:])

    except Exception as e:
        log.error(f"RSS error: {e}")
        for item in all_items: seen_rss_ids.add(item["guid"])

# ── Finnhub news ──────────────────────────────────────────────────
def get_news(ticker):
    try:
        to  = datetime.now().strftime("%Y-%m-%d")
        frm = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
        r = requests.get(
            f"https://finnhub.io/api/v1/company-news?symbol={ticker}&from={frm}&to={to}&token={FINNHUB_KEY}",
            timeout=10
        )
        items = r.json()
        if not isinstance(items,list): return []
        return [{"headline":i.get("headline",""),"source":i.get("source","")} for i in items[:3]]
    except: return []

# ── Theme classification ──────────────────────────────────────────
def classify_theme(ticker, news):
    if ticker in KNOWN_THEMES: return KNOWN_THEMES[ticker]
    if not news: return None
    try:
        news_text = " | ".join([n["headline"] for n in news[:2]])
        r = client.messages.create(model="claude-sonnet-4-20250514", max_tokens=60,
            messages=[{"role":"user","content":f"Does {ticker} fit any theme: {', '.join(THEMES)}?\nNews: {news_text}\nReply ONLY with exact theme name or NONE."}])
        result = r.content[0].text.strip()
        return result if result in THEMES else None
    except: return None

# ── Claude full analysis ──────────────────────────────────────────
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

def format_vol_alert(ticker, snap, a):
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

# ── Main scan ─────────────────────────────────────────────────────
def run_scan():
    now = datetime.now()
    if now.weekday() >= 5: log.info("Weekend — skip"); return
    if now.hour < 7 or now.hour >= 23: log.info("Outside hours — skip"); return

    log.info("="*55)
    log.info(f"SIS SCAN v6 — {now.strftime('%Y-%m-%d %H:%M UTC')}")
    log.info(f"Tickers: {len(SCAN_TICKERS)} | Supabase: {'✓' if SUPABASE_URL else '✗'}")
    log.info("="*55)

    # Clean old signals weekly
    sb_cleanup_old_signals()

    # 1. RSS
    log.info("--- RSS ---")
    scan_rss_feeds()

    # 2. Volume scan
    log.info("--- VOLUME SCAN ---")
    all_snaps = []; vol_signals = []
    for i, ticker in enumerate(SCAN_TICKERS):
        snap = get_snapshot(ticker)
        if snap:
            all_snaps.append(snap)
            if snap["vol_ratio"] >= VOLUME_MULT:
                log.info(f"SIGNAL: {ticker} {snap['vol_ratio']}x · ${snap['price']} ({snap['price_chg']:+.1f}%)")
                vol_signals.append(snap)
        time.sleep(0.15)
        if (i+1) % 30 == 0:
            log.info(f"Progress: {i+1}/{len(SCAN_TICKERS)} · {len(vol_signals)} signals")

    log.info(f"Scan: {len(vol_signals)} signals from {len(all_snaps)} tickers")

    # 3. Price alerts
    log.info("--- PRICE ALERTS ---")
    check_price_alerts(all_snaps)

    # 4. AI analysis on top vol signals
    log.info("--- AI ANALYSIS ---")
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

        # Write to Supabase
        sb_insert("signals", {
            "ticker": ticker,
            "headline": f"Volume spike {snap['vol_ratio']}x — ${snap['price']} ({snap['price_chg']:+.1f}%)",
            "source": "Volume Scanner",
            "theme": theme,
            "second_order": a.get("second_order",""),
            "signal_strength": "HIGH" if verdict in ("STRONG BUY","BUY") else "MEDIUM" if verdict == "WATCHLIST" else "LOW",
            "action": a.get("summary",""),
            "tickers": [ticker],
            "verdict": verdict,
            "conviction": a.get("conviction",""),
            "is_watchlist": verdict in ("STRONG BUY","BUY")
        })

        # Auto-add BUY signals to watchlist
        if verdict in ("STRONG BUY","BUY"):
            sb_upsert_watchlist(ticker, f"Volume spike + {verdict} signal", theme, "auto-volume")

        if verdict in ("STRONG BUY","BUY","WATCHLIST"):
            if send_telegram(format_vol_alert(ticker, snap, a)): alerts += 1
            time.sleep(1)

    summary = (
        f"📡 *SIS Scan v6* — {now.strftime('%H:%M UTC')}\n"
        f"Vol signals: {len(vol_signals)} · Alerts: {alerts}\n"
        f"Tickers: {len(SCAN_TICKERS)} · RSS ✓ · DB ✓"
    )
    send_telegram(summary)
    log.info(f"Scan done — {alerts} alerts")

# ── Entry point ───────────────────────────────────────────────────
def main():
    log.info("SIS Scanner v7 — Supabase connected")
    load_seen_ids()  # Persist seen RSS IDs across restarts
    log.info(f"Tickers: {len(SCAN_TICKERS)} | Vol: {VOLUME_MULT}x | Price alert: {PRICE_ALERT_PCT}%")
    missing = [k for k,v in {
        "ANTHROPIC_API_KEY": ANTHROPIC_KEY,"FINNHUB_API_KEY": FINNHUB_KEY,
        "TELEGRAM_BOT_TOKEN": TELEGRAM_TOKEN,"TELEGRAM_CHAT_ID": TELEGRAM_CHAT,
        "SUPABASE_URL": SUPABASE_URL,"SUPABASE_KEY": SUPABASE_KEY,
    }.items() if not v]
    if missing: log.error(f"Missing: {missing}"); return
    log.info("All keys ✓")
    run_scan()
    schedule.every(30).minutes.do(run_scan)
    log.info("Scheduler active — every 30 min, Mon-Fri 07:00-23:00 UTC")
    while True:
        schedule.run_pending()
        time.sleep(30)

if __name__ == "__main__":
    main()
