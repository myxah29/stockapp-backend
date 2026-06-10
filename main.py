import os, json, asyncio, httpx, time
from datetime import datetime
from typing import Optional, List
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

# ── ENV VARS ──────────────────────────────────────────────────────────────────
GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "")
TWELVE_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")

# ── IN-MEMORY STORE ───────────────────────────────────────────────────────────
store = {}  # { username: { ticker: { alerts: [], history: [] } } }

def get_user(username: str):
    if username not in store:
        store[username] = {}
    return store[username]

def get_user_ticker(username: str, ticker: str):
    u = get_user(username)
    if ticker not in u:
        u[ticker] = {"alerts": [], "history": []}
    return u[ticker]

# ── ALERT CHECKER (runs every 5 mins in background) ───────────────────────────
async def check_alerts_loop():
    while True:
        await asyncio.sleep(300)
        if not TWELVE_API_KEY:
            continue
        for username, tickers in list(store.items()):
            for t, data in list(tickers.items()):
                untriggered = [a for a in data.get("alerts", []) if not a["triggered"]]
                if not untriggered:
                    continue
                try:
                    async with httpx.AsyncClient(timeout=8) as client:
                        r = await client.get(
                            "https://api.twelvedata.com/price",
                            params={"symbol": t, "apikey": TWELVE_API_KEY}
                        )
                        price = float(r.json().get("price", 0) or 0)
                    for alert in untriggered:
                        hit = (alert["type"] == "above" and price >= alert["price"]) or \
                              (alert["type"] == "below" and price <= alert["price"])
                        if hit:
                            alert["triggered"]       = True
                            alert["triggered_at"]    = datetime.utcnow().isoformat()
                            alert["triggered_price"] = price
                            print(f"[ALERT] {username} {t} {alert['type']} {alert['price']} @ {price}")
                except Exception:
                    pass

# ── APP LIFESPAN ──────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(check_alerts_loop())
    yield

app = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# ── LIVE PRICE ────────────────────────────────────────────────────────────────
@app.get("/api/price/{ticker}")
async def get_price(ticker: str, exchange: Optional[str] = None):
    if not TWELVE_API_KEY:
        raise HTTPException(500, "TWELVE_DATA_API_KEY not set in Render environment variables.")

    params = {"symbol": ticker, "apikey": TWELVE_API_KEY}
    if exchange:
        params["exchange"] = exchange

    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r = await client.get("https://api.twelvedata.com/quote", params=params)
            data = r.json()
        except Exception as e:
            raise HTTPException(504, f"Could not reach Twelve Data API: {str(e)}")

        if data.get("status") == "error":
            raise HTTPException(404, data.get("message", f"Ticker '{ticker}' not found."))

        price  = float(data.get("close", 0) or data.get("price", 0) or 0)
        prev   = float(data.get("previous_close", price) or price)
        fw     = data.get("fifty_two_week") or {}
        low52  = float(fw.get("low",  0) or 0) or None
        high52 = float(fw.get("high", 0) or 0) or None

        # Analyst targets from Yahoo Finance (best-effort, non-blocking)
        targets = {}
        try:
            yurl = (
                f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}"
                f"?modules=financialData,defaultKeyStatistics,price"
            )
            yr = await client.get(
                yurl, timeout=10,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
            )
            res = (yr.json().get("quoteSummary") or {}).get("result") or [{}]
            res = res[0] if res else {}
            fin = res.get("financialData") or {}
            kst = res.get("defaultKeyStatistics") or {}
            prc = res.get("price") or {}
            def raw(d, key):
                v = d.get(key)
                return v.get("raw") if isinstance(v, dict) else v
            targets = {
                "target_low":     raw(fin, "targetLowPrice"),
                "target_mean":    raw(fin, "targetMeanPrice"),
                "target_median":  raw(fin, "targetMedianPrice"),
                "target_high":    raw(fin, "targetHighPrice"),
                "recommendation": fin.get("recommendationKey", ""),
                "num_analysts":   raw(fin, "numberOfAnalystOpinions"),
                "sector":         prc.get("sector", ""),
                "industry":       prc.get("industry", ""),
                "market_cap":     raw(prc, "marketCap"),
                "fwd_pe":         raw(fin, "forwardPE") or raw(kst, "forwardPE"),
                "revenue_growth": raw(fin, "revenueGrowth"),
                "gross_margin":   raw(fin, "grossMargins"),
            }
        except Exception:
            pass

        # W1: sufficiency check
        key_fields = [price, low52, high52, targets.get("target_mean"),
                      targets.get("sector"), targets.get("fwd_pe"), targets.get("market_cap")]
        sufficiency_pct = round(sum(1 for v in key_fields if v) / len(key_fields) * 100)

        # W2: conflict detection
        conflicts = []
        if price and low52 and high52:
            if price < low52 * 0.9 or price > high52 * 1.1:
                conflicts.append(
                    f"Live price {price} falls outside 52W range {low52}–{high52}. Data may be stale."
                )

        upside = round(((targets["target_mean"] - price) / price) * 100, 1) \
                 if price and targets.get("target_mean") else None
        pct_of_range = round(((price - low52) / (high52 - low52)) * 100, 1) \
                       if price and low52 and high52 and high52 != low52 else None

        return {
            "ticker":          ticker.upper(),
            "exchange":        data.get("exchange", exchange or ""),
            "currency":        data.get("currency", "USD"),
            "price":           price,
            "prev_close":      prev,
            "change_pct":      round(((price - prev) / prev) * 100, 2) if prev else None,
            "low_52w":         low52,
            "high_52w":        high52,
            "pct_of_range":    pct_of_range,
            "volume":          data.get("volume"),
            "avg_volume":      data.get("average_volume"),
            "name":            data.get("name", ticker),
            "fetched_at":      datetime.utcnow().isoformat(),
            "sufficiency_pct": sufficiency_pct,
            "conflicts":       conflicts,
            **targets,
            "upside_to_mean":  upside,
        }

# ── WEB SEARCH ────────────────────────────────────────────────────────────────
CREDIBLE_SOURCES = [
    "reuters.com", "bloomberg.com", "ft.com", "wsj.com",
    "cnbc.com", "marketwatch.com", "seekingalpha.com",
    "sec.gov", "fool.com", "barrons.com", "finance.yahoo.com",
    "businessinsider.com", "investopedia.com",
]

async def web_search(query: str, num: int = 5) -> List[dict]:
    if not SERPER_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": num, "gl": "us", "hl": "en"},
            )
            results = r.json().get("organic", [])
            filtered = [
                x for x in results
                if any(d in x.get("link", "") for d in CREDIBLE_SOURCES)
            ]
            return (filtered or results)[:5]
    except Exception:
        return []

def format_search_results(results: List[dict]) -> str:
    if not results:
        return ""
    lines = ["## RECENT WEB SEARCH RESULTS (credible sources)"]
    for r in results:
        lines.append(f"- [{r.get('title','')}]({r.get('link','')}) — {r.get('snippet','')}")
    return "\n".join(lines)

# ── PROMPTS ───────────────────────────────────────────────────────────────────
PROMPTS = {
    "deep_dive": lambda t: (
        f"Write a Deep Dive research report on {t}:\n"
        f"1. **Business Model** — how they make money in plain English\n"
        f"2. **Moat & Competition** — top 3 competitors, key advantages\n"
        f"3. **Catalysts** — specific upcoming events next 12 months with dates where known\n"
        f"4. **Asymmetry Check** — is upside to analyst target bigger than the downside?\n\n"
        f"Cite every specific claim. Label training-knowledge figures as '(as of latest known reporting)'.\n"
        f"Never invent numbers not in the verified data above.\n\n"
        f"End with **Asymmetry Score: X/10**, ### Why this score (cite a specific data point), "
        f"## TL;DR (2-3 plain English sentences)."
    ),
    "relative_valuation": lambda t: (
        f"Sector-aware relative valuation for {t}:\n"
        f"1. Confirm sector and profitable/growth stage\n"
        f"2. Choose 3-4 most relevant metrics for this sector with justification\n"
        f"3. Markdown table: {t} vs 2 closest peers on those metrics\n"
        f"4. Interpretation: cheap, fair, or expensive vs peers?\n\n"
        f"Cite every figure. Label estimates as '(as of latest known reporting)'.\n"
        f"Never invent numbers not in the verified data above.\n\n"
        f"End with **Valuation Score: X/10**, ### Why this score (cite specific figures), "
        f"## TL;DR (2-3 plain English sentences)."
    ),
    "bear_case": lambda t: (
        f"Bear case risk assessment for {t}:\n"
        f"1. **Accounting or Financial Risks** — specific red flags with sources\n"
        f"2. **Revenue Concentration** — key client or product dependency\n"
        f"3. **Competitive Threats** — named competitors with credible threats\n\n"
        f"For each claim state source: verified data, web search, or training knowledge.\n"
        f"Label training knowledge as '(as of latest known reporting)'.\n\n"
        f"End with **Risk Score: X/10** (10 = most risky), ### Why this score (cite specific figures), "
        f"## TL;DR (2-3 plain English sentences)."
    ),
    "price_target": lambda t: (
        f"Price Target Report for {t}:\n"
        f"## Current Price\nState the verified live price including exchange and currency.\n\n"
        f"## Analyst Price Targets\n"
        f"Table: Source | Low | Median | Mean | High | Upside to Mean\n"
        f"Use verified figures. Label estimates '(as of latest known reporting)'.\n\n"
        f"## Expert Valuation Range\nBull vs bear in plain English.\n\n"
        f"## Best Entry Price\nSpecific price or narrow range with reasoning. One paragraph.\n\n"
        f"## Key Risk\nOne sentence: main scenario it falls well below entry.\n\n"
        f"End with **Upside Score: X/10**, ### Why this score (cite upside % to mean target), "
        f"## TL;DR (3 sentences: current price, where it could go, when to buy)."
    ),
}

# ── ANALYZE STREAM ────────────────────────────────────────────────────────────
class AnalysisRequest(BaseModel):
    ticker:   str
    card_id:  str
    username: str
    exchange: Optional[str] = None
    currency: Optional[str] = None

@app.post("/api/analyze/stream")
async def analyze_stream(
    req: AnalysisRequest,
    x_groq_key: Optional[str] = Header(None)
):
    groq_key = x_groq_key or GROQ_API_KEY
    if not groq_key:
        raise HTTPException(500, "No Groq API key provided. Enter it in the app banner.")

    ticker = req.ticker.upper()

    # 1 — Live price from Twelve Data
    price_data = {}
    try:
        params = {"symbol": ticker, "apikey": TWELVE_API_KEY}
        if req.exchange:
            params["exchange"] = req.exchange
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get("https://api.twelvedata.com/quote", params=params)
            price_data = r.json()
            if price_data.get("status") == "error":
                price_data = {}
    except Exception:
        pass

    # 2 — Web search for recent news
    search_queries = {
        "deep_dive":          f"{ticker} stock business model earnings news 2025",
        "relative_valuation": f"{ticker} stock valuation PE ratio sector peers 2025",
        "bear_case":          f"{ticker} stock risks concerns analyst downgrade 2025",
        "price_target":       f"{ticker} stock analyst price target rating 2025",
    }
    search_results = await web_search(search_queries.get(req.card_id, f"{ticker} stock 2025"))
    search_block   = format_search_results(search_results)

    # W1: block if truly no data at all
    price = float(price_data.get("close", 0) or price_data.get("price", 0) or 0)
    if not price and not search_results:
        raise HTTPException(422,
            f"No data found for {ticker}. Check the ticker symbol is correct and try again."
        )

    # 3 — Build verified data block
    currency = price_data.get("currency", req.currency or "USD")
    fw = price_data.get("fifty_two_week") or {}
    live_block = f"""## VERIFIED LIVE DATA FOR {ticker}
Fetched: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
Exchange: {price_data.get('exchange', req.exchange or 'N/A')} | Currency: {currency}
Current Price: {price if price else 'N/A'} {currency}
Previous Close: {price_data.get('previous_close', 'N/A')}
52-Week Low: {fw.get('low', 'N/A')} | 52-Week High: {fw.get('high', 'N/A')}
Day Change: {price_data.get('change', 'N/A')} ({price_data.get('percent_change', 'N/A')}%)
Volume: {price_data.get('volume', 'N/A')} | Avg Volume: {price_data.get('average_volume', 'N/A')}
Company Name: {price_data.get('name', ticker)}

RULES: These figures are ground truth. Do not modify or replace them.
Any figure NOT above must be labelled "(as of latest known reporting)" or "N/A — not in verified data"."""

    system_prompt = f"""You are a senior equity analyst. Today: {datetime.utcnow().strftime('%d %b %Y')}.

{live_block}

{search_block}

STRICT RULES:
1. Never invent or estimate any number not in the verified data block
2. If a figure is missing write "N/A — not in verified data"
3. Label ALL training-knowledge figures as "(as of latest known reporting)"
4. For every factual claim state its source: verified data / web search / training knowledge
5. Never write "recently" without specifying a date"""

    # 4 — Stream from Groq
    async def generate():
        sources_meta = [{"title": r.get("title", ""), "url": r.get("link", "")} for r in search_results]
        yield f"data: {json.dumps({'type':'meta','sources':sources_meta,'currency':currency,'exchange':price_data.get('exchange','')})}\n\n"

        try:
            async with httpx.AsyncClient(timeout=120) as client:
                async with client.stream(
                    "POST",
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                    json={
                        "model":       "llama-3.3-70b-versatile",
                        "max_tokens":  1800,
                        "temperature": 0.4,
                        "stream":      True,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user",   "content": PROMPTS[req.card_id](ticker)},
                        ],
                    },
                ) as resp:
                    if resp.status_code != 200:
                        body = await resp.aread()
                        try:
                            err = json.loads(body).get("error", {}).get("message", str(resp.status_code))
                        except Exception:
                            err = str(resp.status_code)
                        yield f"data: {json.dumps({'type':'error','message':str(err)})}\n\n"
                        return

                    full_text = ""
                    async for line in resp.aiter_lines():
                        if not line.startswith("data: "):
                            continue
                        raw = line[6:].strip()
                        if raw == "[DONE]":
                            break
                        try:
                            chunk = json.loads(raw)["choices"][0]["delta"].get("content", "")
                            if chunk:
                                full_text += chunk
                                yield f"data: {json.dumps({'type':'delta','content':chunk})}\n\n"
                        except Exception:
                            pass

                    # Save to server-side history
                    if req.username and full_text:
                        hist = get_user_ticker(req.username, ticker)
                        hist["history"].insert(0, {
                            "card_id":  req.card_id,
                            "text":     full_text[:600],
                            "run_at":   datetime.utcnow().isoformat(),
                            "price":    price,
                            "currency": currency,
                            "exchange": price_data.get("exchange", ""),
                        })
                        hist["history"] = hist["history"][:20]

                    yield f"data: {json.dumps({'type':'done'})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'type':'error','message':str(e)})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )

# ── ALERTS ────────────────────────────────────────────────────────────────────
class Alert(BaseModel):
    username:   str
    ticker:     str
    alert_type: str  # "above" or "below"
    price:      float
    note:       str = ""

@app.post("/api/alerts")
async def create_alert(alert: Alert):
    hist = get_user_ticker(alert.username, alert.ticker.upper())
    new_alert = {
        "id":         f"{alert.ticker}_{int(time.time())}",
        "type":       alert.alert_type,
        "price":      alert.price,
        "note":       alert.note,
        "created_at": datetime.utcnow().isoformat(),
        "triggered":  False,
    }
    hist["alerts"].append(new_alert)
    return {"status": "created", "alert": new_alert}

@app.get("/api/alerts/{username}/triggered")
async def get_triggered(username: str):
    triggered = []
    for t, data in get_user(username).items():
        for a in data.get("alerts", []):
            if a.get("triggered") and not a.get("acknowledged"):
                triggered.append({**a, "ticker": t})
    return {"triggered": triggered}

@app.post("/api/alerts/{username}/{alert_id}/acknowledge")
async def acknowledge_alert(username: str, alert_id: str):
    for t, data in get_user(username).items():
        for a in data.get("alerts", []):
            if a["id"] == alert_id:
                a["acknowledged"] = True
    return {"status": "acknowledged"}

@app.get("/api/alerts/{username}")
async def get_alerts(username: str):
    all_alerts = []
    for t, data in get_user(username).items():
        for a in data.get("alerts", []):
            all_alerts.append({**a, "ticker": t})
    return {"alerts": all_alerts}

@app.delete("/api/alerts/{username}/{alert_id}")
async def delete_alert(username: str, alert_id: str):
    for t, data in get_user(username).items():
        data["alerts"] = [a for a in data.get("alerts", []) if a["id"] != alert_id]
    return {"status": "deleted"}

# ── HISTORY ───────────────────────────────────────────────────────────────────
@app.get("/api/history/{username}/{ticker}")
async def get_history(username: str, ticker: str):
    return {"history": get_user_ticker(username, ticker.upper()).get("history", [])}
