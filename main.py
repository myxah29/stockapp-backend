import os, json, asyncio, httpx, time
from datetime import datetime, timedelta
from typing import Optional
from fastapi import FastAPI, HTTPException, BackgroundTasks, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse, JSONResponse
from pydantic import BaseModel

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── ENV VARS (set in Render dashboard) ──────────────────────────────────────
GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "")
TWELVE_API_KEY  = os.environ.get("TWELVE_DATA_API_KEY", "")
SERPER_API_KEY  = os.environ.get("SERPER_API_KEY", "")

# ── Simple in-memory store (no DB needed for MVP) ────────────────────────────
# Structure: { username: { ticker: { alerts: [], history: [] } } }
store = {}

def get_user(username: str):
    if username not in store:
        store[username] = {}
    return store[username]

def get_user_ticker(username: str, ticker: str):
    u = get_user(username)
    if ticker not in u:
        u[ticker] = {"alerts": [], "history": []}
    return u[ticker]

# ── HEALTH ───────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# ── LIVE PRICE (Twelve Data) ─────────────────────────────────────────────────
@app.get("/api/price/{ticker}")
async def get_price(ticker: str, exchange: Optional[str] = None):
    """Fetch real-time price from Twelve Data."""
    if not TWELVE_API_KEY:
        raise HTTPException(500, "TWELVE_DATA_API_KEY not configured on server. Check Render environment variables.")

    params = {"symbol": ticker, "apikey": TWELVE_API_KEY}
    if exchange:
        params["exchange"] = exchange

    async with httpx.AsyncClient(timeout=20) as client:
        # Real-time quote from Twelve Data
        try:
            r = await client.get("https://api.twelvedata.com/quote", params=params)
            data = r.json()
        except Exception as e:
            raise HTTPException(504, f"Could not reach Twelve Data API: {str(e)}")

        if data.get("status") == "error":
            raise HTTPException(404, data.get("message", f"Ticker '{ticker}' not found. Check the symbol is correct."))

        price = float(data.get("close", 0) or data.get("price", 0) or 0)
        prev  = float(data.get("previous_close", price) or price)
        fifty_two = data.get("fifty_two_week", {}) or {}
        low52  = float(fifty_two.get("low",  0) or 0) or None
        high52 = float(fifty_two.get("high", 0) or 0) or None

        # Try to get analyst targets from Yahoo Finance
        targets = {}
        try:
            yurl = f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}?modules=financialData,defaultKeyStatistics,price"
            yr = await client.get(yurl, timeout=10,
                headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
            yraw = yr.json()
            result = yraw.get("quoteSummary", {}).get("result", [None])[0] or {}
            fin  = result.get("financialData", {})
            kst  = result.get("defaultKeyStatistics", {})
            prc  = result.get("price", {})
            targets = {
                "target_low":    fin.get("targetLowPrice",   {}).get("raw"),
                "target_mean":   fin.get("targetMeanPrice",  {}).get("raw"),
                "target_median": fin.get("targetMedianPrice",{}).get("raw"),
                "target_high":   fin.get("targetHighPrice",  {}).get("raw"),
                "recommendation":fin.get("recommendationKey",""),
                "num_analysts":  fin.get("numberOfAnalystOpinions",{}).get("raw"),
                "sector":        prc.get("sector",""),
                "industry":      prc.get("industry",""),
                "market_cap":    prc.get("marketCap",{}).get("raw"),
                "fwd_pe":        (fin.get("forwardPE",{}) or kst.get("forwardPE",{})).get("raw"),
                "revenue_growth":fin.get("revenueGrowth",{}).get("raw"),
                "gross_margin":  fin.get("grossMargins",{}).get("raw"),
            }
        except Exception:
            pass  # targets are optional — analysis will still work without them

        # W1: data sufficiency
        key_fields = [price, low52, high52, targets.get("target_mean"),
                      targets.get("sector"), targets.get("fwd_pe"), targets.get("market_cap")]
        available       = sum(1 for v in key_fields if v)
        sufficiency_pct = round(available / len(key_fields) * 100)

        # W2: conflict detection
        conflicts = []
        if price and low52 and high52:
            if price < low52 * 0.9 or price > high52 * 1.1:
                conflicts.append(f"Live price {price} falls outside 52W range {low52}–{high52}. Data may be stale.")

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

# ── WEB SEARCH (Serper.dev) ──────────────────────────────────────────────────
async def web_search(query: str, num: int = 5) -> list[dict]:
    """Search Google via Serper.dev and return top results."""
    if not SERPER_API_KEY:
        return []
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
            json={"q": query, "num": num, "gl": "us", "hl": "en"},
        )
        results = r.json().get("organic", [])
        # Filter to credible sources only
        credible = [
            "reuters.com", "bloomberg.com", "ft.com", "wsj.com",
            "cnbc.com", "marketwatch.com", "seekingalpha.com",
            "sec.gov", "fool.com", "barrons.com", "businessinsider.com",
            "finance.yahoo.com", "investopedia.com"
        ]
        filtered = [
            r for r in results
            if any(domain in r.get("link", "") for domain in credible)
        ]
        # Fall back to all results if nothing credible found
        return filtered[:5] if filtered else results[:3]

def format_search_results(results: list[dict]) -> str:
    if not results:
        return ""
    lines = ["## RECENT WEB SEARCH RESULTS (credible sources only)"]
    for r in results:
        lines.append(f"- [{r.get('title','')}]({r.get('link','')}) — {r.get('snippet','')}")
    return "\n".join(lines)

# ── AI ANALYSIS STREAM ────────────────────────────────────────────────────────
class AnalysisRequest(BaseModel):
    ticker:    str
    card_id:   str
    username:  str
    exchange:  Optional[str] = None
    currency:  Optional[str] = "USD"

PROMPTS = {
    "deep_dive": lambda t: (
        f"Write a Deep Dive research report on {t}:\n"
        f"1. **Business Model** — how they make money in plain English\n"
        f"2. **Moat & Competition** — top 3 competitors, key advantages\n"
        f"3. **Catalysts** — specific upcoming events next 12 months with dates where known\n"
        f"4. **Asymmetry Check** — is the upside to analyst target bigger than the downside?\n\n"
        f"Rules: cite every specific claim. Label training-knowledge figures as '(as of latest known reporting)'. "
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
        f"Rules: cite every figure. Label estimates as '(as of latest known reporting)'. "
        f"Never invent numbers not in the verified data above.\n\n"
        f"End with **Valuation Score: X/10**, ### Why this score (cite specific figures), "
        f"## TL;DR (2-3 plain English sentences)."
    ),
    "bear_case": lambda t: (
        f"Bear case risk assessment for {t}:\n"
        f"1. **Accounting or Financial Risks** — specific red flags with sources\n"
        f"2. **Revenue Concentration** — key client or product dependency\n"
        f"3. **Competitive Threats** — named competitors with credible threats\n\n"
        f"Rules: for each claim state whether it comes from the verified data or training knowledge. "
        f"Label training knowledge as '(as of latest known reporting)'.\n\n"
        f"End with **Risk Score: X/10** (10 = most risky), ### Why this score (cite specific figures), "
        f"## TL;DR (2-3 plain English sentences)."
    ),
    "price_target": lambda t: (
        f"Price Target Report for {t}:\n"
        f"## Current Price\nState the verified live price from data above including exchange and currency.\n\n"
        f"## Analyst Price Targets\n"
        f"Table: Source | Low | Median | Mean | High | Upside to Mean\n"
        f"Use verified figures above. Label any estimates '(as of latest known reporting)'.\n\n"
        f"## Expert Valuation Range\nBull vs bear case in plain English.\n\n"
        f"## Best Entry Price\nSpecific price or narrow range with reasoning "
        f"(support levels, margin of safety). One paragraph.\n\n"
        f"## Key Risk\nOne sentence: main scenario where it falls well below entry price.\n\n"
        f"End with **Upside Score: X/10**, ### Why this score (cite upside % to mean target), "
        f"## TL;DR (3 sentences: current price, where it could go, when to buy)."
    ),
}

@app.post("/api/analyze/stream")
async def analyze_stream(req: AnalysisRequest, x_groq_key: Optional[str] = Header(None)):
    """Stream AI analysis with live price data + web search context."""
    # Use key from header (user's own key) or fall back to server env var
    groq_key = x_groq_key or GROQ_API_KEY
    if not groq_key:
        raise HTTPException(500, "No Groq API key provided")

    ticker = req.ticker.upper()

    # 1 — Fetch live price data
    price_data = {}
    try:
        params = {"symbol": ticker, "apikey": TWELVE_API_KEY}
        if req.exchange:
            params["exchange"] = req.exchange
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.twelvedata.com/quote", params=params)
            price_data = r.json()
    except Exception:
        pass

    # 2 — Web search for recent news (credible sources, last 90 days)
    search_queries = {
        "deep_dive":          f"{ticker} stock business model earnings 2025",
        "relative_valuation": f"{ticker} stock valuation PE ratio peers comparison 2025",
        "bear_case":          f"{ticker} stock risks concerns downturn 2025",
        "price_target":       f"{ticker} stock analyst price target upgrade downgrade 2025",
    }
    search_results = await web_search(search_queries.get(req.card_id, f"{ticker} stock 2025"))
    search_block   = format_search_results(search_results)

    # W1: check data sufficiency before proceeding
    price = float(price_data.get("close", 0) or price_data.get("price", 0) or 0)
    if not price and not search_results:
        raise HTTPException(422,
            f"Insufficient data for {ticker}: no live price and no web search results found. "
            "Check the ticker symbol or try again."
        )

    # 3 — Build verified data block
    currency = price_data.get("currency", req.currency or "USD")
    fifty_two = price_data.get("fifty_two_week", {})
    live_block = f"""## VERIFIED LIVE DATA FOR {ticker}
Fetched: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
Exchange: {price_data.get('exchange', req.exchange or 'N/A')} | Currency: {currency}
Current Price: {price} {currency}
Previous Close: {price_data.get('previous_close', 'N/A')} {currency}
52-Week Low: {fifty_two.get('low', 'N/A')} | 52-Week High: {fifty_two.get('high', 'N/A')}
Day Change: {price_data.get('change', 'N/A')} ({price_data.get('percent_change', 'N/A')}%)
Volume: {price_data.get('volume', 'N/A')} | Avg Volume: {price_data.get('average_volume', 'N/A')}
Name: {price_data.get('name', ticker)}

IMPORTANT: These figures are ground truth. Do not modify, estimate, or replace them.
Any figure NOT listed above must be labelled "(as of latest known reporting)" if from training data,
or "N/A — not in verified data" if unknown."""

    system_prompt = f"""You are a senior equity analyst. Today: {datetime.utcnow().strftime('%d %b %Y')}.

{live_block}

{search_block}

RULES — FOLLOW STRICTLY:
1. Never invent or estimate any number not in the verified data block above
2. If a figure is missing, write "N/A — not in verified data"  
3. For every factual claim, state whether it comes from (a) verified live data, (b) web search results above, or (c) training knowledge
4. Label ALL training-knowledge figures as "(as of latest known reporting)"
5. Never use the word "recently" without specifying a date
6. If web search results are older than 90 days, flag them as potentially stale"""

    user_prompt = PROMPTS[req.card_id](ticker)

    # 4 — Stream from Groq
    async def generate():
        async with httpx.AsyncClient(timeout=120) as client:
            async with client.stream(
                "POST",
                "https://api.groq.com/openai/v1/chat/completions",
                headers={
                    "Authorization": f"Bearer {groq_key}",
                    "Content-Type":  "application/json",
                },
                json={
                    "model":       "llama-3.3-70b-versatile",
                    "max_tokens":  1800,
                    "temperature": 0.4,
                    "stream":      True,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user",   "content": user_prompt},
                    ],
                },
            ) as resp:
                if resp.status_code != 200:
                    body = await resp.aread()
                    err  = json.loads(body).get("error", {}).get("message", resp.status_code)
                    yield f"data: {json.dumps({'type':'error','message':str(err)})}\n\n"
                    return

                # Emit metadata first (sources for attribution)
                sources = [{"title": r.get("title",""), "url": r.get("link","")} for r in search_results]
                yield f"data: {json.dumps({'type':'meta','sources':sources,'currency':currency,'exchange':price_data.get('exchange','')})}\n\n"

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

                # Save to history
                if req.username and full_text:
                    hist = get_user_ticker(req.username, ticker)
                    hist["history"].insert(0, {
                        "card_id":   req.card_id,
                        "text":      full_text[:600],
                        "run_at":    datetime.utcnow().isoformat(),
                        "price":     price,
                        "currency":  currency,
                        "exchange":  price_data.get("exchange", ""),
                    })
                    hist["history"] = hist["history"][:20]  # keep last 20

                yield f"data: {json.dumps({'type':'done'})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )

# ── ALERTS ───────────────────────────────────────────────────────────────────
class Alert(BaseModel):
    username: str
    ticker:   str
    type:     str   # "above" or "below"
    price:    float
    note:     str = ""

@app.post("/api/alerts")
async def create_alert(alert: Alert):
    hist = get_user_ticker(alert.username, alert.ticker.upper())
    new_alert = {
        "id":        f"{alert.ticker}_{int(time.time())}",
        "type":      alert.type,
        "price":     alert.price,
        "note":      alert.note,
        "created_at":datetime.utcnow().isoformat(),
        "triggered": False,
    }
    hist["alerts"].append(new_alert)
    return {"status": "created", "alert": new_alert}

@app.get("/api/alerts/{username}")
async def get_alerts(username: str):
    u = get_user(username)
    all_alerts = []
    for ticker, data in u.items():
        for a in data.get("alerts", []):
            all_alerts.append({**a, "ticker": ticker})
    return {"alerts": all_alerts}

@app.delete("/api/alerts/{username}/{alert_id}")
async def delete_alert(username: str, alert_id: str):
    u = get_user(username)
    for ticker, data in u.items():
        data["alerts"] = [a for a in data.get("alerts", []) if a["id"] != alert_id]
    return {"status": "deleted"}

# Background task: check alerts every 5 minutes
async def check_alerts_loop():
    while True:
        await asyncio.sleep(300)  # 5 minutes
        if not TWELVE_API_KEY:
            continue
        triggered = []
        for username, tickers in store.items():
            for ticker, data in tickers.items():
                untriggered = [a for a in data.get("alerts", []) if not a["triggered"]]
                if not untriggered:
                    continue
                try:
                    async with httpx.AsyncClient(timeout=8) as client:
                        r = await client.get(
                            "https://api.twelvedata.com/price",
                            params={"symbol": ticker, "apikey": TWELVE_API_KEY}
                        )
                        price = float(r.json().get("price", 0) or 0)
                    for alert in untriggered:
                        hit = (alert["type"] == "above" and price >= alert["price"]) or \
                              (alert["type"] == "below" and price <= alert["price"])
                        if hit:
                            alert["triggered"]    = True
                            alert["triggered_at"] = datetime.utcnow().isoformat()
                            alert["triggered_price"] = price
                            triggered.append({
                                "username": username,
                                "ticker":   ticker,
                                "alert":    alert,
                                "price":    price,
                            })
                except Exception:
                    pass
        if triggered:
            print(f"[ALERTS] {len(triggered)} alerts triggered: {triggered}")

@app.on_event("startup")
async def startup():
    asyncio.create_task(check_alerts_loop())

# ── HISTORY ──────────────────────────────────────────────────────────────────
@app.get("/api/history/{username}/{ticker}")
async def get_history(username: str, ticker: str):
    hist = get_user_ticker(username, ticker.upper())
    return {"history": hist.get("history", [])}

# ── TRIGGERED ALERTS POLL ────────────────────────────────────────────────────
@app.get("/api/alerts/{username}/triggered")
async def get_triggered(username: str):
    u = get_user(username)
    triggered = []
    for ticker, data in u.items():
        for a in data.get("alerts", []):
            if a.get("triggered") and not a.get("acknowledged"):
                triggered.append({**a, "ticker": ticker})
    return {"triggered": triggered}

@app.post("/api/alerts/{username}/{alert_id}/acknowledge")
async def acknowledge_alert(username: str, alert_id: str):
    u = get_user(username)
    for ticker, data in u.items():
        for a in data.get("alerts", []):
            if a["id"] == alert_id:
                a["acknowledged"] = True
    return {"status": "acknowledged"}
