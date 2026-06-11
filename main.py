import os
import json
import asyncio
import time
from datetime import datetime
from typing import Optional, List, Dict, Any

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.requests import Request

# ── ENV VARS ──────────────────────────────────────────────────────────────────
GROQ_API_KEY   = os.environ.get("GROQ_API_KEY", "")
TWELVE_API_KEY = os.environ.get("TWELVE_DATA_API_KEY", "")
SERPER_API_KEY = os.environ.get("SERPER_API_KEY", "")

# ── IN-MEMORY STORE ───────────────────────────────────────────────────────────
store: Dict[str, Any] = {}

def get_user(username: str) -> dict:
    if username not in store:
        store[username] = {}
    return store[username]

def get_user_ticker(username: str, ticker: str) -> dict:
    u = get_user(username)
    if ticker not in u:
        u[ticker] = {"alerts": [], "history": []}
    return u[ticker]

# ── APP ───────────────────────────────────────────────────────────────────────
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── ALERT CHECKER ─────────────────────────────────────────────────────────────
async def check_alerts_loop() -> None:
    while True:
        await asyncio.sleep(300)
        if not TWELVE_API_KEY:
            continue
        for username, tickers in list(store.items()):
            for t, data in list(tickers.items()):
                untriggered = [a for a in data.get("alerts", []) if not a.get("triggered")]
                if not untriggered:
                    continue
                try:
                    async with httpx.AsyncClient(timeout=8) as client:
                        r = await client.get(
                            "https://api.twelvedata.com/price",
                            params={"symbol": t, "apikey": TWELVE_API_KEY},
                        )
                        price = float(r.json().get("price", 0) or 0)
                    for alert in untriggered:
                        atype = alert.get("alert_type", "")
                        aprice = float(alert.get("price", 0))
                        hit = (atype == "above" and price >= aprice) or \
                              (atype == "below" and price <= aprice)
                        if hit:
                            alert["triggered"]       = True
                            alert["triggered_at"]    = datetime.utcnow().isoformat()
                            alert["triggered_price"] = price
                except Exception:
                    pass

@app.on_event("startup")
async def startup_event() -> None:
    asyncio.create_task(check_alerts_loop())

# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# ── HELPER ────────────────────────────────────────────────────────────────────
def safe_raw(d: dict, key: str):
    v = d.get(key)
    if isinstance(v, dict):
        return v.get("raw")
    return v

# ── LIVE PRICE ────────────────────────────────────────────────────────────────
@app.get("/api/price/{ticker}")
async def get_price(ticker: str, exchange: str = None) -> dict:
    if not TWELVE_API_KEY:
        raise HTTPException(
            status_code=500,
            detail="TWELVE_DATA_API_KEY not set in Render environment variables.",
        )

    params: dict = {"symbol": ticker, "apikey": TWELVE_API_KEY}
    if exchange:
        params["exchange"] = exchange

    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r = await client.get("https://api.twelvedata.com/quote", params=params)
            data = r.json()
        except Exception as exc:
            raise HTTPException(status_code=504, detail=f"Twelve Data unreachable: {exc}")

        if data.get("status") == "error":
            raise HTTPException(
                status_code=404,
                detail=data.get("message", f"Ticker '{ticker}' not found."),
            )

        price  = float(data.get("close") or data.get("price") or 0)
        prev   = float(data.get("previous_close") or price)
        fw     = data.get("fifty_two_week") or {}
        low52  = float(fw.get("low") or 0) or None
        high52 = float(fw.get("high") or 0) or None

        # Analyst targets from Yahoo Finance — best effort
        targets: dict = {}
        try:
            yurl = (
                "https://query1.finance.yahoo.com/v10/finance/quoteSummary/"
                f"{ticker}?modules=financialData,defaultKeyStatistics,price"
            )
            yr = await client.get(
                yurl,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            ydata = yr.json()
            result_list = (ydata.get("quoteSummary") or {}).get("result") or []
            result = result_list[0] if result_list else {}
            fin = result.get("financialData") or {}
            kst = result.get("defaultKeyStatistics") or {}
            prc = result.get("price") or {}
            targets = {
                "target_low":     safe_raw(fin, "targetLowPrice"),
                "target_mean":    safe_raw(fin, "targetMeanPrice"),
                "target_median":  safe_raw(fin, "targetMedianPrice"),
                "target_high":    safe_raw(fin, "targetHighPrice"),
                "recommendation": fin.get("recommendationKey") or "",
                "num_analysts":   safe_raw(fin, "numberOfAnalystOpinions"),
                "sector":         prc.get("sector") or "",
                "industry":       prc.get("industry") or "",
                "market_cap":     safe_raw(prc, "marketCap"),
                "fwd_pe":         safe_raw(fin, "forwardPE") or safe_raw(kst, "forwardPE"),
                "revenue_growth": safe_raw(fin, "revenueGrowth"),
                "gross_margin":   safe_raw(fin, "grossMargins"),
            }
        except Exception:
            pass

        # W1: data sufficiency
        key_fields = [
            price, low52, high52,
            targets.get("target_mean"),
            targets.get("sector"),
            targets.get("fwd_pe"),
            targets.get("market_cap"),
        ]
        filled = sum(1 for v in key_fields if v)
        sufficiency_pct = round(filled / len(key_fields) * 100)

        # W2: conflict detection
        conflicts: List[str] = []
        if price and low52 and high52:
            if price < low52 * 0.9 or price > high52 * 1.1:
                conflicts.append(
                    f"Live price {price} outside 52W range {low52}–{high52}. Data may be stale."
                )

        target_mean = targets.get("target_mean")
        upside = (
            round(((target_mean - price) / price) * 100, 1)
            if price and target_mean else None
        )
        pct_of_range = (
            round(((price - low52) / (high52 - low52)) * 100, 1)
            if price and low52 and high52 and high52 != low52 else None
        )

        return {
            "ticker":          ticker.upper(),
            "exchange":        data.get("exchange") or exchange or "",
            "currency":        data.get("currency") or "USD",
            "price":           price,
            "prev_close":      prev,
            "change_pct":      round(((price - prev) / prev) * 100, 2) if prev else None,
            "low_52w":         low52,
            "high_52w":        high52,
            "pct_of_range":    pct_of_range,
            "volume":          data.get("volume"),
            "avg_volume":      data.get("average_volume"),
            "name":            data.get("name") or ticker,
            "fetched_at":      datetime.utcnow().isoformat(),
            "sufficiency_pct": sufficiency_pct,
            "conflicts":       conflicts,
            "upside_to_mean":  upside,
            **targets,
        }

# ── WEB SEARCH ────────────────────────────────────────────────────────────────
CREDIBLE = [
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
                headers={
                    "X-API-KEY": SERPER_API_KEY,
                    "Content-Type": "application/json",
                },
                json={"q": query, "num": num, "gl": "us", "hl": "en"},
            )
            results = r.json().get("organic") or []
            filtered = [x for x in results if any(d in x.get("link", "") for d in CREDIBLE)]
            return (filtered or results)[:5]
    except Exception:
        return []

def format_search_results(results: List[dict]) -> str:
    if not results:
        return ""
    lines = ["## RECENT WEB SEARCH RESULTS (credible sources only)"]
    for item in results:
        lines.append(f"- [{item.get('title','')}]({item.get('link','')}) — {item.get('snippet','')}")
    return "\n".join(lines)

# ── PROMPTS ───────────────────────────────────────────────────────────────────
PROMPTS: Dict[str, Any] = {
    "deep_dive": lambda t: (
        f"Deep Dive on {t}:\n"
        f"1. **Business Model** — how they make money\n"
        f"2. **Moat & Competition** — top 3 competitors, key advantages\n"
        f"3. **Catalysts** — specific events next 12 months\n"
        f"4. **Asymmetry Check** — upside vs downside vs analyst target\n\n"
        f"Cite every claim. Label training-knowledge as '(as of latest known reporting)'.\n"
        f"Never invent numbers not in the verified data.\n\n"
        f"End: **Asymmetry Score: X/10**, ### Why this score, ## TL;DR (2-3 sentences)."
    ),
    "relative_valuation": lambda t: (
        f"Sector-aware valuation for {t}:\n"
        f"1. Confirm sector and stage\n"
        f"2. Choose 3-4 relevant metrics with justification\n"
        f"3. Table: {t} vs 2 peers\n"
        f"4. Cheap, fair, or expensive vs peers?\n\n"
        f"Cite every figure. Label estimates '(as of latest known reporting)'.\n\n"
        f"End: **Valuation Score: X/10**, ### Why this score, ## TL;DR (2-3 sentences)."
    ),
    "bear_case": lambda t: (
        f"Bear case for {t}:\n"
        f"1. **Accounting Risks** — specific red flags\n"
        f"2. **Revenue Concentration** — client dependency\n"
        f"3. **Competitive Threats** — named threats\n\n"
        f"Label each claim: verified data / web search / training knowledge.\n\n"
        f"End: **Risk Score: X/10**, ### Why this score, ## TL;DR (2-3 sentences)."
    ),
    "price_target": lambda t: (
        f"Price Target Report for {t}:\n"
        f"## Current Price — from verified data, include exchange and currency\n"
        f"## Analyst Targets — table: Source | Low | Median | Mean | High | Upside\n"
        f"## Expert Valuation — bull vs bear in plain English\n"
        f"## Best Entry Price — specific price or range with reasoning\n"
        f"## Key Risk — one sentence\n\n"
        f"End: **Upside Score: X/10**, ### Why this score, ## TL;DR (3 sentences)."
    ),
}

# ── ANALYZE STREAM ────────────────────────────────────────────────────────────
@app.post("/api/analyze/stream")
async def analyze_stream(request: Request) -> StreamingResponse:
    # Parse body manually to avoid pydantic model issues
    body = await request.json()
    ticker   = str(body.get("ticker", "")).upper()
    card_id  = str(body.get("card_id", ""))
    username = str(body.get("username", "anonymous"))
    exchange = body.get("exchange") or None
    currency = body.get("currency") or "USD"

    # Groq key: from request header or server env
    groq_key = request.headers.get("x-groq-key") or GROQ_API_KEY
    if not groq_key:
        raise HTTPException(status_code=500, detail="No Groq API key provided.")
    if card_id not in PROMPTS:
        raise HTTPException(status_code=400, detail=f"Unknown card_id: {card_id}")

    # 1 — Fetch live price
    price_data: dict = {}
    try:
        params: dict = {"symbol": ticker, "apikey": TWELVE_API_KEY}
        if exchange:
            params["exchange"] = exchange
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get("https://api.twelvedata.com/quote", params=params)
            pd = r.json()
            if pd.get("status") != "error":
                price_data = pd
    except Exception:
        pass

    # 2 — Web search
    queries: Dict[str, str] = {
        "deep_dive":          f"{ticker} stock earnings business news 2025",
        "relative_valuation": f"{ticker} stock valuation PE peers comparison 2025",
        "bear_case":          f"{ticker} stock risks analyst downgrade concerns 2025",
        "price_target":       f"{ticker} stock analyst price target forecast 2025",
    }
    search_results = await web_search(queries.get(card_id, f"{ticker} stock 2025"))
    search_block   = format_search_results(search_results)

    # W1: block if no data at all
    price = float(price_data.get("close") or price_data.get("price") or 0)
    if not price and not search_results:
        raise HTTPException(
            status_code=422,
            detail=f"No data found for {ticker}. Check the ticker symbol.",
        )

    # 3 — Build live data block
    cur      = price_data.get("currency") or currency
    fw       = price_data.get("fifty_two_week") or {}
    live_block = (
        f"## VERIFIED LIVE DATA FOR {ticker}\n"
        f"Fetched: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"Exchange: {price_data.get('exchange') or exchange or 'N/A'} | Currency: {cur}\n"
        f"Current Price: {price or 'N/A'} {cur}\n"
        f"Previous Close: {price_data.get('previous_close') or 'N/A'}\n"
        f"52W Low: {fw.get('low') or 'N/A'} | 52W High: {fw.get('high') or 'N/A'}\n"
        f"Change: {price_data.get('change') or 'N/A'} ({price_data.get('percent_change') or 'N/A'}%)\n"
        f"Volume: {price_data.get('volume') or 'N/A'}\n"
        f"Company: {price_data.get('name') or ticker}\n\n"
        f"RULES: These are ground truth. Do not modify or replace them.\n"
        f"Missing figures must be labelled \"N/A — not in verified data\"."
    )

    system_prompt = (
        f"You are a senior equity analyst. Today: {datetime.utcnow().strftime('%d %b %Y')}.\n\n"
        f"{live_block}\n\n"
        f"{search_block}\n\n"
        f"STRICT RULES:\n"
        f"1. Never invent numbers not in the verified data\n"
        f"2. Missing figures: write \"N/A — not in verified data\"\n"
        f"3. Training-knowledge figures: label \"(as of latest known reporting)\"\n"
        f"4. State source for every claim: verified data / web search / training\n"
        f"5. Never write \"recently\" without a specific date"
    )

    user_prompt = PROMPTS[card_id](ticker)

    # 4 — Stream from Groq
    async def generate():
        sources_meta = [
            {"title": x.get("title", ""), "url": x.get("link", "")}
            for x in search_results
        ]
        yield f"data: {json.dumps({'type':'meta','sources':sources_meta,'currency':cur})}\n\n"

        try:
            async with httpx.AsyncClient(timeout=120) as client:
                async with client.stream(
                    "POST",
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {groq_key}",
                        "Content-Type": "application/json",
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
                        body_bytes = await resp.aread()
                        try:
                            err = json.loads(body_bytes).get("error", {}).get("message", resp.status_code)
                        except Exception:
                            err = resp.status_code
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

                    # Save history server-side
                    if username and full_text:
                        hist = get_user_ticker(username, ticker)
                        hist["history"].insert(0, {
                            "card_id":  card_id,
                            "text":     full_text[:600],
                            "run_at":   datetime.utcnow().isoformat(),
                            "price":    price,
                            "currency": cur,
                            "exchange": price_data.get("exchange", ""),
                        })
                        hist["history"] = hist["history"][:20]

                    yield f"data: {json.dumps({'type':'done'})}\n\n"

        except Exception as exc:
            yield f"data: {json.dumps({'type':'error','message':str(exc)})}\n\n"

    return StreamingResponse(
        generate(),
        media_type="text/event-stream",
        headers={"X-Accel-Buffering": "no", "Cache-Control": "no-cache"},
    )

# ── ALERTS ────────────────────────────────────────────────────────────────────
@app.post("/api/alerts")
async def create_alert(request: Request) -> dict:
    body = await request.json()
    username   = str(body.get("username", ""))
    ticker_raw = str(body.get("ticker", "")).upper()
    alert_type = str(body.get("alert_type", "below"))
    price      = float(body.get("price", 0))
    note       = str(body.get("note", ""))

    hist = get_user_ticker(username, ticker_raw)
    new_alert = {
        "id":         f"{ticker_raw}_{int(time.time())}",
        "alert_type": alert_type,
        "price":      price,
        "note":       note,
        "created_at": datetime.utcnow().isoformat(),
        "triggered":  False,
    }
    hist["alerts"].append(new_alert)
    return {"status": "created", "alert": new_alert}

@app.get("/api/alerts/{username}/triggered")
async def get_triggered(username: str) -> dict:
    triggered = []
    for t, data in get_user(username).items():
        for a in data.get("alerts", []):
            if a.get("triggered") and not a.get("acknowledged"):
                triggered.append({**a, "ticker": t})
    return {"triggered": triggered}

@app.post("/api/alerts/{username}/{alert_id}/acknowledge")
async def acknowledge_alert(username: str, alert_id: str) -> dict:
    for t, data in get_user(username).items():
        for a in data.get("alerts", []):
            if a.get("id") == alert_id:
                a["acknowledged"] = True
    return {"status": "acknowledged"}

@app.get("/api/alerts/{username}")
async def get_alerts(username: str) -> dict:
    all_alerts = []
    for t, data in get_user(username).items():
        for a in data.get("alerts", []):
            all_alerts.append({**a, "ticker": t})
    return {"alerts": all_alerts}

@app.delete("/api/alerts/{username}/{alert_id}")
async def delete_alert(username: str, alert_id: str) -> dict:
    for t, data in get_user(username).items():
        data["alerts"] = [a for a in data.get("alerts", []) if a.get("id") != alert_id]
    return {"status": "deleted"}

# ── HISTORY ───────────────────────────────────────────────────────────────────
@app.get("/api/history/{username}/{ticker}")
async def get_history(username: str, ticker: str) -> dict:
    return {"history": get_user_ticker(username, ticker.upper()).get("history", [])}
