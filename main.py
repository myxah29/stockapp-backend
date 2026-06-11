import os
import json
import re
from datetime import datetime

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.requests import Request

# ── ENV VARS ──────────────────────────────────────────────────────────────────
GROQ_API_KEY    = os.environ.get("GROQ_API_KEY", "")
TWELVE_API_KEY  = os.environ.get("TWELVE_DATA_API_KEY", "")
SERPER_API_KEY  = os.environ.get("SERPER_API_KEY", "")
FINNHUB_API_KEY = os.environ.get("FINNHUB_API_KEY", "")

# ── STORE ─────────────────────────────────────────────────────────────────────
store = {}

def get_user(username):
    if username not in store:
        store[username] = {}
    return store[username]

def get_user_ticker(username, ticker):
    u = get_user(username)
    if ticker not in u:
        u[ticker] = {"history": []}
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

# ── HEALTH ────────────────────────────────────────────────────────────────────
@app.get("/api/health")
async def health():
    return {"status": "ok", "time": datetime.utcnow().isoformat()}

# ── HELPER ────────────────────────────────────────────────────────────────────
def safe_float(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default

# ── FINNHUB FUNDAMENTALS + ANALYST TARGETS ───────────────────────────────────
async def fetch_finnhub(client, ticker):
    """Fetch analyst targets, recommendation, and company profile from Finnhub.
    Returns a dict of fields; missing fields are simply absent."""
    out = {}
    if not FINNHUB_API_KEY:
        return out
    tok = FINNHUB_API_KEY

    # 1) Price target (low/mean/median/high)
    try:
        r = await client.get(
            "https://finnhub.io/api/v1/stock/price-target",
            params={"symbol": ticker, "token": tok}, timeout=10,
        )
        pt = r.json() or {}
        out["target_low"]    = pt.get("targetLow") or None
        out["target_mean"]   = pt.get("targetMean") or None
        out["target_median"] = pt.get("targetMedian") or None
        out["target_high"]   = pt.get("targetHigh") or None
    except Exception:
        pass

    # 2) Analyst recommendation consensus (most recent period)
    try:
        r = await client.get(
            "https://finnhub.io/api/v1/stock/recommendation",
            params={"symbol": ticker, "token": tok}, timeout=10,
        )
        recs = r.json() or []
        if isinstance(recs, list) and recs:
            latest = recs[0]
            buy  = (latest.get("strongBuy", 0) or 0) + (latest.get("buy", 0) or 0)
            hold = latest.get("hold", 0) or 0
            sell = (latest.get("strongSell", 0) or 0) + (latest.get("sell", 0) or 0)
            total = buy + hold + sell
            out["num_analysts"] = total or None
            if total:
                if buy / total >= 0.6:
                    out["recommendation"] = "buy"
                elif sell / total >= 0.4:
                    out["recommendation"] = "sell"
                else:
                    out["recommendation"] = "hold"
    except Exception:
        pass

    # 3) Company profile (sector/industry, market cap)
    try:
        r = await client.get(
            "https://finnhub.io/api/v1/stock/profile2",
            params={"symbol": ticker, "token": tok}, timeout=10,
        )
        prof = r.json() or {}
        out["industry"]   = prof.get("finnhubIndustry") or ""
        out["sector"]     = prof.get("finnhubIndustry") or ""  # Finnhub gives industry, use as sector proxy
        mc = prof.get("marketCapitalization")  # in millions
        out["market_cap"] = (mc * 1_000_000) if mc else None
        if prof.get("name"):
            out["company_name"] = prof.get("name")
    except Exception:
        pass

    # 4) Basic financials (forward P/E, margins, growth)
    try:
        r = await client.get(
            "https://finnhub.io/api/v1/stock/metric",
            params={"symbol": ticker, "metric": "all", "token": tok}, timeout=10,
        )
        m = (r.json() or {}).get("metric", {}) or {}
        out["fwd_pe"]         = m.get("peTTM") or m.get("peBasicExclExtraTTM") or None
        out["gross_margin"]   = (m.get("grossMarginTTM") / 100) if m.get("grossMarginTTM") else None
        out["revenue_growth"] = (m.get("revenueGrowthTTMYoy") / 100) if m.get("revenueGrowthTTMYoy") else None
    except Exception:
        pass

    return out

# ── LIVE PRICE ────────────────────────────────────────────────────────────────
@app.get("/api/price/{ticker}")
async def get_price(ticker, exchange=None):
    if not TWELVE_API_KEY:
        raise HTTPException(500, "TWELVE_DATA_API_KEY not set in Render environment variables.")

    params = {"symbol": ticker, "apikey": TWELVE_API_KEY}
    if exchange:
        params["exchange"] = exchange

    async with httpx.AsyncClient(timeout=20) as client:
        try:
            r    = await client.get("https://api.twelvedata.com/quote", params=params)
            data = r.json()
        except Exception as exc:
            raise HTTPException(504, f"Twelve Data unreachable: {exc}")

        if data.get("status") == "error":
            raise HTTPException(404, data.get("message", f"Ticker '{ticker}' not found."))

        price  = safe_float(data.get("close") or data.get("price"))
        prev   = safe_float(data.get("previous_close")) or price
        fw     = data.get("fifty_two_week") or {}
        low52  = safe_float(fw.get("low")) or None
        high52 = safe_float(fw.get("high")) or None

        # Fundamentals + analyst targets from Finnhub
        targets = {
            "target_low": None, "target_mean": None, "target_median": None,
            "target_high": None, "recommendation": "", "num_analysts": None,
            "sector": "", "industry": "", "market_cap": None,
            "fwd_pe": None, "revenue_growth": None, "gross_margin": None,
        }
        try:
            fh = await fetch_finnhub(client, ticker.upper())
            for k, v in fh.items():
                if v:
                    targets[k] = v
        except Exception:
            pass

        # Use Finnhub company name if Twelve Data didn't give one
        company_name = data.get("name") or targets.pop("company_name", None) or ticker

        # Web-sourced target fallback (clearly NOT verified) when Finnhub gave none
        target_source = "finnhub" if targets.get("target_mean") else None
        if not targets.get("target_mean"):
            try:
                results = await web_search(f"{ticker} stock analyst price target {datetime.utcnow().year}")
                wt, wsrc = extract_web_target(results, current_price=price)
                if wt:
                    targets["target_mean"] = wt
                    target_source = wsrc  # e.g. "marketwatch" — signals web-sourced, not verified
            except Exception:
                pass

        key_fields      = [price, low52, high52, targets.get("target_mean"),
                           targets.get("sector"), targets.get("fwd_pe"), targets.get("market_cap")]
        sufficiency_pct = round(sum(1 for v in key_fields if v) / len(key_fields) * 100)

        conflicts = []
        if price and low52 and high52:
            if price < low52 * 0.9 or price > high52 * 1.1:
                conflicts.append(f"Price {price} outside 52W range {low52}–{high52}. Data may be stale.")

        target_mean  = targets.get("target_mean")
        upside       = round(((target_mean - price) / price) * 100, 1) if price and target_mean else None
        pct_of_range = round(((price - low52) / (high52 - low52)) * 100, 1) \
                       if price and low52 and high52 and high52 != low52 else None

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
            "name":            company_name,
            "fetched_at":      datetime.utcnow().isoformat(),
            "sufficiency_pct": sufficiency_pct,
            "conflicts":       conflicts,
            "upside_to_mean":  upside,
            "target_source":   target_source,
            **targets,
        }

# ── WEB SEARCH ────────────────────────────────────────────────────────────────
CREDIBLE = [
    "reuters.com", "bloomberg.com", "ft.com", "wsj.com", "cnbc.com",
    "marketwatch.com", "seekingalpha.com", "sec.gov", "fool.com",
    "barrons.com", "finance.yahoo.com", "businessinsider.com", "investopedia.com",
]

async def web_search(query, num=5):
    if not SERPER_API_KEY:
        return []
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r       = await client.post(
                "https://google.serper.dev/search",
                headers={"X-API-KEY": SERPER_API_KEY, "Content-Type": "application/json"},
                json={"q": query, "num": num, "gl": "us", "hl": "en"},
            )
            results  = r.json().get("organic") or []
            filtered = [x for x in results if any(d in x.get("link", "") for d in CREDIBLE)]
            return (filtered or results)[:5]
    except Exception:
        return []

def format_search_block(results):
    if not results:
        return ""
    lines = ["## RECENT WEB SEARCH RESULTS (credible sources only)"]
    for x in results:
        lines.append(f"- [{x.get('title','')}]({x.get('link','')}) — {x.get('snippet','')}")
    return "\n".join(lines)

def extract_web_target(results, current_price=None):
    """Scan web search snippets for an analyst price target figure.
    Returns (value, source_domain) or (None, None). Clearly NOT verified data."""
    if not results:
        return None, None
    # Patterns like "price target of $1,099", "target price: 142.50", "PT of $95"
    patterns = [
        r"price target[^\d]{0,15}\$?\s*([\d,]+(?:\.\d+)?)",
        r"target price[^\d]{0,15}\$?\s*([\d,]+(?:\.\d+)?)",
        r"\bPT[^\d]{0,8}\$?\s*([\d,]+(?:\.\d+)?)",
        r"average target[^\d]{0,15}\$?\s*([\d,]+(?:\.\d+)?)",
    ]
    for item in results:
        text = f"{item.get('title','')} {item.get('snippet','')}"
        for pat in patterns:
            m = re.search(pat, text, re.I)
            if m:
                val = safe_float(m.group(1).replace(",", ""))
                if not val:
                    continue
                # Sanity check: target should be within a plausible band of current price
                if current_price:
                    if val < current_price * 0.3 or val > current_price * 4:
                        continue  # implausible — likely picked up an unrelated number
                domain = ""
                link = item.get("link", "")
                for d in CREDIBLE:
                    if d in link:
                        domain = d.replace(".com", "").replace(".gov", "")
                        break
                return val, (domain or "web")
    return None, None

# ── PROMPTS ───────────────────────────────────────────────────────────────────
PROMPTS = {
    "deep_dive": lambda t: (
        f"Deep Dive on {t}:\n"
        f"1. **Business Model** — how they make money\n"
        f"2. **Moat & Competition** — top 3 competitors, key advantages\n"
        f"3. **Catalysts** — specific events next 12 months with dates\n"
        f"4. **Asymmetry Check** — upside vs downside to analyst target\n\n"
        f"Cite every claim. Label training-knowledge '(as of latest known reporting)'.\n"
        f"Never invent numbers not in the verified data.\n\n"
        f"End: **Asymmetry Score: X/10**, ### Why this score, ## TL;DR (2-3 sentences)."
    ),
    "relative_valuation": lambda t: (
        f"Sector-aware valuation for {t}:\n"
        f"1. Confirm sector and stage\n"
        f"2. Choose 3-4 relevant metrics with justification\n"
        f"3. Table: {t} vs 2 closest peers\n"
        f"4. Cheap, fair, or expensive vs peers?\n\n"
        f"Cite every figure. Label estimates '(as of latest known reporting)'.\n\n"
        f"End: **Valuation Score: X/10**, ### Why this score, ## TL;DR (2-3 sentences)."
    ),
    "bear_case": lambda t: (
        f"Bear case for {t}:\n"
        f"1. **Accounting Risks** — specific red flags\n"
        f"2. **Revenue Concentration** — client/product dependency\n"
        f"3. **Competitive Threats** — named, credible threats\n\n"
        f"Label each claim: verified data / web search / training knowledge.\n\n"
        f"End: **Risk Score: X/10**, ### Why this score, ## TL;DR (2-3 sentences)."
    ),
    "price_target": lambda t: (
        f"Price Target Report for {t}:\n"
        f"## Current Price — from verified data, with exchange and currency\n"
        f"## Analyst Targets — table: Source | Low | Median | Mean | High | Upside to Mean\n"
        f"## Expert Valuation — bull vs bear in plain English\n"
        f"## Best Entry Price\n"
        f"Give your reasoning in a sentence or two (support levels, margin of safety), "
        f"THEN on its very own final line write exactly this format with no other text:\n"
        f"`ENTRY: <number or range>` — for example `ENTRY: 800-850` or `ENTRY: 142.50`\n"
        f"Use the same currency as the current price. Do not put any other number on the ENTRY line.\n"
        f"## Key Risk — one sentence on main downside scenario\n\n"
        f"End: **Upside Score: X/10**, ### Why this score, ## TL;DR (3 sentences)."
    ),
}

_YEAR = datetime.utcnow().year

SEARCH_QUERIES = {
    "deep_dive":          lambda t: f"{t} stock earnings business model news {_YEAR}",
    "relative_valuation": lambda t: f"{t} stock valuation PE ratio sector peers {_YEAR}",
    "bear_case":          lambda t: f"{t} stock risks analyst downgrade concerns {_YEAR}",
    "price_target":       lambda t: f"{t} stock analyst price target forecast upgrade {_YEAR}",
}

# ── ANALYZE STREAM ────────────────────────────────────────────────────────────
@app.post("/api/analyze/stream")
async def analyze_stream(request: Request):
    body     = await request.json()
    ticker   = str(body.get("ticker", "")).upper()
    card_id  = str(body.get("card_id", ""))
    username = str(body.get("username", "anonymous"))
    exchange = body.get("exchange") or None
    currency = body.get("currency") or "USD"

    groq_key = request.headers.get("x-groq-key") or GROQ_API_KEY
    if not groq_key:
        raise HTTPException(500, "No Groq API key provided.")
    if card_id not in PROMPTS:
        raise HTTPException(400, f"Unknown card_id: {card_id}")

    # 1 — Live price + fundamentals
    price_data = {}
    fundamentals = {}
    try:
        params = {"symbol": ticker, "apikey": TWELVE_API_KEY}
        if exchange:
            params["exchange"] = exchange
        async with httpx.AsyncClient(timeout=15) as client:
            r  = await client.get("https://api.twelvedata.com/quote", params=params)
            pd = r.json()
            if pd.get("status") != "error":
                price_data = pd
            # Fetch Finnhub fundamentals + analyst targets in same client
            try:
                fundamentals = await fetch_finnhub(client, ticker)
            except Exception:
                fundamentals = {}
    except Exception:
        pass

    # 2 — Web search
    query          = SEARCH_QUERIES[card_id](ticker)
    search_results = await web_search(query)
    search_block   = format_search_block(search_results)

    price = safe_float(price_data.get("close") or price_data.get("price"))
    if not price and not search_results:
        raise HTTPException(422, f"No data found for {ticker}. Check the ticker symbol.")

    # 3 — Build context for AI
    cur = price_data.get("currency") or currency
    fw  = price_data.get("fifty_two_week") or {}

    def fval(v):
        return v if v not in (None, "", 0) else "N/A — not in verified data"

    target_mean = fundamentals.get("target_mean")
    upside_str = (
        f"{round(((target_mean - price) / price) * 100, 1)}%"
        if price and target_mean else "N/A"
    )

    live_block = "\n".join([
        f"## VERIFIED LIVE DATA FOR {ticker}",
        f"Fetched: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        f"Exchange: {price_data.get('exchange') or exchange or 'N/A'} | Currency: {cur}",
        f"Current Price: {price or 'N/A'} {cur}",
        f"Previous Close: {price_data.get('previous_close') or 'N/A'}",
        f"52W Low: {fw.get('low') or 'N/A'} | 52W High: {fw.get('high') or 'N/A'}",
        f"Change: {price_data.get('change') or 'N/A'} ({price_data.get('percent_change') or 'N/A'}%)",
        f"Volume: {price_data.get('volume') or 'N/A'}",
        f"Company: {price_data.get('name') or fundamentals.get('company_name') or ticker}",
        f"Sector/Industry: {fval(fundamentals.get('sector') or fundamentals.get('industry'))}",
        f"Market Cap: {fval(fundamentals.get('market_cap'))}",
        f"Forward P/E: {fval(fundamentals.get('fwd_pe'))}",
        f"Gross Margin: {fval(fundamentals.get('gross_margin'))}",
        f"Revenue Growth YoY: {fval(fundamentals.get('revenue_growth'))}",
        "",
        "### ANALYST TARGETS (from Finnhub)",
        f"Target Low: {fval(fundamentals.get('target_low'))}",
        f"Target Mean: {fval(fundamentals.get('target_mean'))}",
        f"Target Median: {fval(fundamentals.get('target_median'))}",
        f"Target High: {fval(fundamentals.get('target_high'))}",
        f"Upside to Mean Target: {upside_str}",
        f"Analyst Consensus: {fval(fundamentals.get('recommendation'))} ({fundamentals.get('num_analysts') or '?'} analysts)",
        "",
        "RULES: These figures are ground truth. Do not modify or replace them.",
        'Any missing figure must be labelled "N/A — not in verified data".',
    ])

    system_prompt = "\n\n".join([
        f"You are a senior equity analyst. Today: {datetime.utcnow().strftime('%d %b %Y')}.",
        live_block,
        search_block,
        "\n".join([
            "STRICT RULES:",
            "1. Never invent numbers not in the verified data",
            '2. Missing figures: write "N/A — not in verified data"',
            '3. Training-knowledge figures: label "(as of latest known reporting)"',
            "4. State source for every claim: verified data / web search / training",
            '5. Never write "recently" without a specific date',
        ]),
    ])

    user_prompt = PROMPTS[card_id](ticker)

    # 4 — Stream from Groq
    async def generate():
        sources_meta = [{"title": x.get("title", ""), "url": x.get("link", "")} for x in search_results]
        yield f"data: {json.dumps({'type':'meta','sources':sources_meta,'currency':cur})}\n\n"

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
                            {"role": "user",   "content": user_prompt},
                        ],
                    },
                ) as resp:
                    if resp.status_code != 200:
                        raw_err = await resp.aread()
                        try:
                            msg = json.loads(raw_err).get("error", {}).get("message", resp.status_code)
                        except Exception:
                            msg = resp.status_code
                        yield f"data: {json.dumps({'type':'error','message':str(msg)})}\n\n"
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

# ── HISTORY ───────────────────────────────────────────────────────────────────
@app.get("/api/history/{username}/{ticker}")
async def get_history(username: str, ticker: str):
    return {"history": get_user_ticker(username, ticker.upper()).get("history", [])}
