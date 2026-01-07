import os
import json
import time
import uuid
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, AsyncIterator, Tuple, List
from contextlib import asynccontextmanager

import aiosqlite
import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import Response, StreamingResponse

OPENAI_BASE = "https://api.openai.com"
OPENAI_API_KEY = os.environ["OPENAI_API_KEY"]  # stored only on the relay server

DB_PATH = os.environ.get("RELAY_DB", "relay.db")
USERS_PATH = os.environ.get("USERS_PATH", "users.json")
PRICES_PATH = os.environ.get("PRICES_PATH", "prices.json")

# ----------------------------
# Hot-reloadable JSON loaders
# ----------------------------

class HotJSON:
    def __init__(self, path: str, default: Any):
        self.path = path
        self.default = default
        self._mtime = 0.0
        self._data = default
        self._lock = asyncio.Lock()

    async def get(self) -> Any:
        async with self._lock:
            try:
                st = os.stat(self.path)
                if st.st_mtime > self._mtime:
                    with open(self.path, "r", encoding="utf-8") as f:
                        self._data = json.load(f)
                    self._mtime = st.st_mtime
            except FileNotFoundError:
                self._data = self.default
            return self._data


users_store = HotJSON(USERS_PATH, {"users": []})
prices_store = HotJSON(PRICES_PATH, {})


# ----------------------------
# SQLite schema
# ----------------------------

CREATE_SQL = """
CREATE TABLE IF NOT EXISTS request_log (
  id TEXT PRIMARY KEY,
  ts_utc TEXT NOT NULL,
  day_utc TEXT NOT NULL,

  user_id TEXT NOT NULL,
  method TEXT NOT NULL,
  path TEXT NOT NULL,
  status_code INTEGER,
  stream INTEGER NOT NULL,

  model TEXT,
  openai_request_id TEXT,
  client_request_id TEXT,
  response_id TEXT,

  input_tokens INTEGER,
  output_tokens INTEGER,
  total_tokens INTEGER,
  cached_tokens INTEGER,
  reasoning_tokens INTEGER,

  cost_usd REAL NOT NULL DEFAULT 0.0,
  error TEXT,
  duration_ms INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_request_log_day_user ON request_log(day_utc, user_id);
CREATE INDEX IF NOT EXISTS idx_request_log_ts ON request_log(ts_utc);
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize database
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(CREATE_SQL)
        await db.commit()
    yield
    # Shutdown: nothing needed


app = FastAPI(lifespan=lifespan)


# ----------------------------
# Auth + per-user config
# ----------------------------

@dataclass
class UserCfg:
    user_id: str
    token: str
    requests_per_minute: int = 60
    burst: int = 20
    max_concurrent_streams: int = 2
    daily_usd_limit: float = 10.0
    daily_total_tokens_limit: int = 500_000


async def get_user_by_token(req: Request) -> UserCfg:
    auth = req.headers.get("authorization", "")
    if not auth.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    token = auth[len("Bearer "):].strip()

    data = await users_store.get()
    for u in data.get("users", []):
        if u.get("token") == token:
            return UserCfg(
                user_id=u["user_id"],
                token=u["token"],
                requests_per_minute=int(u.get("requests_per_minute", 60)),
                burst=int(u.get("burst", 20)),
                max_concurrent_streams=int(u.get("max_concurrent_streams", 2)),
                daily_usd_limit=float(u.get("daily_usd_limit", 10.0)),
                daily_total_tokens_limit=int(u.get("daily_total_tokens_limit", 500_000)),
            )
    raise HTTPException(status_code=401, detail="Invalid token")


# ----------------------------
# Rate limiting (in-memory token bucket) + stream concurrency
# NOTE: for multi-worker / multi-node deployments, move these to Redis.
# ----------------------------

class TokenBucket:
    def __init__(self, rate_per_sec: float, capacity: float):
        self.rate = rate_per_sec
        self.capacity = capacity
        self.tokens = capacity
        self.last = time.monotonic()
        self.lock = asyncio.Lock()

    async def take(self, amount: float = 1.0) -> bool:
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last
            self.last = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            if self.tokens >= amount:
                self.tokens -= amount
                return True
            return False


buckets: Dict[str, TokenBucket] = {}
concurrency: Dict[str, int] = {}
concurrency_lock = asyncio.Lock()


def bucket_for_user(cfg: UserCfg) -> TokenBucket:
    key = cfg.user_id
    if key not in buckets:
        rate = max(cfg.requests_per_minute, 1) / 60.0
        cap = max(cfg.burst, 1)
        buckets[key] = TokenBucket(rate, cap)
    return buckets[key]


async def acquire_stream_slot(cfg: UserCfg) -> None:
    async with concurrency_lock:
        cur = concurrency.get(cfg.user_id, 0)
        if cur >= cfg.max_concurrent_streams:
            raise HTTPException(status_code=429, detail="Too many concurrent streams")
        concurrency[cfg.user_id] = cur + 1


async def release_stream_slot(cfg: UserCfg) -> None:
    async with concurrency_lock:
        cur = concurrency.get(cfg.user_id, 0)
        concurrency[cfg.user_id] = max(cur - 1, 0)


# ----------------------------
# Daily quota checks (tokens / USD)
# ----------------------------

def utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


async def get_daily_usage(user_id: str, day_utc: str) -> Tuple[int, float]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            SELECT
              COALESCE(SUM(total_tokens), 0) AS total_tokens,
              COALESCE(SUM(cost_usd), 0.0)   AS cost_usd
            FROM request_log
            WHERE day_utc = ? AND user_id = ?
            """,
            (day_utc, user_id),
        )
        row = await cur.fetchone()
        return int(row[0] or 0), float(row[1] or 0.0)


async def enforce_daily_quota(cfg: UserCfg) -> None:
    day = utc_day()
    tokens, cost = await get_daily_usage(cfg.user_id, day)
    if tokens >= cfg.daily_total_tokens_limit:
        raise HTTPException(status_code=402, detail="Daily token quota exceeded")
    if cost >= cfg.daily_usd_limit:
        raise HTTPException(status_code=402, detail="Daily USD quota exceeded")


# ----------------------------
# Usage & cost helpers (Responses API)
# ----------------------------

def extract_usage_fields(usage: Dict[str, Any]) -> Dict[str, int]:
    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    total_tokens = int(usage.get("total_tokens") or 0)
    cached_tokens = int(((usage.get("input_tokens_details") or {}).get("cached_tokens")) or 0)
    reasoning_tokens = int(((usage.get("output_tokens_details") or {}).get("reasoning_tokens")) or 0)
    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "cached_tokens": cached_tokens,
        "reasoning_tokens": reasoning_tokens,
    }


def compute_cost_usd(model: str, usage: Dict[str, Any], prices: Dict[str, Dict[str, float]]) -> float:
    p = prices.get(model)
    if not p:
        return 0.0

    input_tokens = int(usage.get("input_tokens") or 0)
    output_tokens = int(usage.get("output_tokens") or 0)
    cached_tokens = int(((usage.get("input_tokens_details") or {}).get("cached_tokens")) or 0)
    non_cached = max(input_tokens - cached_tokens, 0)

    in_price = float(p.get("input", 0.0))
    cached_price = float(p.get("cached_input", in_price))
    out_price = float(p.get("output", 0.0))

    return (non_cached * in_price + cached_tokens * cached_price + output_tokens * out_price) / 1_000_000.0


# ----------------------------
# SSE parser (strict blocks)
# ----------------------------

def parse_sse_block(block: bytes) -> Tuple[Optional[str], str]:
    """
    Returns (event_name, data_str). Data supports multi "data:" lines.
    """
    text = block.decode("utf-8", errors="ignore")
    event_name: Optional[str] = None
    data_lines: List[str] = []

    for line in text.splitlines():
        if line.startswith("event:"):
            event_name = line[len("event:"):].strip()
        elif line.startswith("data:"):
            data_lines.append(line[len("data:"):].lstrip())
        else:
            pass

    data_str = "\n".join(data_lines).strip()
    return event_name, data_str


# ----------------------------
# Logging
# ----------------------------

async def log_request(
    *,
    log_id: str,
    user_id: str,
    method: str,
    path: str,
    status_code: Optional[int],
    stream: bool,
    model: Optional[str],
    openai_request_id: Optional[str],
    client_request_id: str,
    response_id: Optional[str],
    usage: Optional[Dict[str, Any]],
    cost_usd: float,
    error: Optional[str],
    started_ms: int,
):
    ts = datetime.now(timezone.utc).isoformat()
    day_utc = utc_day()
    duration_ms = max(int(time.time() * 1000) - started_ms, 0)

    fields: Dict[str, Any] = {
        "id": log_id,
        "ts_utc": ts,
        "day_utc": day_utc,
        "user_id": user_id,
        "method": method,
        "path": path,
        "status_code": status_code,
        "stream": 1 if stream else 0,
        "model": model,
        "openai_request_id": openai_request_id,
        "client_request_id": client_request_id,
        "response_id": response_id,
        "cost_usd": float(cost_usd),
        "error": error,
        "duration_ms": duration_ms,
        "input_tokens": None,
        "output_tokens": None,
        "total_tokens": None,
        "cached_tokens": None,
        "reasoning_tokens": None,
    }

    if usage:
        fields.update(extract_usage_fields(usage))

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """
            INSERT INTO request_log(
              id, ts_utc, day_utc, user_id, method, path, status_code, stream,
              model, openai_request_id, client_request_id, response_id,
              input_tokens, output_tokens, total_tokens, cached_tokens, reasoning_tokens,
              cost_usd, error, duration_ms
            ) VALUES (
              :id, :ts_utc, :day_utc, :user_id, :method, :path, :status_code, :stream,
              :model, :openai_request_id, :client_request_id, :response_id,
              :input_tokens, :output_tokens, :total_tokens, :cached_tokens, :reasoning_tokens,
              :cost_usd, :error, :duration_ms
            )
            """,
            fields,
        )
        await db.commit()


# ----------------------------
# Upstream request builders
# ----------------------------

def build_upstream_headers(req: Request, client_request_id: str) -> Dict[str, str]:
    headers = dict(req.headers)
    headers.pop("host", None)
    headers.pop("content-length", None)

    # Inject OpenAI key (server-side)
    headers["authorization"] = f"Bearer {OPENAI_API_KEY}"

    # Forward your own request id to OpenAI for easier debugging
    headers["x-client-request-id"] = client_request_id

    return headers


async def read_body_json(req: Request) -> Tuple[bytes, Optional[Dict[str, Any]]]:
    body = await req.body()
    try:
        j = json.loads(body.decode("utf-8")) if body else None
    except Exception:
        j = None
    return body, j


# ----------------------------
# Reporting endpoints
# ----------------------------

@app.get("/requests/recent")
async def requests_recent(limit: int = 50):
    limit = max(1, min(limit, 500))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM request_log ORDER BY ts_utc DESC LIMIT ?", (limit,))
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


@app.get("/stats/daily")
async def stats_daily(days: int = 30):
    days = max(1, min(days, 365))
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            """
            SELECT
              day_utc,
              user_id,
              COUNT(*) AS requests,
              SUM(COALESCE(input_tokens,0)) AS input_tokens,
              SUM(COALESCE(output_tokens,0)) AS output_tokens,
              SUM(COALESCE(total_tokens,0)) AS total_tokens,
              SUM(COALESCE(cost_usd,0)) AS cost_usd
            FROM request_log
            WHERE day_utc >= date('now', ?)
            GROUP BY day_utc, user_id
            ORDER BY day_utc DESC, user_id ASC
            """,
            (f"-{days} day",),
        )
        rows = await cur.fetchall()
        return [dict(r) for r in rows]


# ----------------------------
# Main proxy
# ----------------------------

@app.api_route("/v1/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def relay(path: str, request: Request):
    cfg = await get_user_by_token(request)

    # Per-user rate limit
    if not await bucket_for_user(cfg).take(1.0):
        raise HTTPException(status_code=429, detail="Rate limit exceeded")

    # Daily quotas (simple version; based on already recorded usage)
    await enforce_daily_quota(cfg)

    log_id = str(uuid.uuid4())
    client_request_id = log_id
    started_ms = int(time.time() * 1000)

    upstream_url = f"{OPENAI_BASE}/v1/{path}"
    params = dict(request.query_params)
    headers = build_upstream_headers(request, client_request_id)

    body_bytes, body_json = await read_body_json(request)
    model = body_json.get("model") if isinstance(body_json, dict) else None
    stream = bool(body_json.get("stream", False)) if isinstance(body_json, dict) else False

    prices = await prices_store.get()

    async with httpx.AsyncClient(timeout=None) as client:
        try:
            upstream_req = client.build_request(
                request.method,
                upstream_url,
                params=params,
                headers=headers,
                content=body_bytes,
            )

            # -------- STREAMING (SSE) --------
            if stream:
                await acquire_stream_slot(cfg)

                upstream_resp = await client.send(upstream_req, stream=True)
                openai_request_id = upstream_resp.headers.get("x-request-id")
                ct = upstream_resp.headers.get("content-type", "text/event-stream")

                usage_final: Optional[Dict[str, Any]] = None
                response_id: Optional[str] = None
                error_text: Optional[str] = None

                async def streamer() -> AsyncIterator[bytes]:
                    nonlocal usage_final, response_id, error_text
                    buf = b""
                    try:
                        async for chunk in upstream_resp.aiter_raw():
                            # 1) Forward raw bytes to client immediately
                            yield chunk

                            # 2) Parse SSE blocks for logging
                            buf += chunk
                            while b"\n\n" in buf:
                                block, buf = buf.split(b"\n\n", 1)
                                if not block.strip():
                                    continue
                                event_name, data_str = parse_sse_block(block)

                                if not data_str or data_str == "[DONE]":
                                    continue
                                if not data_str.startswith("{"):
                                    continue

                                try:
                                    obj = json.loads(data_str)
                                except Exception:
                                    continue

                                ev_type = event_name or obj.get("type")

                                # Final usage is available on response.completed
                                if ev_type == "response.completed":
                                    resp_obj = obj.get("response") or {}
                                    response_id = resp_obj.get("id")
                                    usage_final = resp_obj.get("usage")

                                if ev_type in ("response.failed", "response.incomplete"):
                                    resp_obj = obj.get("response") or {}
                                    if resp_obj.get("error"):
                                        error_text = str(resp_obj.get("error"))
                    finally:
                        await release_stream_slot(cfg)
                        cost = compute_cost_usd(model or "", usage_final or {}, prices) if (usage_final and model) else 0.0
                        await log_request(
                            log_id=log_id,
                            user_id=cfg.user_id,
                            method=request.method,
                            path=f"/v1/{path}",
                            status_code=upstream_resp.status_code,
                            stream=True,
                            model=model,
                            openai_request_id=openai_request_id,
                            client_request_id=client_request_id,
                            response_id=response_id,
                            usage=usage_final,
                            cost_usd=cost,
                            error=error_text,
                            started_ms=started_ms,
                        )

                return StreamingResponse(streamer(), status_code=upstream_resp.status_code, media_type=ct)

            # -------- NON-STREAM --------
            upstream_resp = await client.send(upstream_req, stream=False)
            openai_request_id = upstream_resp.headers.get("x-request-id")

            content_type = upstream_resp.headers.get("content-type", "application/json")
            raw = upstream_resp.content

            usage = None
            response_id = None
            cost = 0.0
            err = None

            if "application/json" in content_type:
                try:
                    data = upstream_resp.json()
                    if isinstance(data, dict):
                        response_id = data.get("id")
                        usage = data.get("usage")
                        if usage and model:
                            cost = compute_cost_usd(model, usage, prices)
                except Exception:
                    pass

            if upstream_resp.status_code >= 400:
                err = raw[:5000].decode("utf-8", errors="ignore")

            await log_request(
                log_id=log_id,
                user_id=cfg.user_id,
                method=request.method,
                path=f"/v1/{path}",
                status_code=upstream_resp.status_code,
                stream=False,
                model=model,
                openai_request_id=openai_request_id,
                client_request_id=client_request_id,
                response_id=response_id,
                usage=usage,
                cost_usd=cost,
                error=err,
                started_ms=started_ms,
            )

            return Response(content=raw, status_code=upstream_resp.status_code, media_type=content_type)

        except httpx.HTTPError as e:
            await log_request(
                log_id=log_id,
                user_id=cfg.user_id,
                method=request.method,
                path=f"/v1/{path}",
                status_code=None,
                stream=stream,
                model=model,
                openai_request_id=None,
                client_request_id=client_request_id,
                response_id=None,
                usage=None,
                cost_usd=0.0,
                error=str(e),
                started_ms=started_ms,
            )
            raise HTTPException(status_code=502, detail=f"Upstream error: {e}")
