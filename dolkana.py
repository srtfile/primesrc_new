#!/usr/bin/env python3
"""
primesrc_pipeline.py  –  Unified PrimeSrc pipeline
====================================================

Stage 1  (primesrcembed.py logic)
    Read multiple_primesrc.txt  →  fetch /api/v1/s for every tmdb embed URL
    →  collect all server option keys  →  write api_url_list.txt

Stage 2  (extract_primesrc_urls.py logic  — copied verbatim and integrated)
    Read api_url_list.txt  →  send every /api/v1/l?key=… to FlareSolverr
    →  extract stream URL from the JSON response
    →  extract stream / embed link URL  →  write final_stream_urls.txt

Stage 3  –  GitHub sync
    Fetch pipeline_summary.json (and pipeline_summary-2.json, -3.json …)
    from the target GitHub repo via the Contents API.
    Merge new results in (upsert by tmdb_id, deduplicate sources).
    Auto-split: when a file reaches ≥ GITHUB_FILE_SIZE_LIMIT bytes,
    overflow entries are written to the next numbered file.
    Push every changed file back via a single authenticated PUT.

Extras
  - Single CLI entry point, no manual hand-off between scripts
  - Stage 1 uses plain urllib (no browser overhead)
  - --skip-stage1 / --skip-stage2 for incremental runs
  - Deduplication of keys before Stage 2 runs
  - JSON summary + dark HTML report written at the end
  - Graceful Ctrl-C at any stage

GitHub env vars required for Stage 3:
  GH_TOKEN   – personal access token (repo scope)
  GH_REPO    – owner/repo  (e.g. srtfile/movie-data)
  GH_BRANCH  – branch to push to (default: main)
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import gzip
import json
import os
import re
import sys
import time
import warnings
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlparse
from urllib.request import Request, urlopen

warnings.filterwarnings("ignore", category=ResourceWarning)

# ═══════════════════════════════════════════════════════════════
# PATHS & TUNABLES
# ═══════════════════════════════════════════════════════════════

HERE                 = Path(__file__).parent
DEFAULT_INPUT_FILE   = HERE / "multiple_primesrc.txt" 
DEFAULT_API_LIST     = HERE / "api_url_list.txt"
DEFAULT_STREAM_OUT   = HERE / "final_stream_urls.txt"
DEFAULT_JSON_SUMMARY = HERE / "pipeline_summary.json"
DEFAULT_HTML_OUT     = HERE / "pipeline_report.html"

STAGE1_REQUEST_TIMEOUT = 20   # urllib timeout per /api/v1/s call
STAGE2_BATCH_SIZE      = 5    # concurrent FlareSolverr requests
STAGE2_RELOADS         = 2    # retry attempts per failed URL
STAGE2_FINAL_RETRIES   = 1    # extra full retry passes for still-failed keys

TMDB_ID_RE = re.compile(r"^\d+$")

# ═══════════════════════════════════════════════════════════════
# GITHUB SYNC CONSTANTS
# ═══════════════════════════════════════════════════════════════

# Maximum size (bytes) for a single pipeline_summary*.json file before
# overflow entries are written to the next numbered file (-2, -3, …).
GITHUB_FILE_SIZE_LIMIT = 20 * 1024 * 1024   # 20 MB

# Base filename (without extension) used for the summary files.
GITHUB_BASE_FILENAME   = "pipeline_summary"

# GitHub API root
GITHUB_API_ROOT        = "https://api.github.com"

# ═══════════════════════════════════════════════════════════════
# CONSOLE HELPERS
# ═══════════════════════════════════════════════════════════════

_RESET  = "\033[0m"
_GREEN  = "\033[92m"
_YELLOW = "\033[93m"
_RED    = "\033[91m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"

def _c(text: str, colour: str) -> str:
    try:
        return colour + text + _RESET if sys.stdout.isatty() else text
    except Exception:
        return text

def log_info(msg: str) -> None: print(_c(f"[INFO]  {msg}", _CYAN))
def log_ok(msg: str)   -> None: print(_c(f"[OK]    {msg}", _GREEN))
def log_warn(msg: str) -> None: print(_c(f"[WARN]  {msg}", _YELLOW))
def log_err(msg: str)  -> None: print(_c(f"[ERR]   {msg}", _RED))
def log_head(msg: str) -> None: print(_c(f"\n{'='*60}\n{msg}\n{'='*60}", _BOLD))

# ═══════════════════════════════════════════════════════════════
# STAGE 1  –  embed URLs → /api/v1/s → api_url_list.txt
# ═══════════════════════════════════════════════════════════════

@dataclass
class ServerOption:
    server_name: str
    key: str
    api_url: str
    main_url: str
    title: str = ""
    quality: str = ""
    audio_language: str = ""


def _build_server_api_url(main_url: str) -> str:
    parsed = urlparse(main_url)
    params = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if parsed.path.startswith("/embed/movie"):
        params.setdefault("type", "movie")
    elif parsed.path.startswith("/embed/tv"):
        params.setdefault("type", "tv")
    base = f"{parsed.scheme or 'https'}://{parsed.netloc or 'primesrc.me'}"
    return f"{base}/api/v1/s?{urlencode(params)}"


def _fetch_json_http(url: str, referer: str) -> Any:
    req = Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json, */*",
            "Referer": referer,
        },
    )
    with urlopen(req, timeout=STAGE1_REQUEST_TIMEOUT) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return json.loads(resp.read().decode(charset, errors="replace"))


def _normalise_embed_url(raw: str, media_type: str = "movie") -> str:
    raw = raw.strip()
    if TMDB_ID_RE.fullmatch(raw):
        return f"https://primesrc.me/embed/{media_type}?tmdb={raw}"
    if raw.startswith("primesrc.me/"):
        return "https://" + raw
    if raw.startswith("/embed/"):
        return "https://primesrc.me" + raw
    return raw


def _find_server_lists(obj: Any) -> list[dict[str, Any]]:
    lists: list[dict[str, Any]] = []
    if isinstance(obj, dict):
        servers = obj.get("servers")
        if isinstance(servers, list) and servers:
            if any(
                "key" in item or "file_name" in item
                for item in servers
                if isinstance(item, dict)
            ):
                info = obj.get("info") if isinstance(obj.get("info"), dict) else {}
                lists.append({"servers": servers, "info": info})
        for v in obj.values():
            lists.extend(_find_server_lists(v))
    elif isinstance(obj, list):
        for item in obj:
            lists.extend(_find_server_lists(item))
    return lists


def _options_from_server_list(servers: list[dict], main_url: str) -> list[ServerOption]:
    options: list[ServerOption] = []
    for item in servers:
        key  = str(item.get("key")  or "").strip()
        name = str(item.get("name") or "").strip()
        if not key:
            continue
        options.append(ServerOption(
            server_name   = name,
            key           = key,
            api_url       = f"https://primesrc.me/api/v1/l?key={quote(key, safe='')}",
            main_url      = main_url,
            title         = str(item.get("file_name")       or "").strip(),
            quality       = str(item.get("quality")         or "").strip(),
            audio_language= str(item.get("audio_language")  or "").strip(),
        ))
    return options


def stage1_fetch_api_keys(
    input_file: Path,
    api_list_file: Path,
    media_type: str = "movie",
) -> list[ServerOption]:
    log_head("STAGE 1  –  Fetch server keys from PrimeSrc /api/v1/s")

    raw_lines = [
        l.strip()
        for l in input_file.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.startswith("#")
    ]
    log_info(f"Input embed URLs : {len(raw_lines)}  ({input_file})")

    seen_urls: set[str] = set()
    embed_urls: list[str] = []
    for raw in raw_lines:
        url = _normalise_embed_url(raw, media_type)
        if url not in seen_urls:
            seen_urls.add(url)
            embed_urls.append(url)

    all_options: list[ServerOption] = []
    errors: list[tuple[str, str]] = []

    for idx, embed_url in enumerate(embed_urls, 1):
        label = f"  [{idx:>4}/{len(embed_urls)}]"
        api_url = _build_server_api_url(embed_url)
        try:
            obj = _fetch_json_http(api_url, embed_url)
            server_lists = _find_server_lists(obj)
            if not server_lists:
                log_warn(f"{label} no server list  {embed_url}")
                continue
            for sl in server_lists:
                opts = _options_from_server_list(sl.get("servers", []), embed_url)
                all_options.extend(opts)
            count = sum(
                len(_options_from_server_list(sl.get("servers", []), embed_url))
                for sl in server_lists
            )
            log_ok(f"{label} {count} keys  {embed_url}")
        except Exception as exc:
            errors.append((embed_url, str(exc)))
            log_err(f"{label} {exc}  {embed_url}")

    # Deduplicate by api_url
    seen_api: set[str] = set()
    unique_options: list[ServerOption] = []
    for opt in all_options:
        if opt.api_url not in seen_api:
            seen_api.add(opt.api_url)
            unique_options.append(opt)

    api_list_file.write_text(
        "\n".join(opt.api_url for opt in unique_options) + "\n",
        encoding="utf-8",
    )
    log_info(f"Total keys : {len(all_options)}  (unique: {len(unique_options)})")
    log_info(f"Errors     : {len(errors)}")
    log_ok(f"Written → {api_list_file}")

    if errors:
        log_warn("Failed embed URLs (stage 1):")
        for url, err in errors:
            log_warn(f"  {url}  → {err}")

    return unique_options


# ═══════════════════════════════════════════════════════════════
# STAGE 2  –  api_url_list.txt → FlareSolverr → stream URLs
#
#  FlareSolverr is a proxy server that bypasses Cloudflare and
#  similar anti-bot challenges.  It accepts a JSON POST to its
#  /v1 endpoint and returns the fully-rendered page content.
#
#  Self-host locally or in CI with Docker:
#    docker run -d -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest
#
#  Set FLARESOLVERR_URL env var if your instance runs elsewhere.
#  Default: http://localhost:8191
# ═══════════════════════════════════════════════════════════════

FLARESOLVERR_DEFAULT_URL = "http://localhost:8191"

# Timeout FlareSolverr should use internally (ms) when solving a challenge.
# The /api/v1/l?key=… endpoints don't need a challenge solve — they just
# need the Cloudflare cookie already in the session — so 30 s is ample.
FLARESOLVERR_MAX_TIMEOUT = 30_000  # ms

_print_lock: asyncio.Lock | None = None


async def safe_print(*a: Any, **kw: Any) -> None:
    async with _print_lock:  # type: ignore[union-attr]
        print(*a, **kw)


# ── JSON / URL helpers ──────────────────────────────────────────

def extract_json(text: str) -> Any:
    text = (text or "").strip()
    if not text:
        raise ValueError("Empty page content")
    if text[0] in "{[":
        return json.loads(text)
    s = text.find("{")
    e = text.rfind("}") + 1
    if s == -1 or e <= s:
        raise ValueError("No JSON object found in page")
    return json.loads(text[s:e])


def get_play_url(data: Any) -> str | None:
    if isinstance(data, dict):
        for key in ("link", "url", "file", "src", "stream"):
            v = data.get(key)
            if isinstance(v, str) and v.startswith(("http://", "https://")):
                return v
        for key in ("sources", "tracks", "streams"):
            items = data.get(key)
            if isinstance(items, list):
                for item in items:
                    if isinstance(item, str) and item.startswith(("http://", "https://")):
                        return item
                    if isinstance(item, dict):
                        nested = get_play_url(item)
                        if nested:
                            return nested
    elif isinstance(data, list):
        for item in data:
            nested = get_play_url(item)
            if nested:
                return nested
    return None


# ── FlareSolverr session management ────────────────────────────

def _flaresolverr_url(args: argparse.Namespace) -> str:
    return (
        os.environ.get("FLARESOLVERR_URL")
        or getattr(args, "flaresolverr_url", None)
        or FLARESOLVERR_DEFAULT_URL
    ).rstrip("/")


def _fs_post(base_url: str, payload: dict[str, Any], http_timeout: int = 120) -> dict[str, Any]:
    """Blocking POST to FlareSolverr /v1 endpoint (runs in executor)."""
    import urllib.error
    data = json.dumps(payload).encode("utf-8")
    req  = Request(
        f"{base_url}/v1",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=http_timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except urllib.error.HTTPError as exc:
        # FlareSolverr returns HTTP 500 with a JSON body describing the real error.
        # Read and surface it instead of a misleading "cannot reach" message.
        try:
            body = exc.read().decode("utf-8", errors="replace")
            fs_resp = json.loads(body)
            # Return it as a failed FlareSolverr response so callers can log properly.
            return {
                "status": "error",
                "message": fs_resp.get("message", body[:300]),
                "_http_status": exc.code,
            }
        except Exception:
            raise ConnectionError(
                f"FlareSolverr at {base_url}/v1 returned HTTP {exc.code}: {exc.reason}"
            ) from exc
    except urllib.error.URLError as exc:
        raise ConnectionError(
            f"Cannot reach FlareSolverr at {base_url}/v1 — "
            f"is it running?  ({exc})"
        ) from exc


async def _fs_create_session(base_url: str, session_id: str) -> None:
    """Create a persistent FlareSolverr session (reuses Cloudflare cookies)."""
    loop = asyncio.get_running_loop()
    resp = await loop.run_in_executor(
        None,
        lambda: _fs_post(base_url, {
            "cmd": "sessions.create",
            "session": session_id,
        }),
    )
    if resp.get("status") not in ("ok", "warning"):
        log_warn(f"FlareSolverr session.create status: {resp.get('status')} — {resp.get('message')}")
    else:
        log_ok(f"FlareSolverr session created: {session_id}")


async def _fs_destroy_session(base_url: str, session_id: str) -> None:
    loop = asyncio.get_running_loop()
    try:
        await loop.run_in_executor(
            None,
            lambda: _fs_post(base_url, {
                "cmd": "sessions.destroy",
                "session": session_id,
            }),
        )
        log_info(f"FlareSolverr session destroyed: {session_id}")
    except Exception:
        pass


def _check_flaresolverr_health(base_url: str) -> bool:
    """Return True if FlareSolverr /health responds OK."""
    try:
        with urlopen(f"{base_url}/health", timeout=5) as resp:
            body = json.loads(resp.read())
            return body.get("status") == "ok"
    except Exception:
        return False


# ── Per-URL resolver via FlareSolverr ──────────────────────────

def _direct_fetch_api_url(api_url: str, timeout: int = 20) -> Any:
    """
    Try to fetch a /api/v1/l?key=… URL directly with plain urllib.
    Returns parsed JSON on success, raises on HTTP 403/429/5xx (Cloudflare block).
    This avoids FlareSolverr entirely when the endpoint isn't behind a challenge.
    """
    import urllib.error
    # Use the embed page as referer so the site accepts the request
    parsed   = urlparse(api_url)
    referer  = f"{parsed.scheme}://{parsed.netloc}/embed/movie"
    req = Request(
        api_url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept":          "application/json, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer":         referer,
            "Origin":          f"{parsed.scheme}://{parsed.netloc}",
        },
    )
    with urlopen(req, timeout=timeout) as resp:
        charset = resp.headers.get_content_charset() or "utf-8"
        return json.loads(resp.read().decode(charset, errors="replace"))


def _parse_flaresolverr_response(resp: dict[str, Any]) -> Any:
    """
    Extract and parse the JSON body from a successful FlareSolverr response.
    FlareSolverr wraps bare-JSON responses in a minimal HTML skeleton;
    this strips the HTML and returns the parsed JSON object.
    """
    solution  = resp.get("solution", {})
    body_html = solution.get("response", "")
    body_text = body_html
    m = re.search(r"<body[^>]*>(.*?)</body>", body_html, re.S | re.I)
    if m:
        body_text = re.sub(r"<[^>]+>", "", m.group(1))
    return extract_json(body_text)


async def _resolve_one_flaresolverr(
    base_url: str,
    session_id: str,
    api_url: str,
    timeout_ms: int,
    reloads: int,
    sem: asyncio.Semaphore,
    index: int,
    total: int,
) -> dict[str, Any]:
    """
    Fetch one /api/v1/l?key=… URL and extract a play URL.

    Strategy (in order):
      1. Direct urllib fetch — works when the endpoint isn't behind a
         Cloudflare JS challenge (no browser overhead, no 500 from FS).
      2. FlareSolverr — fallback if the direct fetch is blocked (403/503).

    FlareSolverr returning HTTP 500 on raw-JSON endpoints is a known
    upstream bug: it tries to "solve" a page that has no challenge and
    its internal browser chokes.  We avoid that by only calling FS when
    the direct fetch actually fails with a Cloudflare-style block.
    """
    import urllib.error

    loop  = asyncio.get_running_loop()
    label = f"[{index:>3}/{total}]"

    async with sem:
        await safe_print(f"{label} → {api_url}")

        last_error: str | None = None

        for attempt in range(reloads + 1):
            if attempt:
                await safe_print(f"{label} ↻ retry {attempt}/{reloads}")
                await asyncio.sleep(1.5)

            # ── Step 1: direct urllib (fast path) ──────────────────
            try:
                data = await loop.run_in_executor(
                    None, lambda: _direct_fetch_api_url(api_url)
                )
                play_url = get_play_url(data)
                if play_url:
                    await safe_print(f"{label} ✓ (direct) {play_url}")
                    return {
                        "index":         index,
                        "api_url":       api_url,
                        "data":          data,
                        "extracted_url": play_url,
                        "method":        "direct",
                    }
                if isinstance(data, dict):
                    for candidate_key in ("url", "link", "redirect", "location"):
                        candidate = data.get(candidate_key, "")
                        if isinstance(candidate, str) and candidate.startswith("http"):
                            await safe_print(f"{label} ✓ (direct/redirect) {candidate}")
                            return {
                                "index":         index,
                                "api_url":       api_url,
                                "data":          data,
                                "extracted_url": candidate,
                                "method":        "direct",
                            }
                last_error = f"no play URL in direct response: {str(data)[:120]}"
                await safe_print(f"{label} ✗ (direct) {last_error}")
                # No point trying FlareSolverr — the data arrived fine, just no URL.
                continue

            except urllib.error.HTTPError as exc:
                if exc.code in (403, 429, 503):
                    # Likely a Cloudflare block — fall through to FlareSolverr.
                    await safe_print(
                        f"{label} ↷ direct blocked (HTTP {exc.code}), trying FlareSolverr…"
                    )
                else:
                    last_error = f"direct HTTP {exc.code}: {exc.reason}"
                    await safe_print(f"{label} ✗ (direct) {last_error}")
                    continue

            except Exception as exc:
                # Network error on direct fetch — fall through to FlareSolverr.
                await safe_print(f"{label} ↷ direct failed ({exc}), trying FlareSolverr…")

            # ── Step 2: FlareSolverr fallback ──────────────────────
            try:
                fs_resp = await loop.run_in_executor(
                    None,
                    lambda: _fs_post(base_url, {
                        "cmd":        "request.get",
                        "url":        api_url,
                        "maxTimeout": timeout_ms,
                        "session":    session_id,
                    }),
                )

                if fs_resp.get("status") != "ok":
                    last_error = (
                        f"FlareSolverr error: {fs_resp.get('message', '')}"
                        + (f" (HTTP {fs_resp.get('_http_status')})"
                           if "_http_status" in fs_resp else "")
                    )
                    await safe_print(f"{label} ✗ (FS) {last_error}")
                    continue

                data     = _parse_flaresolverr_response(fs_resp)
                play_url = get_play_url(data)

                if play_url:
                    await safe_print(f"{label} ✓ (FlareSolverr) {play_url}")
                    return {
                        "index":         index,
                        "api_url":       api_url,
                        "data":          data,
                        "extracted_url": play_url,
                        "method":        "flaresolverr",
                    }

                if isinstance(data, dict):
                    for candidate_key in ("url", "link", "redirect", "location"):
                        candidate = data.get(candidate_key, "")
                        if isinstance(candidate, str) and candidate.startswith("http"):
                            await safe_print(f"{label} ✓ (FS/redirect) {candidate}")
                            return {
                                "index":         index,
                                "api_url":       api_url,
                                "data":          data,
                                "extracted_url": candidate,
                                "method":        "flaresolverr",
                            }

                last_error = f"no play URL in FS response: {str(data)[:120]}"
                await safe_print(f"{label} ✗ (FS) {last_error}")

            except Exception as exc:
                last_error = str(exc)
                await safe_print(f"{label} ✗ (FS) {last_error}")

        return {
            "index":         index,
            "api_url":       api_url,
            "error":         last_error or "failed",
            "extracted_url": None,
        }


async def _process_batch_fs(
    base_url: str,
    session_id: str,
    indexed_urls: list[tuple[int, str]],
    total: int,
    timeout_ms: int,
    reloads: int,
    batch_size: int,
    title: str,
) -> list[dict[str, Any]]:
    print(f"\n{title}: resolving {len(indexed_urls)} URL(s)")
    sem   = asyncio.Semaphore(batch_size)
    tasks = [
        asyncio.create_task(
            _resolve_one_flaresolverr(
                base_url, session_id, url, timeout_ms, reloads, sem, index, total
            )
        )
        for index, url in indexed_urls
    ]
    return await asyncio.gather(*tasks)


# ── Stage 2 main runner ─────────────────────────────────────────

async def stage2_extract_stream_urls(
    api_list_file: Path,
    stream_out_file: Path,
    args: argparse.Namespace,
) -> list[dict[str, Any]]:
    log_head("STAGE 2  –  Resolve keys → stream/embed URLs via FlareSolverr")

    global _print_lock
    _print_lock = asyncio.Lock()

    api_urls = [
        l.strip()
        for l in api_list_file.read_text(encoding="utf-8").splitlines()
        if l.strip() and not l.startswith("#")
    ]
    if not api_urls:
        log_warn("api_url_list.txt is empty – nothing to resolve in Stage 2.")
        return []

    base_url    = _flaresolverr_url(args)
    timeout_ms  = getattr(args, "fs_timeout_ms", FLARESOLVERR_MAX_TIMEOUT)
    session_id  = f"primesrc_{int(time.time())}"

    log_info(f"API keys to resolve : {len(api_urls)}")
    log_info(f"FlareSolverr URL    : {base_url}")
    log_info(f"Batch size          : {args.batch_size}")
    log_info(f"Reloads per URL     : {args.reloads}")
    log_info(f"Final retry passes  : {args.final_retries}")
    log_info(f"Solver timeout      : {timeout_ms} ms")

    # ── Health check ────────────────────────────────────────────
    log_info("Checking FlareSolverr health…")
    if not _check_flaresolverr_health(base_url):
        log_err(
            f"FlareSolverr is not reachable at {base_url}\n"
            "  Start it with Docker:\n"
            "    docker run -d -p 8191:8191 ghcr.io/flaresolverr/flaresolverr:latest\n"
            "  Or set FLARESOLVERR_URL to point at your instance."
        )
        raise ConnectionError("FlareSolverr not reachable")
    log_ok("FlareSolverr is healthy")

    # ── Create shared session (reuses Cloudflare cookies across requests) ──
    await _fs_create_session(base_url, session_id)

    t_start = time.monotonic()
    results: list[dict[str, Any]] = []

    try:
        indexed = list(enumerate(api_urls, 1))
        batch_total = (len(indexed) + args.batch_size - 1) // args.batch_size

        for batch_num, start in enumerate(range(0, len(indexed), args.batch_size), 1):
            batch = indexed[start : start + args.batch_size]
            results.extend(await _process_batch_fs(
                base_url, session_id, batch, len(api_urls),
                timeout_ms, args.reloads, args.batch_size,
                f"Batch {batch_num}/{batch_total}",
            ))

        # Final retry passes for still-failed URLs
        for attempt in range(1, args.final_retries + 1):
            failed = [
                (item["index"], item["api_url"])
                for item in results
                if not item.get("extracted_url")
            ]
            if not failed:
                break
            log_info(f"Final retry pass {attempt}/{args.final_retries}: {len(failed)} URL(s)")
            retry_results  = await _process_batch_fs(
                base_url, session_id, failed, len(api_urls),
                timeout_ms, 0, args.batch_size,
                f"Final retry {attempt}/{args.final_retries}",
            )
            retry_by_index = {r["index"]: r for r in retry_results}
            results = [
                retry_by_index.get(item["index"], item)
                if not item.get("extracted_url")
                else item
                for item in results
            ]

    finally:
        await _fs_destroy_session(base_url, session_id)

    results.sort(key=lambda r: r.get("index", 0))

    # Write stream URLs to file
    lines: list[str] = []
    for item in results:
        if item.get("extracted_url"):
            lines.append(item["extracted_url"])
        else:
            lines.append(f"# FAILED: {item['api_url']}  ({item.get('error', 'no URL')})")
    stream_out_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log_ok(f"Stream URLs → {stream_out_file}")

    elapsed = time.monotonic() - t_start
    ok      = [r for r in results if r.get("extracted_url")]
    fails   = [r for r in results if not r.get("extracted_url")]

    log_head(f"STAGE 2 RESULTS  ({elapsed:.1f}s total)")
    for item in results:
        if item.get("extracted_url"):
            log_ok(item["extracted_url"])
        else:
            log_err(f"FAILED : {item['api_url']}  ({item.get('error', 'no URL')})")

    log_info(f"Success : {len(ok)} / {len(results)}    Failed : {len(fails)}")

    return results


# ═══════════════════════════════════════════════════════════════
# SUMMARY — JSON + HTML report
# ═══════════════════════════════════════════════════════════════

# ═══════════════════════════════════════════════════════════════
# TMDB TITLE LOOKUP  (no API key needed — uses public endpoint)
# ═══════════════════════════════════════════════════════════════

TMDB_API_KEY = "6fad3f86b8452ee232deb7977d7dcf58"   # optional: set your v3 key here for faster lookups

def _tmdb_request(path: str) -> dict:
    """Make a TMDB API request. Uses API key if set."""
    base = "https://api.themoviedb.org/3"
    sep  = "&" if "?" in path else "?"
    url  = f"{base}{path}{sep}language=en-US"
    if TMDB_API_KEY:
        url += f"&api_key={TMDB_API_KEY}"
    req = Request(url, headers={
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
    })
    with urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace"))


def _fetch_tmdb_info(tmdb_id: str) -> tuple[str, str]:
    """
    Returns (title, imdb_id) for a given TMDB movie ID.
    Hits /movie/{id} for title and /movie/{id}/external_ids for imdb_id.
    Falls back to ("", None) on any error.
    """
    title   = ""
    imdb_id = None
    try:
        data    = _tmdb_request(f"/movie/{tmdb_id}")
        title   = data.get("title") or data.get("original_title") or ""
        imdb_id = data.get("imdb_id") or None   # already "tt..." format
        if not imdb_id:
            # fallback: external_ids endpoint
            ext     = _tmdb_request(f"/movie/{tmdb_id}/external_ids")
            imdb_id = ext.get("imdb_id") or None
    except Exception as exc:
        log_warn(f"TMDB info fetch failed for tmdb={tmdb_id}: {exc}")
    return title, imdb_id


# ═══════════════════════════════════════════════════════════════
# GZIP / BASE64 COMPRESSOR
# ═══════════════════════════════════════════════════════════════

def _to_gz_b64_json(pretty_path: Path, gz_path: Path) -> None:
    """Read pretty JSON → gzip-compress → base64-encode → save as JSON wrapper."""
    raw   = pretty_path.read_bytes()
    gz    = gzip.compress(raw, compresslevel=9)
    b64   = base64.b64encode(gz).decode("ascii")
    wrapper = {
        "encoding":    "gzip+base64",
        "source_file": pretty_path.name,
        "compressed":  b64,
    }
    gz_path.write_text(json.dumps(wrapper, ensure_ascii=False), encoding="utf-8")
    log_ok(
        f"Compressed JSON → {gz_path}  "
        f"({len(raw):,} B → {len(gz):,} B gz → {len(b64):,} B b64)"
    )


# ═══════════════════════════════════════════════════════════════
# SUMMARY WRITER  (pretty JSON  +  gzip/base64 JSON)
# ═══════════════════════════════════════════════════════════════

def _format_summary_json(records: list[dict[str, Any]]) -> str:
    """
    Serialise the summary list to JSON with a custom layout:
      - Each record's header fields (serial, title, tmdb_id …) are on their own lines.
      - Each host-N / url-N *pair* is kept on a single line:
          "host-1": "dood.watch", "url-1": "https://…"
    The result is valid JSON (readable by json.loads) even though it isn't
    produced by the standard indent= formatter.
    """
    import re as _re

    def _jv(v: Any) -> str:
        """JSON-encode a scalar value."""
        return json.dumps(v, ensure_ascii=False)

    lines: list[str] = ["["]
    for rec_idx, rec in enumerate(records):
        lines.append("  {")
        # Collect keys in order; separate header keys from host-N/url-N pairs
        header_keys = ["serial", "title", "tmdb_id", "imdb_id", "extracted_at"]
        # Find how many source pairs exist
        n_sources = sum(1 for k in rec if _re.fullmatch(r"host-\d+", k))

        all_field_lines: list[str] = []

        # Header fields
        for hk in header_keys:
            if hk in rec:
                all_field_lines.append(f'    {_jv(hk)}: {_jv(rec[hk])}')

        # Source pairs — host-N and url-N on the same line
        for n in range(1, n_sources + 1):
            hkey = f"host-{n}"
            ukey = f"url-{n}"
            host_part = f'{_jv(hkey)}: {_jv(rec.get(hkey, ""))}'
            url_part  = f'{_jv(ukey)}: {_jv(rec.get(ukey, ""))}'
            all_field_lines.append(f"    {host_part}, {url_part}")

        # Join with commas; last field of last record has no trailing comma
        is_last_rec = rec_idx == len(records) - 1
        for fi, fl in enumerate(all_field_lines):
            is_last_field = fi == len(all_field_lines) - 1
            if is_last_field:
                lines.append(fl)          # no comma after last field in object
            else:
                lines.append(fl + ",")

        if is_last_rec:
            lines.append("  }")
        else:
            lines.append("  },")

    lines.append("]")
    return "\n".join(lines) + "\n"


def _write_summary(
    stage1_options: list[ServerOption],
    stage2_results: list[dict[str, Any]],
    json_path: Path,
    html_path: Path,  # kept for CLI compat — not used
) -> None:
    link_map = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}

    # ── 1. group new sources by tmdb_id ──────────────────────────
    new_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    tmdb_to_main: dict[str, str] = {}
    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups[tmdb].append({"host": urlparse(stream_url).netloc, "url": stream_url})
        tmdb_to_main.setdefault(tmdb, opt.main_url)

    # ── 2. load existing pretty JSON (if any) ────────────────────
    existing: list[dict[str, Any]] = []
    if json_path.exists():
        try:
            existing = json.loads(json_path.read_text(encoding="utf-8"))
            if not isinstance(existing, list):
                existing = []
            log_info(f"Loaded {len(existing)} existing entries from {json_path}")
        except Exception as exc:
            log_warn(f"Could not load existing JSON ({exc}) — starting fresh")
            existing = []

    # ── 3. upsert: merge new into existing keyed by tmdb_id ──────
    # One entry per movie; sources stored internally as a list,
    # deduplicated by url.
    index: dict[int, dict[str, Any]] = {}
    for e in existing:
        tmdb_int = e["tmdb_id"]
        # Reconstruct internal sources list from flat host-N / url-N keys
        sources: list[dict[str, str]] = []
        n = 1
        while f"host-{n}" in e:
            sources.append({"host": e[f"host-{n}"], "url": e[f"url-{n}"]})
            n += 1
        index[tmdb_int] = {
            "tmdb_id":      tmdb_int,
            "imdb_id":      e.get("imdb_id"),
            "title":        e.get("title", ""),
            "extracted_at": e["extracted_at"],
            "_sources":     sources,   # internal list, not written to JSON
        }

    extracted_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmdb_meta_cache: dict[int, tuple[str, Any]] = {}

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)

        if tmdb_int in index:
            entry = index[tmdb_int]
            existing_urls = {s["url"] for s in entry["_sources"]}
            added = [s for s in new_sources if s["url"] not in existing_urls]
            entry["_sources"].extend(added)
            entry["extracted_at"] = extracted_at
            log_info(f"  tmdb={tmdb_int} — merged {len(added)} new source(s)")
        else:
            if tmdb_int not in tmdb_meta_cache:
                log_info(f"  tmdb={tmdb_int} — fetching title + imdb_id…")
                title, imdb_id = _fetch_tmdb_info(tmdb_str)
                tmdb_meta_cache[tmdb_int] = (title, imdb_id)
                log_ok(f"  tmdb={tmdb_int} — '{title}'  imdb={imdb_id}")
            else:
                title, imdb_id = tmdb_meta_cache[tmdb_int]
            index[tmdb_int] = {
                "tmdb_id":      tmdb_int,
                "imdb_id":      imdb_id,
                "title":        title,
                "extracted_at": extracted_at,
                "_sources":     list(new_sources),
            }
            log_ok(f"  tmdb={tmdb_int} — '{title}'  sources: {len(new_sources)}")

    # ── 4. sort by tmdb_id, assign serial numbers ─────────────────
    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    # ── 5. build flat output: host-1/url-1, host-2/url-2, … ───────
    output: list[dict[str, Any]] = []
    for e in sorted_entries:
        row: dict[str, Any] = {
            "serial":       e["serial"],
            "title":        e.get("title", ""),
            "tmdb_id":      e["tmdb_id"],
            "imdb_id":      e.get("imdb_id"),
            "extracted_at": e["extracted_at"],
        }
        for n, src in enumerate(e["_sources"], 1):
            row[f"host-{n}"] = src["host"]
            row[f"url-{n}"]  = src["url"]
        output.append(row)

    # ── 6. write pretty JSON  (host-N and url-N on the same line) ──
    json_path.write_text(_format_summary_json(output), encoding="utf-8")
    log_ok(f"Pretty JSON → {json_path}")
    total_sources = sum(
        sum(1 for k in row if k.startswith("url-")) for row in output
    )
    log_info(f"Movies : {len(output)}   Sources : {total_sources}")

    # ── 7. write gzip/base64 JSON alongside the pretty one ───────
    gz_path = json_path.with_suffix("").with_suffix(".gz.json")
    _to_gz_b64_json(json_path, gz_path)


# ═══════════════════════════════════════════════════════════════
# GITHUB SYNC  –  fetch → merge → split → push
# ═══════════════════════════════════════════════════════════════

def _gh_filename(n: int) -> str:
    """Return the filename for the nth summary file (1-based)."""
    if n == 1:
        return f"{GITHUB_BASE_FILENAME}.json"
    return f"{GITHUB_BASE_FILENAME}-{n}.json"


def _gh_api_request(
    method: str,
    path: str,
    token: str,
    payload: dict[str, Any] | None = None,
    timeout: int = 30,
) -> dict[str, Any]:
    """
    Make an authenticated GitHub API request.
    path: relative to GITHUB_API_ROOT, e.g. '/repos/owner/repo/contents/file.json'
    Returns the parsed JSON response.
    """
    import urllib.error
    url  = GITHUB_API_ROOT + path
    data = json.dumps(payload).encode("utf-8") if payload else None
    req  = Request(
        url,
        data=data,
        headers={
            "Authorization":        f"Bearer {token}",
            "Accept":               "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "Content-Type":         "application/json",
            "User-Agent":           "primesrc-pipeline/1.0",
        },
        method=method,
    )
    try:
        with urlopen(req, timeout=timeout) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"GitHub API {method} {path} → HTTP {exc.code}: {body[:400]}"
        ) from exc


def _gh_get_file(
    token: str,
    repo: str,
    path: str,
    branch: str,
) -> tuple[list[dict[str, Any]], str | None]:
    """
    Fetch one pipeline_summary*.json file from GitHub.
    Returns (records, sha) where sha is None when the file doesn't exist yet.
    records is the parsed JSON array (empty list if file missing).
    """
    import urllib.error, urllib.request
    api_path = f"/repos/{repo}/contents/{path}?ref={branch}"
    try:
        meta = _gh_api_request("GET", api_path, token)
    except RuntimeError as exc:
        if "HTTP 404" in str(exc):
            return [], None
        raise

    # GitHub returns file content as base64 in meta['content']
    raw_b64 = meta.get("content", "").replace("\n", "")
    sha     = meta.get("sha")
    if not raw_b64:
        return [], sha
    try:
        raw_bytes = base64.b64decode(raw_b64)
        records   = json.loads(raw_bytes.decode("utf-8"))
        if not isinstance(records, list):
            records = []
        log_info(f"  GitHub ← {path}: {len(records)} entries (sha={sha[:7]})")
        return records, sha
    except Exception as exc:
        log_warn(f"  Could not parse {path} from GitHub ({exc}) — treating as empty")
        return [], sha


def _gh_push_file(
    token: str,
    repo: str,
    path: str,
    branch: str,
    content_bytes: bytes,
    sha: str | None,
    commit_msg: str,
) -> None:
    """
    Create or update a file in the GitHub repo via the Contents API.
    sha must be provided when updating an existing file; None for new files.
    """
    payload: dict[str, Any] = {
        "message": commit_msg,
        "content": base64.b64encode(content_bytes).decode("ascii"),
        "branch":  branch,
    }
    if sha:
        payload["sha"] = sha

    api_path = f"/repos/{repo}/contents/{path}"
    _gh_api_request("PUT", api_path, token, payload=payload, timeout=60)
    action = "updated" if sha else "created"
    log_ok(f"  GitHub → {path} {action} ({len(content_bytes):,} B)")


def _gh_fetch_all_summary_files(
    token: str,
    repo: str,
    branch: str,
) -> tuple[list[dict[str, Any]], list[tuple[str, str | None]]]:
    """
    Fetch every pipeline_summary*.json from the repo (1, 2, 3, …) until one
    is missing.  Returns:
      - all_records : merged list of all records across all files
      - file_meta   : [(filename, sha_or_None), …]  in order
    """
    all_records: list[dict[str, Any]] = []
    file_meta:   list[tuple[str, str | None]] = []

    for n in range(1, 9999):
        fname     = _gh_filename(n)
        records, sha = _gh_get_file(token, repo, fname, branch)
        file_meta.append((fname, sha))
        all_records.extend(records)
        if sha is None:
            # File doesn't exist yet — no more files to check
            break

    return all_records, file_meta


def _gh_split_records(
    records: list[dict[str, Any]],
) -> list[bytes]:
    """
    Serialize records into one or more JSON byte-strings, each ≤ GITHUB_FILE_SIZE_LIMIT.
    Returns a list of encoded file contents in order.
    Each chunk is a valid JSON array.
    """
    chunks:       list[bytes] = []
    current:      list[dict[str, Any]] = []
    current_size: int = 2   # "[\n" + "]"

    for rec in records:
        # Serialize this record alone to estimate its size
        rec_json = _format_summary_json([rec]).encode("utf-8")
        rec_size = len(rec_json) - 4  # subtract "[\n" prefix + "]\n" suffix

        if current and current_size + rec_size + 2 > GITHUB_FILE_SIZE_LIMIT:
            # Flush current chunk
            chunks.append(_format_summary_json(current).encode("utf-8"))
            current      = []
            current_size = 2

        current.append(rec)
        current_size += rec_size + 2   # +2 for comma + newline between records

    if current:
        chunks.append(_format_summary_json(current).encode("utf-8"))

    return chunks if chunks else [b"[]\n"]


def github_sync_summary(
    stage1_options: list["ServerOption"],
    stage2_results: list[dict[str, Any]],
    local_json_path: Path,
    token: str,
    repo: str,
    branch: str,
) -> None:
    """
    Full GitHub sync for the pipeline summary:
      1. Fetch all existing pipeline_summary*.json from GitHub
      2. Merge new results in (upsert by tmdb_id, deduplicate sources)
      3. Split merged records across files respecting GITHUB_FILE_SIZE_LIMIT
      4. Push only changed/new files back to GitHub
      5. Write the first chunk also to local_json_path for the artifact upload
    """
    log_head("STAGE 3  –  GitHub sync  →  " + repo)

    if not token:
        log_warn("GH_TOKEN not set — skipping GitHub sync")
        return
    if not repo:
        log_warn("GH_REPO not set — skipping GitHub sync")
        return

    # ── 1. Fetch all existing files ─────────────────────────────
    log_info(f"Fetching existing summary files from {repo} (branch: {branch})…")
    try:
        remote_records, file_meta = _gh_fetch_all_summary_files(token, repo, branch)
    except Exception as exc:
        log_err(f"Failed to fetch from GitHub: {exc}")
        return

    log_info(f"Remote total: {len(remote_records)} entries across {len(file_meta)} file(s)")

    # ── 2. Build upsert index from remote ───────────────────────
    link_map = {r["api_url"]: r.get("extracted_url") or "" for r in stage2_results}

    new_groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for opt in stage1_options:
        stream_url = link_map.get(opt.api_url, "")
        if not stream_url:
            continue
        qs   = dict(x.split("=", 1) for x in urlparse(opt.main_url).query.split("&") if "=" in x)
        tmdb = qs.get("tmdb", "")
        if not tmdb:
            continue
        new_groups[tmdb].append({"host": urlparse(stream_url).netloc, "url": stream_url})

    # Build index from remote
    index: dict[int, dict[str, Any]] = {}
    for e in remote_records:
        tmdb_int = int(e.get("tmdb_id", 0))
        if not tmdb_int:
            continue
        sources: list[dict[str, str]] = []
        n = 1
        while f"host-{n}" in e:
            sources.append({"host": e[f"host-{n}"], "url": e[f"url-{n}"]})
            n += 1
        index[tmdb_int] = {
            "tmdb_id":      tmdb_int,
            "imdb_id":      e.get("imdb_id"),
            "title":        e.get("title", ""),
            "extracted_at": e.get("extracted_at", ""),
            "_sources":     sources,
        }

    extracted_at    = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tmdb_meta_cache: dict[int, tuple[str, Any]] = {}

    for tmdb_str, new_sources in new_groups.items():
        tmdb_int = int(tmdb_str)
        if tmdb_int in index:
            entry         = index[tmdb_int]
            existing_urls = {s["url"] for s in entry["_sources"]}
            added         = [s for s in new_sources if s["url"] not in existing_urls]
            entry["_sources"].extend(added)
            entry["extracted_at"] = extracted_at
            log_info(f"  tmdb={tmdb_int} — merged {len(added)} new source(s)")
        else:
            if tmdb_int not in tmdb_meta_cache:
                log_info(f"  tmdb={tmdb_int} — fetching title + imdb_id…")
                title, imdb_id = _fetch_tmdb_info(tmdb_str)
                tmdb_meta_cache[tmdb_int] = (title, imdb_id)
                log_ok(f"  tmdb={tmdb_int} — '{title}'  imdb={imdb_id}")
            else:
                title, imdb_id = tmdb_meta_cache[tmdb_int]
            index[tmdb_int] = {
                "tmdb_id":      tmdb_int,
                "imdb_id":      imdb_id,
                "title":        title,
                "extracted_at": extracted_at,
                "_sources":     list(new_sources),
            }
            log_ok(f"  tmdb={tmdb_int} — '{title}'  sources: {len(new_sources)}")

    # ── 3. Sort + assign serials ─────────────────────────────────
    sorted_entries = sorted(index.values(), key=lambda x: x["tmdb_id"])
    for i, entry in enumerate(sorted_entries, 1):
        entry["serial"] = i

    # Build flat output rows
    output: list[dict[str, Any]] = []
    for e in sorted_entries:
        row: dict[str, Any] = {
            "serial":       e["serial"],
            "title":        e.get("title", ""),
            "tmdb_id":      e["tmdb_id"],
            "imdb_id":      e.get("imdb_id"),
            "extracted_at": e["extracted_at"],
        }
        for n, src in enumerate(e["_sources"], 1):
            row[f"host-{n}"] = src["host"]
            row[f"url-{n}"]  = src["url"]
        output.append(row)

    total_sources = sum(sum(1 for k in r if k.startswith("url-")) for r in output)
    log_info(f"Merged total: {len(output)} movies, {total_sources} sources")

    # ── 4. Split into chunks ≤ GITHUB_FILE_SIZE_LIMIT ───────────
    chunks = _gh_split_records(output)
    log_info(f"Split into {len(chunks)} file(s) ({GITHUB_FILE_SIZE_LIMIT // 1024 // 1024} MB limit each)")

    # ── 5. Write local copy of chunk 1 ──────────────────────────
    local_json_path.write_bytes(chunks[0])
    log_ok(f"Local JSON → {local_json_path}  ({len(chunks[0]):,} B)")
    gz_path = local_json_path.with_suffix("").with_suffix(".gz.json")
    _to_gz_b64_json(local_json_path, gz_path)

    # ── 6. Push to GitHub ────────────────────────────────────────
    # Extend file_meta if we now have more chunks than before
    while len(file_meta) < len(chunks):
        n = len(file_meta) + 1
        file_meta.append((_gh_filename(n), None))

    pushed = 0
    for i, chunk_bytes in enumerate(chunks):
        fname, sha = file_meta[i]

        # Skip push if content identical to what's already there
        # (saves a commit when nothing changed in a file)
        if sha is not None and remote_records:
            # We can't cheaply compare — always push when we have new data
            pass

        commit_msg = (
            f"Update {fname} via pipeline [{extracted_at}]"
            if sha else
            f"Create {fname} via pipeline [{extracted_at}]"
        )
        try:
            _gh_push_file(token, repo, fname, branch, chunk_bytes, sha, commit_msg)
            pushed += 1
        except Exception as exc:
            log_err(f"  Failed to push {fname}: {exc}")

    log_ok(f"GitHub sync complete — {pushed}/{len(chunks)} file(s) pushed")


# ═══════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════

def _parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="PrimeSrc unified pipeline: embed URLs → API keys → stream URLs"
    )
    # Paths
    p.add_argument("--input",       type=Path, default=DEFAULT_INPUT_FILE)
    p.add_argument("--api-list",    type=Path, default=DEFAULT_API_LIST)
    p.add_argument("--output",      type=Path, default=DEFAULT_STREAM_OUT)
    p.add_argument("--json-out",    type=Path, default=DEFAULT_JSON_SUMMARY)
    p.add_argument("--html-out",    type=Path, default=DEFAULT_HTML_OUT)
    # Pipeline control
    p.add_argument("--skip-stage1", action="store_true",
                   help="Skip Stage 1; use existing api_url_list.txt")
    p.add_argument("--skip-stage2", action="store_true",
                   help="Skip Stage 2; only collect keys, no FlareSolverr")
    p.add_argument("--type",        choices=("movie", "tv"), default="movie")
    # FlareSolverr / Stage 2
    p.add_argument("--flaresolverr-url",
                   default=None,
                   dest="flaresolverr_url",
                   help=(
                       "FlareSolverr base URL "
                       f"(default: {FLARESOLVERR_DEFAULT_URL} or $FLARESOLVERR_URL env var)"
                   ))
    p.add_argument("--fs-timeout",   type=int, default=FLARESOLVERR_MAX_TIMEOUT,
                   dest="fs_timeout_ms",
                   help="Max timeout FlareSolverr will wait per request (ms, default 30000)")
    p.add_argument("--batch-size",   type=int, default=STAGE2_BATCH_SIZE,
                   dest="batch_size",
                   help="Concurrent FlareSolverr requests (default 5)")
    p.add_argument("--reloads",      type=int, default=STAGE2_RELOADS,
                   help="Retry attempts per failed URL (default 2)")
    p.add_argument("--final-retries", type=int, default=STAGE2_FINAL_RETRIES,
                   dest="final_retries",
                   help="Extra full retry passes for still-failed keys (default 1)")
    # GitHub sync
    p.add_argument("--no-github-sync", action="store_true", default=False,
                   dest="no_github_sync",
                   help="Skip GitHub sync (Stage 3); results stay local only")
    p.add_argument("--gh-token",   default=None, dest="gh_token",
                   help="GitHub personal access token (default: $GH_TOKEN env var)")
    p.add_argument("--gh-repo",    default=None, dest="gh_repo",
                   help="GitHub repo owner/name (default: $GH_REPO env var)")
    p.add_argument("--gh-branch",  default=None, dest="gh_branch",
                   help="GitHub branch (default: $GH_BRANCH or 'main')")
    return p.parse_args(argv)


async def _run(args: argparse.Namespace) -> int:
    log_head("PrimeSRC UNIFIED PIPELINE")
    log_info(f"Input   : {args.input}")
    log_info(f"API list: {args.api_list}")
    log_info(f"Output  : {args.output}")

    stage1_options: list[ServerOption] = []
    stage2_results: list[dict[str, Any]] = []

    # Stage 1
    if args.skip_stage1:
        log_info("Stage 1 skipped — using existing api_url_list.txt")
    else:
        if not args.input.exists():
            log_err(f"Input file not found: {args.input}")
            return 1
        stage1_options = stage1_fetch_api_keys(args.input, args.api_list, args.type)

    # Stage 2
    if args.skip_stage2:
        log_info("Stage 2 skipped.")
    else:
        if not args.api_list.exists():
            log_err(f"API list not found: {args.api_list}")
            return 1
        try:
            stage2_results = await stage2_extract_stream_urls(
                args.api_list, args.output, args
            )
        except ImportError:
            log_err("FlareSolverr unreachable — is Docker running?  See --flaresolverr-url")
            return 2

    # Summary + GitHub sync
    if stage1_options or stage2_results:
        if not stage1_options and args.api_list.exists():
            # Reconstruct stubs when stage1 was skipped
            for line in args.api_list.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                key = line.split("key=")[-1] if "key=" in line else ""
                stage1_options.append(ServerOption("", key, line, ""))

        # Resolve GitHub credentials from args → env → defaults
        gh_token  = args.gh_token  or os.environ.get("GH_TOKEN", "")
        gh_repo   = args.gh_repo   or os.environ.get("GH_REPO",  "")
        gh_branch = args.gh_branch or os.environ.get("GH_BRANCH", "main")

        if not args.no_github_sync and gh_token and gh_repo:
            # Stage 3: fetch remote, merge, split, push — also writes local json
            github_sync_summary(
                stage1_options, stage2_results,
                args.json_out,
                gh_token, gh_repo, gh_branch,
            )
        else:
            if not args.no_github_sync and not gh_token:
                log_warn("GH_TOKEN not set — GitHub sync skipped; writing locally only")
            # Fallback: local-only write (original behaviour)
            _write_summary(stage1_options, stage2_results, args.json_out, args.html_out)

    log_head("DONE")
    if not args.skip_stage2 and stage2_results:
        ok = sum(1 for r in stage2_results if r.get("extracted_url"))
        log_ok(f"Stream URLs extracted : {ok} / {len(stage2_results)}")
        log_ok(f"Results written to    : {args.output}")
    return 0


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv if argv is not None else sys.argv[1:])
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:
        print("\nInterrupted by user.")
        return 130


if __name__ == "__main__":
    sys.exit(main())
