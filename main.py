from __future__ import annotations

import asyncio
import base64
import json
import os
import secrets
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit


PROXY_ROOT = Path(__file__).resolve().parent
ROOT = PROXY_ROOT if (PROXY_ROOT / "src").exists() else PROXY_ROOT.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    from src.api.admin import _build_auto_node_pool, _fetch_subscription
    from src.transport.codec import needs_worker
    from src.transport.worker import worker
except ModuleNotFoundError:
    import urllib.request

    _META_KEYWORDS = (
        "剩余流量", "距离下次重置", "套餐到期", "官网", "官網", "重置",
        "expire", "traffic", "subscription", "流量", "到期",
    )
    _BLOCKED_REGION_KEYWORDS = (
        "香港", "港", "hong kong", "hongkong", " hk", "hk ",
        "新加坡", "狮城", "singapore", " sg", "sg ",
    )
    _DIRECT_PROXY_SCHEMES = ("socks5://", "socks://", "http://", "https://")
    _NODE_SCHEMES = (
        "socks5://", "socks://", "http://", "https://", "ss://", "ssr://",
        "vmess://", "vless://", "trojan://", "hysteria://", "hysteria2://",
        "hy2://", "tuic://", "anytls://",
    )

    def _pad_b64(s: str) -> str:
        s = s.strip().replace("-", "+").replace("_", "/")
        return s + "=" * ((4 - len(s) % 4) % 4)

    def _decode_b64_text(raw: str) -> str:
        return base64.b64decode(_pad_b64(raw)).decode("utf-8", errors="replace")

    def _node_name(uri: str) -> str:
        if "#" in uri:
            return unquote(uri.rsplit("#", 1)[-1]).strip()
        if uri.startswith("vmess://"):
            try:
                data = json.loads(_decode_b64_text(uri[len("vmess://"):]))
                return str(data.get("ps") or data.get("name") or "").strip()
            except Exception:
                return ""
        split = urlsplit(uri)
        return unquote(split.hostname or "").strip()

    def _looks_like_meta(name: str, uri: str = "") -> bool:
        text = f"{name} {uri}".lower()
        return any(k.lower() in text for k in _META_KEYWORDS)

    def _looks_like_blocked_region(name: str, uri: str = "") -> bool:
        text = f" {name} {uri} ".lower()
        return any(k.lower() in text for k in _BLOCKED_REGION_KEYWORDS)

    def _parse_clash_yaml(text: str) -> list[dict[str, Any]]:
        try:
            import yaml
        except Exception:
            return []
        try:
            data = yaml.safe_load(text) or {}
        except Exception:
            return []
        proxies = data.get("proxies", []) if isinstance(data, dict) else []
        out: list[dict[str, Any]] = []
        if not isinstance(proxies, list):
            return out
        for item in proxies:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "").strip()
            server = str(item.get("server") or "").strip()
            port = item.get("port")
            node_type = str(item.get("type") or "").strip().lower()
            raw = str(item.get("raw_uri") or item.get("uri") or "").strip()
            if not raw and node_type in ("http", "socks5", "socks") and server and port:
                raw = f"{node_type}://{server}:{port}"
            if raw:
                out.append({"name": name or _node_name(raw), "raw_uri": raw, **item})
        return out

    async def _fetch_subscription(url: str) -> list[dict[str, Any]]:
        def _fetch() -> str:
            req = urllib.request.Request(url, headers={"User-Agent": "dynamic-proxy/1.0"})
            with urllib.request.urlopen(req, timeout=30) as resp:
                return resp.read().decode("utf-8", errors="replace")

        text = await asyncio.to_thread(_fetch)
        nodes: list[dict[str, Any]] = []
        if "proxies:" in text:
            nodes.extend(_parse_clash_yaml(text))
        candidates = [line.strip() for line in text.replace("\r", "\n").split("\n") if line.strip()]
        if not any(c.startswith(_NODE_SCHEMES) for c in candidates):
            try:
                decoded = _decode_b64_text(text)
                candidates = [line.strip() for line in decoded.replace("\r", "\n").split("\n") if line.strip()]
            except Exception:
                pass
        seen = {n.get("raw_uri") for n in nodes}
        for uri in candidates:
            if uri.startswith(_NODE_SCHEMES) and uri not in seen:
                nodes.append({"name": _node_name(uri), "raw_uri": uri})
                seen.add(uri)
        return nodes

    def _build_auto_node_pool(nodes: list[dict[str, Any]], config: dict[str, Any]) -> tuple[list[dict[str, str]], int]:
        allow_hk_sg = bool(config.get("allow_hk_sg_nodes", False))
        pool: list[dict[str, str]] = []
        excluded = 0
        seen: set[str] = set()
        for item in nodes:
            uri = str(item.get("raw_uri") or item.get("uri") or "").strip()
            name = str(item.get("name") or _node_name(uri) or "").strip()
            if not uri or uri in seen or _looks_like_meta(name, uri):
                excluded += 1
                continue
            if not allow_hk_sg and _looks_like_blocked_region(name, uri):
                excluded += 1
                continue
            if not uri.startswith(_DIRECT_PROXY_SCHEMES):
                item["unsupported_reason"] = "当前 standalone 镜像未包含 src/transport，复杂协议需要用仓库根目录构建或复制转换器。"
                excluded += 1
                continue
            pool.append({"name": name, "raw_uri": uri})
            seen.add(uri)
        return pool, excluded

    def needs_worker(uri: str) -> bool:
        return not uri.startswith(_DIRECT_PROXY_SCHEMES)

    class _StandaloneWorker:
        is_running = False

        def find_binary(self) -> str:
            return ""

        def start_with_uri(self, uri: str, name: str = "") -> str:
            raise RuntimeError("standalone proxy image has no src/transport worker; use http/socks nodes or build from repo root")

        def stop(self) -> None:
            return None

    worker = _StandaloneWorker()


DEFAULT_CONFIG = PROXY_ROOT / "config.json"
API_KEYS_FILE = ROOT / "config" / "api_keys.txt"
MODELS_FILE = ROOT / "config" / "models.json"
STATIC_DIR = Path(__file__).resolve().parent / "static"
DEFAULT_HOST = "0.0.0.0"
DEFAULT_PORT = 7860
SESSION_TTL = 7 * 24 * 3600
_SESSIONS: dict[str, float] = {}


def load_config() -> dict[str, Any]:
    raw = os.environ.get("CONFIG", "").strip()
    if raw:
        if raw.startswith(("{", "[")):
            return json.loads(raw)
        p = Path(raw)
        if p.exists():
            return json.loads(p.read_text(encoding="utf-8"))

    if DEFAULT_CONFIG.exists():
        return json.loads(DEFAULT_CONFIG.read_text(encoding="utf-8"))
    legacy_config = ROOT / "config" / "config.json"
    if legacy_config.exists():
        return json.loads(legacy_config.read_text(encoding="utf-8"))
    return {}


def subscription_urls(config: dict[str, Any]) -> list[str]:
    raw = config.get("subscription_urls")
    urls: list[str] = []
    if isinstance(raw, list):
        urls.extend(str(x).strip() for x in raw if str(x).strip())
    elif isinstance(raw, str) and raw.strip():
        urls.extend(x.strip() for x in raw.splitlines() if x.strip())
    legacy = str(config.get("subscription_url") or "").strip()
    if legacy:
        urls.append(legacy)
    out: list[str] = []
    for url in urls:
        if url not in out:
            out.append(url)
    return out


def config_port(config: dict[str, Any]) -> int:
    return int(config.get("port") or config.get("port_api") or DEFAULT_PORT)


def save_config(config: dict[str, Any]) -> None:
    DEFAULT_CONFIG.parent.mkdir(parents=True, exist_ok=True)
    DEFAULT_CONFIG.write_text(json.dumps(config, ensure_ascii=False, indent=2), encoding="utf-8")


def read_api_keys() -> list[dict[str, str]]:
    if not API_KEYS_FILE.exists():
        return []
    out: list[dict[str, str]] = []
    for line in API_KEYS_FILE.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split(":", 2)
        if len(parts) >= 2:
            out.append({
                "name": parts[0].strip(),
                "key": parts[1].strip(),
                "description": parts[2].strip() if len(parts) > 2 else "",
            })
    return out


def write_api_keys(keys: list[dict[str, str]]) -> None:
    API_KEYS_FILE.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# 格式: name:key:description"]
    for item in keys:
        name = str(item.get("name", "")).strip()
        key = str(item.get("key", "")).strip()
        desc = str(item.get("description", "")).strip()
        if name and key:
            lines.append(f"{name}:{key}:{desc}" if desc else f"{name}:{key}")
    API_KEYS_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def read_models() -> dict[str, Any]:
    if MODELS_FILE.exists():
        return json.loads(MODELS_FILE.read_text(encoding="utf-8"))
    return {"models": [], "alias_map": {}}


def write_models(data: dict[str, Any]) -> None:
    MODELS_FILE.parent.mkdir(parents=True, exist_ok=True)
    MODELS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


class DynamicProxy:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.nodes: list[dict[str, Any]] = []
        self.pool: list[dict[str, str]] = []
        self.index = 0
        self.request_count = 0
        self.active_name = ""
        self.active_proxy_url = ""
        self.active_index = -1
        self.lock = asyncio.Lock()

    async def refresh(self) -> None:
        urls = subscription_urls(self.config)
        if not urls:
            raise RuntimeError("subscription_urls is required")
        nodes: list[dict[str, Any]] = []
        for url in urls:
            nodes.extend(await _fetch_subscription(url))
        pool, excluded = _build_auto_node_pool(nodes, self.config)
        if not pool:
            raise RuntimeError("subscription produced no usable nodes")
        self.nodes = nodes
        self.pool = pool
        self.index = 0
        print(f"[proxy] loaded {len(pool)} nodes from {len(urls)} subscriptions, excluded {excluded}", flush=True)

    async def proxy_for_request(self) -> tuple[str, str]:
        rotate_every = max(1, int(self.config.get("rotate_every", 1) or 1))
        self.request_count += 1
        if not self.active_proxy_url or self.request_count == 1 or (self.request_count - 1) % rotate_every == 0:
            return await self.activate_next()
        return self.active_proxy_url, self.active_name

    async def activate_next(self) -> tuple[str, str]:
        if not self.pool:
            await self.refresh()

        async with self.lock:
            last_error: Exception | None = None
            for _ in range(len(self.pool)):
                node = self.pool[self.index % len(self.pool)]
                self.index = (self.index + 1) % len(self.pool)
                uri = node["raw_uri"]
                name = node.get("name", "")
                try:
                    if needs_worker(uri):
                        proxy_url = await asyncio.to_thread(worker.start_with_uri, uri, name)
                    elif uri.startswith(("socks5://", "socks://", "http://", "https://")):
                        proxy_url = uri
                    else:
                        continue
                    self.active_name = name
                    self.active_proxy_url = proxy_url
                    self.active_index = (self.index - 1) % len(self.pool)
                    print(f"[proxy] active node: {name or uri[:50]} -> {proxy_url}", flush=True)
                    return proxy_url, name
                except Exception as e:
                    last_error = e
                    print(f"[proxy] failed node: {name or uri[:50]}: {e}", flush=True)

        raise RuntimeError(f"no node could be activated: {last_error}")

    async def activate_uri(self, uri: str, name: str = "") -> str:
        async with self.lock:
            if needs_worker(uri):
                proxy_url = await asyncio.to_thread(worker.start_with_uri, uri, name)
            elif uri.startswith(("socks5://", "socks://", "http://", "https://")):
                proxy_url = uri
            else:
                raise RuntimeError(f"unsupported node: {uri[:30]}")
            self.active_name = name
            self.active_proxy_url = proxy_url
            for idx, node in enumerate(self.pool):
                if node.get("raw_uri") == uri:
                    self.active_index = idx
                    self.index = (idx + 1) % len(self.pool) if self.pool else 0
                    break
            print(f"[proxy] manually active node: {name or uri[:50]} -> {proxy_url}", flush=True)
            return proxy_url


def _admin_password(config: dict[str, Any]) -> str:
    return str(
        os.environ.get("ADMIN_PASSWORD")
        or config.get("admin_password")
        or ""
    ).strip()


def _proxy_password(config: dict[str, Any]) -> str:
    return str(
        os.environ.get("PROXY_PASSWORD")
        or config.get("proxy_password")
        or ""
    ).strip()


def _check_basic_password(auth: str, password: str) -> bool:
    if not password:
        return True
    if not auth.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(auth.split(" ", 1)[1]).decode("utf-8")
        _, _, pw = decoded.partition(":")
        return pw == password
    except Exception:
        return False


def _issue_session() -> str:
    token = secrets.token_urlsafe(32)
    _SESSIONS[token] = time.time() + SESSION_TTL
    return token


def _parse_headers(lines: list[str]) -> dict[str, str]:
    headers: dict[str, str] = {}
    for line in lines[1:]:
        if not line or ":" not in line:
            continue
        k, v = line.split(":", 1)
        headers[k.strip().lower()] = v.strip()
    return headers


def _is_authorized(headers: dict[str, str], config: dict[str, Any]) -> bool:
    password = _admin_password(config)
    if not password:
        return True

    auth = headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        token = auth[7:].strip()
        exp = _SESSIONS.get(token)
        if exp and exp > time.time():
            return True
    if _check_basic_password(auth, password):
        return True
    return False


def http_response(
    code: int,
    body: bytes,
    content_type: str = "application/json; charset=utf-8",
    headers: dict[str, str] | None = None,
) -> bytes:
    reason = {
        200: "OK",
        204: "No Content",
        302: "Found",
        400: "Bad Request",
        401: "Unauthorized",
        404: "Not Found",
        502: "Bad Gateway",
    }.get(code, "Error")
    extra = "".join(f"{k}: {v}\r\n" for k, v in (headers or {}).items())
    return (
        f"HTTP/1.1 {code} {reason}\r\n"
        f"Content-Type: {content_type}\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"{extra}"
        "Connection: close\r\n\r\n"
    ).encode("latin-1") + body


def json_response(data: Any, code: int = 200) -> bytes:
    return http_response(code, json.dumps(data, ensure_ascii=False).encode("utf-8"))


def unauthorized() -> bytes:
    return json_response({"detail": "Unauthorized"}, 401)


async def read_body(reader: asyncio.StreamReader, headers: dict[str, str]) -> bytes:
    try:
        length = int(headers.get("content-length", "0") or "0")
    except ValueError:
        length = 0
    return await reader.readexactly(length) if length > 0 else b""


def _admin_html() -> bytes:
    path = STATIC_DIR / "admin.html"
    if path.exists():
        return path.read_bytes()
    return status_page_bytes(0)


def status_page_bytes(count: int) -> bytes:
    body = (
        "<!doctype html><meta charset='utf-8'>"
        "<title>Dynamic Proxy</title>"
        "<body style='font-family:system-ui;margin:40px'>"
        "<h1>Dynamic HTTP Proxy</h1>"
        f"<p>Loaded nodes: {count}</p>"
        "<p>Open <a href='/admin'>/admin</a>.</p>"
        "</body>"
    )
    return body.encode("utf-8")


async def handle_local_request(
    proxy: DynamicProxy,
    method: str,
    target: str,
    headers: dict[str, str],
    body: bytes,
) -> bytes | None:
    path = urlsplit(target).path if target.startswith(("http://", "https://")) else target.split("?", 1)[0]
    if path == "/":
        return http_response(302, b"", headers={"Location": "/admin"})
    if path == "/favicon.ico":
        return http_response(204, b"", "text/plain")
    if path == "/admin":
        return http_response(200, _admin_html(), "text/html; charset=utf-8")
    if path == "/api/admin/login" and method == "POST":
        payload = json.loads(body.decode("utf-8") or "{}")
        if str(payload.get("password", "")) != _admin_password(proxy.config):
            return unauthorized()
        return json_response({"token": _issue_session(), "ttl_seconds": SESSION_TTL})
    if path == "/api/admin/logout" and method == "POST":
        return json_response({"status": "ok"})
    if path.startswith("/api/admin/") and not _is_authorized(headers, proxy.config):
        return unauthorized()
    if path == "/api/admin/settings":
        if method == "GET":
            return json_response({
                "port_api": config_port(proxy.config),
                "debug": bool(proxy.config.get("debug", False)),
                "max_retries": int(proxy.config.get("max_retries", 0) or 0),
                "proxy_url": proxy.active_proxy_url,
                "env_proxy_url_override": os.environ.get("PROXY_URL", ""),
                "admin_password_env_locked": bool(os.environ.get("ADMIN_PASSWORD", "").strip()),
                "anti429_enabled": bool(proxy.config.get("anti429_enabled", False)),
                "anti429_target": proxy.config.get("anti429_target", "system"),
                "force_no_stream": bool(proxy.config.get("force_no_stream", False)),
                "anti_tracking": bool(proxy.config.get("anti_tracking", False)),
                "drop_max_tokens": bool(proxy.config.get("drop_max_tokens", True)),
            })
        if method == "PUT":
            payload = json.loads(body.decode("utf-8") or "{}")
            for key in ("port", "port_api", "rotate_every", "proxy_password", "debug", "proxy_url"):
                if key in payload:
                    proxy.config[key] = payload[key]
            if "admin_password" in payload:
                if os.environ.get("ADMIN_PASSWORD", "").strip():
                    return json_response({"detail": "ADMIN_PASSWORD is locked by environment"}, 400)
                proxy.config["admin_password"] = str(payload["admin_password"])
            try:
                save_config(proxy.config)
            except Exception:
                pass
            return json_response({"status": "ok", "notes": []})
    if path == "/api/admin/keys":
        if method == "GET":
            keys = read_api_keys()
            if not keys and proxy.config.get("api_key"):
                keys = [{"name": "config_system", "key": str(proxy.config["api_key"]), "description": "from CONFIG"}]
            return json_response({"keys": keys})
        if method == "POST":
            payload = json.loads(body.decode("utf-8") or "{}")
            keys = [k for k in read_api_keys() if k.get("name") != payload.get("name")]
            keys.append({
                "name": str(payload.get("name", "")).strip(),
                "key": str(payload.get("key", "")).strip(),
                "description": str(payload.get("description", "")).strip(),
            })
            write_api_keys(keys)
            return json_response({"status": "ok"})
    if path.startswith("/api/admin/keys/") and method == "DELETE":
        name = path.rsplit("/", 1)[-1]
        from urllib.parse import unquote
        keys = [k for k in read_api_keys() if k.get("name") != unquote(name)]
        write_api_keys(keys)
        return json_response({"status": "ok"})
    if path == "/api/admin/models":
        if method == "GET":
            data = read_models()
            return json_response({"models": data.get("models", []), "alias_map": data.get("alias_map", {})})
        if method == "PUT":
            payload = json.loads(body.decode("utf-8") or "{}")
            data = read_models()
            if "models" in payload:
                data["models"] = [str(x).strip() for x in payload.get("models", []) if str(x).strip()]
            if "alias_map" in payload:
                data["alias_map"] = payload.get("alias_map") or {}
            write_models(data)
            return json_response({"status": "ok"})
    if path == "/api/admin/subscription" and method == "GET":
        return json_response({"url": "\n".join(subscription_urls(proxy.config))})
    if path == "/api/admin/subscribe" and method == "POST":
        payload = json.loads(body.decode("utf-8") or "{}")
        if "url" in payload:
            proxy.config["subscription_urls"] = [
                u.strip() for u in str(payload["url"]).splitlines() if u.strip()
            ]
        await proxy.refresh()
        if proxy.pool:
            await proxy.activate_next()
        try:
            save_config(proxy.config)
        except Exception:
            pass
        nodes = proxy.nodes
        return json_response({
            "total": len(nodes),
            "usable_count": len(nodes),
            "pool": proxy.pool,
            "pool_count": len(proxy.pool),
            "nodes": nodes,
        })
    if path == "/api/admin/node-pool":
        if method == "GET":
            return json_response({"pool": proxy.pool, "current_index": proxy.active_index})
        if method == "POST":
            payload = json.loads(body.decode("utf-8") or "{}")
            pool = []
            for item in payload.get("pool", []):
                uri = str(item.get("raw_uri", "")).strip()
                if uri:
                    pool.append({"raw_uri": uri, "name": str(item.get("name", ""))})
            proxy.pool = pool
            proxy.index = 0
            return json_response({"status": "ok", "count": len(pool)})
    if path == "/api/admin/use-node" and method == "POST":
        payload = json.loads(body.decode("utf-8") or "{}")
        uri = str(payload.get("raw_uri", "")).strip()
        name = str(payload.get("name", ""))
        if not uri:
            return json_response({"detail": "raw_uri required"}, 400)
        proxy_url = await proxy.activate_uri(uri, name)
        return json_response({"status": "ok", "proxy_url": proxy_url, "via": "worker", "node_name": name})
    if path == "/api/admin/stop-proxy" and method == "POST":
        try:
            worker.stop()
        except Exception:
            pass
        proxy.active_name = ""
        proxy.active_proxy_url = ""
        proxy.active_index = -1
        return json_response({"status": "ok"})
    if path == "/api/admin/proxy-status" and method == "GET":
        return json_response({
            "binary_available": bool(worker.find_binary()),
            "running": worker.is_running,
            "proxy_url": proxy.active_proxy_url,
            "configured_proxy_url": proxy.active_proxy_url,
            "active_node_uri": proxy.pool[proxy.active_index]["raw_uri"] if 0 <= proxy.active_index < len(proxy.pool) else "",
            "active_node_name": proxy.active_name,
        })

    # Legacy small API used by the first lightweight page. Keep it working.
    if path == "/api/login" and method == "POST":
        payload = json.loads(body.decode("utf-8") or "{}")
        if str(payload.get("password", "")) != _admin_password(proxy.config):
            return unauthorized()
        return json_response({"token": _issue_session(), "ttl_seconds": SESSION_TTL})
    if path.startswith("/api/") and not _is_authorized(headers, proxy.config):
        return unauthorized()
    if path == "/api/status":
        return json_response({
            "subscription_url": proxy.config.get("subscription_url", ""),
            "subscription_urls": subscription_urls(proxy.config),
            "allow_hk_sg_nodes": bool(proxy.config.get("allow_hk_sg_nodes", False)),
            "rotate_every": int(proxy.config.get("rotate_every", 1) or 1),
            "proxy_password_set": bool(_proxy_password(proxy.config)),
            "pool": proxy.pool,
            "pool_count": len(proxy.pool),
            "active_name": proxy.active_name,
            "active_proxy_url": proxy.active_proxy_url,
            "active_index": proxy.active_index,
            "next_index": proxy.index,
        })
    if path == "/api/refresh" and method == "POST":
        payload = json.loads(body.decode("utf-8") or "{}")
        if "subscription_url" in payload:
            proxy.config["subscription_url"] = str(payload["subscription_url"]).strip()
        if "subscription_urls" in payload:
            proxy.config["subscription_urls"] = payload["subscription_urls"]
        if "allow_hk_sg_nodes" in payload:
            proxy.config["allow_hk_sg_nodes"] = bool(payload["allow_hk_sg_nodes"])
        if "rotate_every" in payload:
            proxy.config["rotate_every"] = int(payload["rotate_every"] or 1)
        if "proxy_password" in payload:
            proxy.config["proxy_password"] = str(payload["proxy_password"])
        await proxy.refresh()
        if proxy.pool:
            await proxy.activate_next()
        return json_response({"status": "ok", "pool": proxy.pool, "pool_count": len(proxy.pool)})
    if path == "/api/use-node" and method == "POST":
        payload = json.loads(body.decode("utf-8") or "{}")
        uri = str(payload.get("raw_uri", "")).strip()
        name = str(payload.get("name", ""))
        if not uri:
            return json_response({"error": "raw_uri required"}, 400)
        proxy_url = await proxy.activate_uri(uri, name)
        return json_response({"status": "ok", "proxy_url": proxy_url})
    if path == "/api/next" and method == "POST":
        proxy_url, name = await proxy.activate_next()
        return json_response({"status": "ok", "proxy_url": proxy_url, "name": name})
    if path.startswith("/api/"):
        return json_response({"error": "not found"}, 404)
    return None


async def socks5_connect(proxy_host: str, proxy_port: int, host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    reader, writer = await asyncio.open_connection(proxy_host, proxy_port)
    writer.write(b"\x05\x01\x00")
    await writer.drain()
    resp = await reader.readexactly(2)
    if resp != b"\x05\x00":
        writer.close()
        await writer.wait_closed()
        raise RuntimeError(f"SOCKS5 auth failed: {resp!r}")

    host_bytes = host.encode("idna")
    if len(host_bytes) > 255:
        raise RuntimeError("target host is too long")
    req = b"\x05\x01\x00\x03" + bytes([len(host_bytes)]) + host_bytes + port.to_bytes(2, "big")
    writer.write(req)
    await writer.drain()

    head = await reader.readexactly(4)
    if head[1] != 0:
        writer.close()
        await writer.wait_closed()
        raise RuntimeError(f"SOCKS5 connect failed: {head[1]}")
    atyp = head[3]
    if atyp == 1:
        await reader.readexactly(4)
    elif atyp == 3:
        ln = await reader.readexactly(1)
        await reader.readexactly(ln[0])
    elif atyp == 4:
        await reader.readexactly(16)
    await reader.readexactly(2)
    return reader, writer


async def connect_via(proxy_url: str, host: str, port: int) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
    parsed = urlsplit(proxy_url)
    scheme = parsed.scheme.lower()
    proxy_host = parsed.hostname or "127.0.0.1"
    proxy_port = parsed.port or 10808

    if scheme in ("socks5", "socks"):
        return await socks5_connect(proxy_host, proxy_port, host, port)

    if scheme in ("http", "https"):
        reader, writer = await asyncio.open_connection(proxy_host, proxy_port)
        writer.write(f"CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n".encode("latin-1"))
        await writer.drain()
        resp = await reader.readuntil(b"\r\n\r\n")
        if b" 200 " not in resp.split(b"\r\n", 1)[0]:
            writer.close()
            await writer.wait_closed()
            raise RuntimeError(resp.decode("latin-1", errors="replace").splitlines()[0])
        return reader, writer

    raise RuntimeError(f"unsupported upstream proxy: {proxy_url}")


async def relay(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    try:
        while not reader.at_eof():
            data = await reader.read(65536)
            if not data:
                break
            writer.write(data)
            await writer.drain()
    except Exception:
        pass
    finally:
        try:
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass


def error_response(code: int, message: str) -> bytes:
    return http_response(code, message.encode("utf-8", errors="replace"), "text/plain; charset=utf-8")


async def handle_client(proxy: DynamicProxy, client_reader: asyncio.StreamReader, client_writer: asyncio.StreamWriter) -> None:
    try:
        header = await client_reader.readuntil(b"\r\n\r\n")
        lines = header.decode("latin-1", errors="replace").split("\r\n")
        request_line = lines[0]
        method, target, version = request_line.split(" ", 2)
        headers = _parse_headers(lines)
        body = await read_body(client_reader, headers)

        local_response = await handle_local_request(proxy, method.upper(), target, headers, body)
        if local_response is not None:
            client_writer.write(local_response)
            await client_writer.drain()
            return

        if method.upper() != "CONNECT" and not target.startswith(("http://", "https://")):
            client_writer.write(http_response(200, status_page_bytes(len(proxy.pool)), "text/html; charset=utf-8"))
            await client_writer.drain()
            return

        if not _check_basic_password(headers.get("proxy-authorization", ""), _proxy_password(proxy.config)):
            client_writer.write(http_response(
                407,
                b"Proxy authentication required",
                "text/plain; charset=utf-8",
                {"Proxy-Authenticate": 'Basic realm="Dynamic Proxy"'}
            ))
            await client_writer.drain()
            return

        proxy_url, _ = await proxy.proxy_for_request()

        if method.upper() == "CONNECT":
            host, _, port_s = target.partition(":")
            port = int(port_s or 443)
            upstream_reader, upstream_writer = await connect_via(proxy_url, host, port)
            client_writer.write(b"HTTP/1.1 200 Connection Established\r\n\r\n")
            await client_writer.drain()
        else:
            parsed = urlsplit(target)
            host = parsed.hostname
            if not host:
                client_writer.write(error_response(400, "missing target host"))
                await client_writer.drain()
                return
            port = parsed.port or (443 if parsed.scheme == "https" else 80)
            path = parsed.path or "/"
            if parsed.query:
                path += "?" + parsed.query

            upstream_reader, upstream_writer = await connect_via(proxy_url, host, port)
            new_lines = [f"{method} {path} {version}"]
            for line in lines[1:]:
                if not line:
                    continue
                key = line.split(":", 1)[0].lower()
                if key in {"proxy-connection", "proxy-authorization", "connection", "content-length"}:
                    continue
                new_lines.append(line)
            upstream_writer.write(("\r\n".join(new_lines) + "\r\n\r\n").encode("latin-1"))
            if body:
                upstream_writer.write(body)
            await upstream_writer.drain()

        await asyncio.gather(
            relay(client_reader, upstream_writer),
            relay(upstream_reader, client_writer),
        )
    except Exception as e:
        try:
            client_writer.write(error_response(502, str(e)))
            await client_writer.drain()
        except Exception:
            pass
    finally:
        try:
            client_writer.close()
            await client_writer.wait_closed()
        except Exception:
            pass


async def main() -> None:
    config = load_config()
    proxy = DynamicProxy(config)
    await proxy.refresh()

    host = str(config.get("host") or DEFAULT_HOST)
    port = config_port(config)
    server = await asyncio.start_server(
        lambda r, w: handle_client(proxy, r, w),
        host,
        port,
    )
    print(f"[proxy] listening on http://{host}:{port}", flush=True)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
