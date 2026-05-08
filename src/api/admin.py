from __future__ import annotations

import asyncio
import base64
import json
import urllib.request
from typing import Any
from urllib.parse import unquote, urlsplit


_META_KEYWORDS = (
    "剩余流量", "距离下次重置", "套餐到期", "官网", "官網", "重置",
    "expire", "traffic", "subscription", "流量", "到期",
)
_BLOCKED_REGION_KEYWORDS = (
    "香港", "港", "hong kong", "hongkong", " hk", "hk ",
    "新加坡", "狮城", "singapore", " sg", "sg ",
)
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
        pool.append({"name": name, "raw_uri": uri})
        seen.add(uri)
    return pool, excluded

