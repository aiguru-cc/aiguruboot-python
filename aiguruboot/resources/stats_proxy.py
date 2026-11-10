#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""本地推理 API 网关 + 统计代理。

监听 --listen 端口，把 HTTP 请求转发到 --target 端口的引擎进程，
对外统一提供 OpenAI / DeepSeek / Anthropic 三套兼容接口，并按模型自动适配：

  OpenAI    POST /v1/responses、/v1/chat/completions、/v1/completions、/v1/embeddings、GET /v1/models
  DeepSeek  POST /chat/completions（无 /v1 前缀）；推理模型响应带 reasoning_content
  Anthropic POST /v1/messages（请求/响应/流式事件全量转换）

模型自动适配（--model/--model-id/--family/--reasoning 由插件启动时传入）：
  - 请求里的任意 model 名（deepseek-chat / claude-3-5-sonnet / gpt-4o / local…）
    一律改写为当前已加载的本地模型 id，避免 Ollama 等后端 404；
  - 推理型模型（deepseek-r1 / reasoner / glm-4.5 等）：OpenAI 响应拆出 reasoning_content；
  - Anthropic 请求自动翻译为 OpenAI 格式转发，响应/流式事件翻译回 Anthropic 格式。

同时解析推理响应的 usage 追加写入 --stats（JSON 行），供插件统计页使用。

用法:
  stats_proxy.py --listen 11080 --target 19080 --stats /path/usage.jsonl \
      [--model 名称] [--model-id 后端模型id] [--family deepseek|qwen|glm|grok|other] \
      [--reasoning 0|1] -- <引擎启动命令...>

子进程退出时代理随之退出（插件以此判断引擎存活）；代理收到 SIGTERM/SIGINT 时
先停转发、再终止子进程。
"""

import argparse
from collections import deque
from datetime import datetime, timezone
import hashlib
import hmac
import ipaddress
import json
import os
import re
import signal
import secrets
import subprocess
import sys
import threading
import time
import uuid
import xml.etree.ElementTree as ET
import base64
import tempfile
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit


INSTALLATION_PROPOSAL_TTL_SECONDS = 10 * 60
INSTALLATION_PROPOSAL_LOCK = threading.RLock()
# 提案创建限速：与 installer_agent.py 的独立服务保持同一水位，防止 LAN 刷审批队列。
INSTALLATION_RATE_LOCK = threading.Lock()
INSTALLATION_RATE_HITS = {}
INSTALLATION_RATE_LIMIT = 10
INSTALLATION_RATE_WINDOW = 60

STATS_LOCK = threading.Lock()
SECURITY_AUDIT_LOCK = threading.Lock()

# 推理输出中思考段的分隔标记（按出现位置取最早者拆分 thinking / answer）
THINKING_MARKERS = [
    "<|begin_of_thought|>",   # GLM-4.5 思维块起始（前半为思考）
    "<|end_of_thought|>",     # GLM-4.5 思维块结束
    "<|/think|>",
    "</think>",
    "<|im_start|>assistant",  # Qwen R1 蒸馏模板：思考后再次进入 assistant 段
    "### 回答",
    "Final Answer:",
]
MAX_REQUEST_BYTES = 32 * 1024 * 1024
MAX_BUFFERED_RESPONSE_BYTES = 128 * 1024 * 1024
# 本地 Agent 会把思考、工具参数和最终正文都计入单轮生成预算。4096 对长任务偏小，
# 尤其在强制结论轮容易留下语义未完成的正文；大型工作站场景统一采用更宽裕的缺省值。
DEFAULT_MAX_OUTPUT_TOKENS = 16384
MIN_TOOL_OUTPUT_TOKENS = 8192
MAX_RETRY_OUTPUT_TOKENS = 32768
MAX_IMAGE_DATA_URL_BYTES = 16 * 1024 * 1024
SECURITY_STATUS_PATH = "/v1/security/status"
SECURITY_RESET_PATH = "/v1/security/sessions/reset"
ALLOWED_GET_PATHS = {"/health", "/metrics", "/props", "/v1/models", "/v1/deployments/models",
                     SECURITY_STATUS_PATH, "/v1/status", "/v1/status/components",
                     "/v1/status/events", "/v1/status/history", "/v1/status/test-event", "/status"}
ALLOWED_POST_PATHS = {
    "/v1/chat/completions", "/chat/completions", "/v1/completions",
    "/v1/embeddings", "/v1/messages", "/v1/responses", "/v1/images/generations",
    "/v1/vision/ground",
    "/v1/deployments/models",
    SECURITY_RESET_PATH,
}
CLUSTER_PREFIX = "/v1/cluster/"
INSTALLATION_PREFIX = "/v1/installations/proposals"

# ---- 虚拟 API（Virtual API / vAPI）----
# 每个 Agent 拥有独立的虚拟 API：独立凭据、独立 base path、独立模型命名空间、
# 独立安全会话、独立并发配额与独立用量归因。所有状态按 vapi_id 分区，避免
# 多个 Agent 共用一个网关时互相污染上下文、误报模型身份或抢占推理槽。
VAPI_PREFIX = "/v1/a/"
VAPI_MANAGEMENT_PREFIX = "/v1/vapis"
LEGACY_VAPI_ID = "legacy"
VAPI_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
# 虚拟身份允许访问的引擎只读/推理接口；/api/pull|create|delete|copy|push 等管理面拒绝。
VAPI_SAFE_API_PATHS = {"/api/chat", "/api/generate", "/api/tags",
                       "/api/show", "/api/embed", "/api/embeddings", "/api/version"}


def normalize_virtual_path(path):
    """把 /v1/a/<vapi-id>/... 还原成内部路径，返回 (vapi_id, internal_path)。"""
    raw = path or "/"
    clean, _, query = raw.partition("?")
    if clean.startswith(VAPI_PREFIX):
        rest = clean[len(VAPI_PREFIX):]
        head, separator, tail = rest.partition("/")
        internal = "/" + tail if separator and tail else "/"
        return head, internal + ("?" + query if query else "")
    return None, raw


def is_vapi_management_path(clean_path):
    return clean_path == VAPI_MANAGEMENT_PREFIX or \
        clean_path.startswith(VAPI_MANAGEMENT_PREFIX + "/")


def valid_vapi_id(value):
    return isinstance(value, str) and bool(VAPI_ID_PATTERN.fullmatch(value))


def hash_credential(value):
    """Agent 凭据只以 SHA-256 摘要落盘；明文仅在签发/轮换响应里出现一次。"""
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def validate_vapi_models(models):
    """校验「虚拟别名 → 真实模型 id」映射，拒绝控制字符与超长值。"""
    if not isinstance(models, dict) or len(models) > 64:
        raise ValueError("models must be an object with at most 64 aliases")
    cleaned = {}
    for name, actual in models.items():
        if not isinstance(name, str) or not isinstance(actual, str) \
                or not name or not actual or len(name) > 128 or len(actual) > 128 \
                or any(ord(ch) < 32 for ch in name + actual):
            raise ValueError("models must map short virtual names to registered model ids")
        cleaned[name] = actual
    return cleaned


class ClientDisconnected(Exception):
    """客户端已断开；用于立即关闭后端流，停止无意义生成。"""


class ResponseTooLarge(Exception):
    """后端非流式响应超过内存安全上限。"""


def read_limited(stream, limit=MAX_BUFFERED_RESPONSE_BYTES):
    data = stream.read(limit + 1)
    if len(data) > limit:
        raise ResponseTooLarge()
    return data


def is_allowed_route(method, path):
    """限制网关转发面；Ollama CLI/状态接口保留在本机 /api 命名空间。"""
    clean = (path or "").split("?", 1)[0]
    if method in ("GET", "POST") and clean.startswith("/api/"):
        return True
    if method in ("GET", "POST") and clean.startswith(CLUSTER_PREFIX):
        return True
    if method in ("GET", "POST") and clean.startswith(INSTALLATION_PREFIX):
        return True
    if method == "GET" and clean.startswith("/v1/deployments/models/"):
        return True
    if is_vapi_management_path(clean):
        return method in ("GET", "POST")
    if method == "GET":
        return clean in ALLOWED_GET_PATHS
    if method == "POST":
        return clean in ALLOWED_POST_PATHS
    return False


def valid_installation_artifact(kind, value):
    """与 installer_agent._valid_artifact 同构：拒绝路径穿越、URL 与控制字符。"""
    if not isinstance(value, str) or not 1 <= len(value) <= 512:
        return False
    if any(ord(character) < 32 for character in value) or "\\" in value:
        return False
    if value.startswith("/") or "://" in value or any(
            part in (".", "..") for part in value.split("/")):
        return False
    if kind in ("gateway", "service"):
        return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value))
    return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@+/-]{0,511}", value))


def installation_rate_allowed(client_ip, now=None):
    """每来源 IP 在窗口内最多创建 INSTALLATION_RATE_LIMIT 个提案。"""
    now = time.time() if now is None else now
    with INSTALLATION_RATE_LOCK:
        cutoff = now - INSTALLATION_RATE_WINDOW
        for key, stamps in list(INSTALLATION_RATE_HITS.items()):
            recent = [stamp for stamp in stamps if stamp > cutoff]
            if recent:
                INSTALLATION_RATE_HITS[key] = recent
            else:
                INSTALLATION_RATE_HITS.pop(key, None)
        stamps = INSTALLATION_RATE_HITS.setdefault(client_ip, [])
        if len(stamps) >= INSTALLATION_RATE_LIMIT:
            return False
        stamps.append(now)
        return True


def token_is_authorized(client_ip, headers, token):
    """回环调用无需令牌；所有远端调用必须携带 Bearer 或 X-API-Key。"""
    if client_ip in ("127.0.0.1", "::1"):
        return True
    if not token:
        return False
    authorization = headers.get("Authorization", "") if headers else ""
    bearer = authorization[7:].strip() if authorization.lower().startswith("bearer ") else ""
    api_key = headers.get("X-API-Key", "") if headers else ""
    return hmac.compare_digest(bearer or api_key, token)


def request_is_authorized(client_ip, headers, token, cors_origins):
    """带 Origin 的浏览器请求先受 CORS 限制，不能利用回环免 Token 绕过。"""
    origin = headers.get("Origin", "") if headers else ""
    if origin and origin not in cors_origins:
        return False, 403
    if token_is_authorized(client_ip, headers, token):
        return True, 200
    return False, 401


def record_usage(stats_path, usage, kind, vapi_id=None):
    """把一次推理请求的 usage 追加为一行 JSON（按虚拟 API 归因）。"""
    if not usage:
        return
    row = {
        "ts": time.time(),
        "kind": kind,
        "prompt": int(usage.get("prompt_tokens", 0) or 0),
        "completion": int(usage.get("completion_tokens", 0) or 0),
        "total": int(usage.get("total_tokens", 0) or 0),
    }
    if vapi_id:
        row["vapi"] = str(vapi_id)
    with STATS_LOCK:
        try:
            with open(stats_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(row, ensure_ascii=False) + "\n")
        except OSError:
            pass


def status_timestamp(value=None):
    """Return a stable UTC timestamp for the status-page wire contract."""
    return datetime.fromtimestamp(value or time.time(), timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class StatusEventStore:
    """Bounded, process-local event history used by the status page."""

    def __init__(self, limit=200):
        self._events = deque(maxlen=max(20, int(limit)))
        self._lock = threading.RLock()

    def record(self, component, status, summary, details=None):
        event = {
            "id": uuid.uuid4().hex,
            "timestamp": status_timestamp(),
            "component": str(component),
            "status": str(status),
            "summary": str(summary),
        }
        if isinstance(details, dict) and details:
            event["details"] = {str(k): v for k, v in details.items()
                                 if isinstance(k, (str, int)) and isinstance(v, (str, int, float, bool, type(None)))}
        with self._lock:
            self._events.append(event)
        return event

    def list(self, limit=50):
        try:
            limit = max(1, min(200, int(limit)))
        except (TypeError, ValueError):
            limit = 50
        with self._lock:
            return list(self._events)[-limit:][::-1]


def usage_summary(stats_path, window_seconds=24 * 60 * 60, vapi_id=None):
    summary = {"requests": 0, "prompt_tokens": 0, "completion_tokens": 0,
               "total_tokens": 0, "window_seconds": window_seconds}
    if vapi_id:
        summary["vapi"] = str(vapi_id)
    if not stats_path:
        return summary
    cutoff = time.time() - window_seconds
    try:
        with open(stats_path, "r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if not isinstance(row, dict) or float(row.get("ts", 0) or 0) < cutoff:
                    continue
                if vapi_id and row.get("vapi") != str(vapi_id):
                    continue
                summary["requests"] += 1
                for key in ("prompt_tokens", "completion_tokens", "total_tokens"):
                    summary[key] += int(row.get({"prompt_tokens": "prompt", "completion_tokens": "completion", "total_tokens": "total"}[key], 0) or 0)
    except (OSError, ValueError, TypeError):
        pass
    return summary


def usage_summary_by_vapi(stats_path, window_seconds=24 * 60 * 60):
    """一次遍历按 vAPI 聚合 24h 用量，避免管理列表为每个 Agent 重复扫描 JSONL。"""
    result = {}
    if not stats_path:
        return result
    cutoff = time.time() - window_seconds
    try:
        with open(stats_path, "r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except (TypeError, ValueError):
                    continue
                if not isinstance(row, dict) or float(row.get("ts", 0) or 0) < cutoff:
                    continue
                name = str(row.get("vapi") or LEGACY_VAPI_ID)
                entry = result.setdefault(name, {
                    "requests": 0, "prompt_tokens": 0, "completion_tokens": 0,
                    "total_tokens": 0, "window_seconds": window_seconds})
                entry["requests"] += 1
                for target, field in (("prompt_tokens", "prompt"),
                                      ("completion_tokens", "completion"),
                                      ("total_tokens", "total")):
                    entry[target] += int(row.get(field, 0) or 0)
    except (OSError, ValueError, TypeError):
        pass
    return result


def validate_json_schema(value, schema, path="$", depth=0):
    """校验工具参数使用的 JSON Schema 安全子集；返回首个错误，None 表示通过。"""
    if depth > 32:
        return "{}: schema nesting is too deep".format(path)
    if not isinstance(schema, dict):
        return "{}: schema must be an object".format(path)
    if "const" in schema and value != schema["const"]:
        return "{}: value does not match const".format(path)
    if "enum" in schema and value not in schema["enum"]:
        return "{}: value is not in enum".format(path)
    for keyword in ("allOf", "anyOf", "oneOf"):
        variants = schema.get(keyword)
        if isinstance(variants, list):
            results = [validate_json_schema(value, item, path, depth + 1)
                       for item in variants]
            passed = sum(result is None for result in results)
            if keyword == "allOf" and passed != len(results):
                return next(result for result in results if result is not None)
            if keyword == "anyOf" and passed == 0:
                return "{}: value does not match anyOf".format(path)
            if keyword == "oneOf" and passed != 1:
                return "{}: value must match exactly one oneOf branch".format(path)
    expected = schema.get("type")
    expected_types = expected if isinstance(expected, list) else [expected] if expected else []
    type_checks = {
        "null": lambda item: item is None,
        "boolean": lambda item: isinstance(item, bool),
        "integer": lambda item: isinstance(item, int) and not isinstance(item, bool),
        "number": lambda item: isinstance(item, (int, float)) and not isinstance(item, bool),
        "string": lambda item: isinstance(item, str),
        "array": lambda item: isinstance(item, list),
        "object": lambda item: isinstance(item, dict),
    }
    if expected_types and not any(type_checks.get(kind, lambda _: False)(value)
                                  for kind in expected_types):
        return "{}: expected {}".format(path, " or ".join(expected_types))
    if isinstance(value, dict):
        properties = schema.get("properties") or {}
        required = schema.get("required") or []
        for name in required:
            if name not in value:
                return "{}: missing required property {}".format(path, name)
        additional = schema.get("additionalProperties", True)
        for name, item in value.items():
            child_schema = properties.get(name)
            if child_schema is None:
                if additional is False:
                    return "{}: additional property {} is not allowed".format(path, name)
                child_schema = additional if isinstance(additional, dict) else None
            if child_schema is not None:
                error = validate_json_schema(item, child_schema,
                                             "{}.{}".format(path, name), depth + 1)
                if error:
                    return error
        if isinstance(schema.get("minProperties"), int) and len(value) < schema["minProperties"]:
            return "{}: too few properties".format(path)
        if isinstance(schema.get("maxProperties"), int) and len(value) > schema["maxProperties"]:
            return "{}: too many properties".format(path)
    if isinstance(value, list):
        if isinstance(schema.get("minItems"), int) and len(value) < schema["minItems"]:
            return "{}: too few items".format(path)
        if isinstance(schema.get("maxItems"), int) and len(value) > schema["maxItems"]:
            return "{}: too many items".format(path)
        item_schema = schema.get("items")
        if isinstance(item_schema, dict):
            for index, item in enumerate(value):
                error = validate_json_schema(item, item_schema,
                                             "{}[{}]".format(path, index), depth + 1)
                if error:
                    return error
    if isinstance(value, str):
        if isinstance(schema.get("minLength"), int) and len(value) < schema["minLength"]:
            return "{}: string is too short".format(path)
        if isinstance(schema.get("maxLength"), int) and len(value) > schema["maxLength"]:
            return "{}: string is too long".format(path)
        pattern = schema.get("pattern")
        if isinstance(pattern, str):
            try:
                if re.search(pattern, value) is None:
                    return "{}: string does not match pattern".format(path)
            except re.error:
                return "{}: invalid schema pattern".format(path)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        if isinstance(schema.get("minimum"), (int, float)) and value < schema["minimum"]:
            return "{}: number is below minimum".format(path)
        if isinstance(schema.get("maximum"), (int, float)) and value > schema["maximum"]:
            return "{}: number is above maximum".format(path)
    return None


class SemanticSecurityGuard:
    """模型外的确定性安全层：来源标记、会话熔断、隔离与最小化审计。"""
    INJECTION_PATTERNS = tuple(re.compile(pattern, re.IGNORECASE) for pattern in (
        r"ignore\s+(?:all\s+)?(?:previous|prior|above)\s+instructions?",
        r"忽略(?:以上|之前|先前|所有).{0,12}(?:指令|提示|规则)",
        r"(?:reveal|print|show|泄露|输出).{0,20}(?:system prompt|系统提示|developer message)",
        r"(?:exfiltrat|窃取|外传|发送).{0,24}(?:secret|token|password|api.?key|密钥|令牌|密码)",
        r"(?:hidden|invisible|隐藏).{0,12}(?:instruction|prompt|指令|提示)",
    ))
    QUARANTINE_SECONDS = 15 * 60
    STRIKE_LIMIT = 3
    # 会话键由客户端提供，必须有界；否则多 Agent 长时间运行会无限累积内存。
    SESSION_TTL_SECONDS = 30 * 60
    MAX_TRACKED_SESSIONS = 4096

    def __init__(self, audit_path=None):
        self.audit_path = audit_path
        self.lock = threading.RLock()
        self.sessions = {}
        self.last_seen = {}

    def _prune_sessions(self, now):
        """淘汰过期会话并限制总量；调用方持有或可重入获取 self.lock。"""
        with self.lock:
            for key, seen in list(self.last_seen.items()):
                if now - seen > self.SESSION_TTL_SECONDS:
                    self.sessions.pop(key, None)
                    self.last_seen.pop(key, None)
            while len(self.last_seen) > self.MAX_TRACKED_SESSIONS:
                oldest = min(self.last_seen, key=self.last_seen.get)
                self.sessions.pop(oldest, None)
                self.last_seen.pop(oldest, None)

    @classmethod
    def _content_text(cls, content, depth=0):
        if depth > 8:
            return ""
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            return "\n".join(cls._content_text(item, depth + 1) for item in content)
        if isinstance(content, dict):
            return "\n".join(cls._content_text(content.get(key), depth + 1)
                             for key in ("text", "content", "output") if key in content)
        return ""

    @staticmethod
    def _trust(message):
        declared = message.get("aiguru_trust") if isinstance(message, dict) else None
        if declared in ("trusted", "user_authorized", "untrusted", "quarantined"):
            return declared
        role = message.get("role") if isinstance(message, dict) else None
        message_type = message.get("type") if isinstance(message, dict) else None
        content = message.get("content") if isinstance(message, dict) else None
        has_tool_result = isinstance(content, list) and any(
            isinstance(item, dict) and item.get("type") == "tool_result" for item in content)
        if role in ("tool", "function") or message_type == "function_call_output" \
                or has_tool_result:
            return "untrusted"
        if role == "system":
            return "trusted"
        return "user_authorized"

    def inspect(self, payload, session_id):
        now = time.time()
        session_id = session_id if re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", session_id or "") else None
        self._prune_sessions(now)
        with self.lock:
            if session_id:
                self.last_seen[session_id] = now
            state = self.sessions.get(session_id, {}) if session_id else {}
            if state.get("quarantined_until", 0) <= now:
                state.pop("quarantined_until", None)
            if state.get("quarantined_until", 0) > now:
                return {"blocked": True, "reason": "session_quarantined",
                        "session_id": session_id, "until": state["quarantined_until"]}
        messages = payload.get("messages") or payload.get("input") or []
        if isinstance(messages, str):
            messages = [{"role": "user", "content": messages}]
        findings = []
        for index, message in enumerate(messages if isinstance(messages, list) else []):
            if not isinstance(message, dict):
                continue
            trust = self._trust(message)
            text = self._content_text(message)
            matches = [pattern.pattern for pattern in self.INJECTION_PATTERNS
                       if pattern.search(text)]
            if matches:
                findings.append({"index": index, "trust": trust,
                                 "content_sha256": hashlib.sha256(text.encode()).hexdigest(),
                                 "signals": len(matches)})
        score = sum(item["signals"] * (2 if item["trust"] in ("untrusted", "quarantined") else 1)
                    for item in findings)
        blocked = bool(payload.get("tools")) and score >= 2
        if blocked and session_id:
            with self.lock:
                state = self.sessions.setdefault(session_id, {"strikes": 0})
                state["strikes"] = int(state.get("strikes", 0)) + 1
                if state["strikes"] >= self.STRIKE_LIMIT:
                    state["quarantined_until"] = now + self.QUARANTINE_SECONDS
        decision = {"blocked": blocked, "reason": "prompt_injection" if blocked else None,
                    "session_id": session_id, "score": score, "findings": findings}
        if findings:
            audit_decision = dict(decision)
            audit_decision.pop("session_id", None)
            if session_id:
                audit_decision["session_id_sha256"] = hashlib.sha256(
                    session_id.encode()).hexdigest()
            self.audit("security_signal", audit_decision)
        return decision

    def reset(self, session_id):
        if not re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", session_id or ""):
            return False
        with self.lock:
            self.last_seen.pop(session_id, None)
            return self.sessions.pop(session_id, None) is not None

    def status(self, vapi_id=None):
        """按虚拟 API 命名空间统计隔离状态；不传则汇总全部。"""
        now = time.time()
        self._prune_sessions(now)
        prefix = "{}:".format(vapi_id) if vapi_id else None
        with self.lock:
            active = {key: dict(value) for key, value in self.sessions.items()
                      if (not prefix or key.startswith(prefix))
                      and (value.get("strikes", 0) or value.get("quarantined_until", 0) > now)}
        result = {"enabled": True, "policy": "enforce", "active_sessions": len(active),
                  "quarantined_sessions": sum(value.get("quarantined_until", 0) > now
                                               for value in active.values())}
        if vapi_id:
            result["vapi"] = str(vapi_id)
        return result

    def audit(self, event, detail):
        if not self.audit_path:
            return
        row = {"ts": time.time(), "event": event, "detail": detail}
        encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        with SECURITY_AUDIT_LOCK:
            try:
                with open(self.audit_path, "a", encoding="utf-8") as handle:
                    handle.write(encoded)
            except OSError:
                pass


class VirtualApi:
    """一个 Agent 独占的虚拟 API 实例。

    凭据、模型别名、并发配额、安全会话与用量都按实例隔离。这样多个 Agent 共用
    同一个物理网关时，不会共享彼此的模型视图、隔离状态或排队槽位。
    """

    def __init__(self, api_id, key="", key_hash="", key_prefix="", label="", models=None,
                 max_concurrent=2, allow_default_model=True, allow_loopback_anonymous=False,
                 enabled=True, created_at=None, is_legacy=False):
        self.id = api_id
        # 明文 key 只在构造时短暂出现（签发/迁移），随后立刻转成摘要；磁盘上
        # 永远不保存可直接使用的高熵凭据，注册表泄露也不会让所有 Agent 失陷。
        self.key_hash = key_hash or (hash_credential(key) if key else "")
        self.key_prefix = key_prefix or ((key[:6]) if key else "")
        self.label = label or api_id
        self.models = {str(name): str(actual)
                       for name, actual in (models or {}).items()
                       if isinstance(name, str) and isinstance(actual, str)
                       and name and actual and len(name) <= 128 and len(actual) <= 128}
        try:
            limit = int(max_concurrent)
        except (TypeError, ValueError):
            limit = 2
        self.max_concurrent = max(1, min(64, limit))
        self.allow_default_model = bool(allow_default_model)
        self.allow_loopback_anonymous = bool(allow_loopback_anonymous)
        self.enabled = bool(enabled)
        self.is_legacy = bool(is_legacy)
        self.created_at = float(created_at or time.time())
        self.last_used_at = 0.0
        self.requests = 0
        self.prompt_tokens = 0
        self.completion_tokens = 0
        self.slot = threading.BoundedSemaphore(self.max_concurrent)
        self.lock = threading.RLock()

    @classmethod
    def from_config(cls, item):
        if not isinstance(item, dict) or not valid_vapi_id(item.get("id")):
            return None
        if item["id"] == LEGACY_VAPI_ID:
            return None
        return cls(
            api_id=item["id"], key=item.get("key") or "",
            key_hash=item.get("key_hash") or "",
            key_prefix=item.get("key_prefix") or "",
            label=item.get("label") or item["id"],
            models=item.get("models") if isinstance(item.get("models"), dict) else {},
            max_concurrent=item.get("max_concurrent", 2),
            allow_default_model=item.get("allow_default_model", True),
            allow_loopback_anonymous=item.get("allow_loopback_anonymous", False),
            enabled=item.get("enabled", True), created_at=item.get("created_at"))

    def matches_key(self, credential):
        return bool(self.key_hash and credential
                    and hmac.compare_digest(hash_credential(credential), self.key_hash))

    def actual_model(self, requested):
        """虚拟模型名 → 注册表中的真实模型 id；None 表示拒绝，避免静默错模型幻觉。"""
        if self.is_legacy:
            return requested
        if isinstance(requested, str) and requested in self.models:
            return self.models[requested]
        if self.allow_default_model:
            return requested
        return None

    def note_request(self):
        with self.lock:
            self.requests += 1
            self.last_used_at = time.time()

    def note_usage(self, usage):
        if not isinstance(usage, dict):
            return
        with self.lock:
            self.prompt_tokens += int(usage.get("prompt_tokens", 0) or 0)
            self.completion_tokens += int(usage.get("completion_tokens", 0) or 0)

    def public(self):
        with self.lock:
            return {
                "id": self.id, "label": self.label, "enabled": self.enabled,
                "is_legacy": self.is_legacy,
                "key_hint": (self.key_prefix + "...") if self.key_prefix else None,
                "base_path": VAPI_PREFIX + self.id,
                "models": dict(self.models),
                "max_concurrent": self.max_concurrent,
                "allow_default_model": self.allow_default_model,
                "allow_loopback_anonymous": self.allow_loopback_anonymous,
                "created_at": self.created_at, "last_used_at": self.last_used_at,
                "requests": self.requests,
                "prompt_tokens": self.prompt_tokens,
                "completion_tokens": self.completion_tokens,
            }

    def stored(self):
        with self.lock:
            return {"id": self.id, "key_hash": self.key_hash,
                    "key_prefix": self.key_prefix, "label": self.label,
                    "models": dict(self.models), "max_concurrent": self.max_concurrent,
                    "allow_default_model": self.allow_default_model,
                    "allow_loopback_anonymous": self.allow_loopback_anonymous,
                    "enabled": self.enabled, "created_at": self.created_at}


class VirtualApiRegistry:
    """虚拟 API 注册中心：身份解析、凭据校验、隔离路由与持久化。"""

    def __init__(self, path=None, strict=False):
        self.path = os.path.realpath(path) if path else None
        self.strict = bool(strict)
        self.lock = threading.RLock()
        self.legacy = VirtualApi(
            LEGACY_VAPI_ID, label="Legacy shared endpoint", max_concurrent=8,
            allow_default_model=True, allow_loopback_anonymous=True, is_legacy=True)
        self.vapis = {LEGACY_VAPI_ID: self.legacy}
        self.by_hash = {}
        if self.path:
            self._load()

    def _load(self):
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
        except FileNotFoundError:
            self._persist()
            return
        except (OSError, ValueError, TypeError):
            return
        items = payload.get("vapis", []) if isinstance(payload, dict) else []
        with self.lock:
            migrated = False
            for item in items if isinstance(items, list) else []:
                api = VirtualApi.from_config(item)
                if api is None:
                    continue
                self.vapis[api.id] = api
                if api.key_hash:
                    self.by_hash[api.key_hash] = api
                # 旧版注册表把明文 key 写进了文件；读取后立刻升级为摘要并回写。
                if isinstance(item, dict) and item.get("key"):
                    migrated = True
            if migrated:
                self._persist()

    def _persist(self):
        if not self.path:
            return False
        payload = {"version": 1,
                   "vapis": [api.stored() for api in self.vapis.values()
                             if not api.is_legacy]}
        directory = os.path.dirname(self.path)
        try:
            os.makedirs(directory, mode=0o700, exist_ok=True)
            atomic_json_write(self.path, payload)
            os.chmod(self.path, 0o600)
            return True
        except OSError:
            return False

    @staticmethod
    def credential_from(headers):
        authorization = headers.get("Authorization", "") if headers else ""
        if authorization.lower().startswith("bearer "):
            return authorization[7:].strip()
        return (headers.get("X-API-Key", "") if headers else "").strip()

    def _authorized(self, api, credential, client_ip, gateway_token):
        if api.matches_key(credential):
            return True
        if api.is_legacy and gateway_token and credential \
                and hmac.compare_digest(credential, gateway_token):
            return True
        if api.allow_loopback_anonymous and not self.strict \
                and client_ip in ("127.0.0.1", "::1"):
            return True
        return False

    def resolve(self, headers, raw_path, client_ip, gateway_token=""):
        """返回 (vapi, normalized_path, error)；error 为 (status, message, type)。"""
        credential = self.credential_from(headers)
        digest = hash_credential(credential) if credential else ""
        vapi_id, normalized = normalize_virtual_path(raw_path)
        with self.lock:
            matched_by_key = self.by_hash.get(digest) if digest else None
            api = None
            if vapi_id is not None:
                api = self.vapis.get(vapi_id)
                if api is None or not api.enabled:
                    return None, raw_path, (404, "Virtual API is not registered",
                                           "virtual_api_not_found")
                if not self._authorized(api, credential, client_ip, gateway_token):
                    return None, raw_path, (401, "Invalid virtual API credential",
                                           "authentication_error")
            elif matched_by_key is not None and matched_by_key.enabled:
                api = matched_by_key
            elif client_ip in ("127.0.0.1", "::1") and not self.strict:
                api = self.legacy
            elif gateway_token and credential \
                    and hmac.compare_digest(credential, gateway_token):
                api = self.legacy
            else:
                return None, raw_path, (401, "Virtual API credential is required",
                                       "authentication_error")
        return api, normalized, None

    def list(self):
        with self.lock:
            return [api.public() for api in self.vapis.values()]

    def get(self, api_id):
        with self.lock:
            api = self.vapis.get(api_id)
            return api.public() if api else None

    def create(self, payload):
        api_id = payload.get("id") if isinstance(payload, dict) else None
        if not valid_vapi_id(api_id) or api_id == LEGACY_VAPI_ID:
            raise ValueError("id must match [A-Za-z0-9][A-Za-z0-9_.-]{0,63} and not be 'legacy'")
        label = payload.get("label") or api_id
        if not isinstance(label, str) or not 1 <= len(label) <= 128 \
                or any(ord(ch) < 32 for ch in label):
            raise ValueError("label must be a 1..128 character string")
        models = validate_vapi_models(payload.get("models", {}))
        with self.lock:
            if api_id in self.vapis:
                raise ValueError("virtual API already exists")
            if len(self.vapis) > 256:
                raise ValueError("too many virtual APIs")
            key = secrets.token_urlsafe(32)
            api = VirtualApi(
                api_id, key=key, label=label, models=models,
                max_concurrent=payload.get("max_concurrent", 2),
                allow_default_model=payload.get("allow_default_model", True),
                allow_loopback_anonymous=payload.get("allow_loopback_anonymous", False))
            self.vapis[api_id] = api
            self.by_hash[api.key_hash] = api
            self._persist()
        return api, key

    def rotate(self, api_id):
        with self.lock:
            api = self.vapis.get(api_id)
            if api is None or api.is_legacy:
                return None
            self.by_hash.pop(api.key_hash, None)
            key = secrets.token_urlsafe(32)
            api.key_hash = hash_credential(key)
            api.key_prefix = key[:6]
            self.by_hash[api.key_hash] = api
            self._persist()
            return key

    def update(self, api_id, payload):
        """编辑展示名、模型别名与并发配额；凭据本身只能通过 rotate 更换。"""
        changes = {}
        if "label" in payload:
            label = payload["label"]
            if not isinstance(label, str) or not 1 <= len(label) <= 128 \
                    or any(ord(ch) < 32 for ch in label):
                raise ValueError("label must be a 1..128 character string")
            changes["label"] = label
        if "models" in payload:
            changes["models"] = validate_vapi_models(payload["models"])
        for flag in ("allow_default_model", "allow_loopback_anonymous"):
            if flag in payload:
                changes[flag] = bool(payload[flag])
        if "max_concurrent" in payload:
            try:
                limit = int(payload["max_concurrent"])
            except (TypeError, ValueError):
                raise ValueError("max_concurrent must be an integer in 1..64")
            if not 1 <= limit <= 64:
                raise ValueError("max_concurrent must be an integer in 1..64")
            changes["max_concurrent"] = limit
        with self.lock:
            api = self.vapis.get(api_id)
            if api is None or api.is_legacy:
                return None
            for name, value in changes.items():
                setattr(api, name, value)
            # 在途请求释放的是获取瞬间的 semaphore 对象（见 _release_vapi_slot），
            # 因此这里可以安全替换槽，不会把旧配额归还到新槽上造成计数错乱。
            if "max_concurrent" in changes:
                api.slot = threading.BoundedSemaphore(api.max_concurrent)
            self._persist()
            return api.public()

    def revoke(self, api_id):
        with self.lock:
            api = self.vapis.pop(api_id, None)
            if api is None or api.is_legacy:
                return False
            self.by_hash.pop(api.key_hash, None)
            self._persist()
            return True

    def set_enabled(self, api_id, enabled):
        with self.lock:
            api = self.vapis.get(api_id)
            if api is None or api.is_legacy:
                return False
            api.enabled = bool(enabled)
            self._persist()
            return True


class ApiGateway:
    """协议适配与模型自动适配逻辑（与 HTTP 层解耦，便于单测）。"""

    def __init__(self, model_name="local", model_id="local",
                 family="other", reasoning=False, engine="other", capabilities=None):
        self.model_name = model_name or "local"
        self.model_id = model_id or "local"
        self.family = (family or "other").lower()
        self.reasoning = bool(reasoning)
        self.engine = (engine or "other").lower()
        self.capabilities = list(capabilities or [])

    def is_reasoning_model(self):
        """本地模型是否为推理型（响应需带 reasoning_content）。"""
        if self.reasoning:
            return True
        s = (self.model_name + " " + self.model_id).lower()
        return ("r1" in s or "reasoner" in s or "think" in s or "glm-4.5" in s)

    def resolve_model(self, requested):
        """请求 model → 本地后端模型 id（任意别名一律落到已加载模型，避免 404）。"""
        return self.model_id

    def split_reasoning(self, text):
        """把推理模型输出拆为 (reasoning, content)；无标记则 content 为全文。"""
        if not text:
            return "", text
        best = None
        for marker in THINKING_MARKERS:
            idx = text.find(marker)
            if idx < 0:
                continue
            if best is None or idx < best[0]:
                best = (idx, marker)
        if best is None:
            return "", text
        idx, marker = best
        if marker == "<|begin_of_thought|>":
            # GLM-4.5：思维在标记内；正文在 <|end_of_thought|> 之后
            end = text.find("<|end_of_thought|>", idx)
            if end >= 0:
                return text[idx + len(marker):end], text[end + len("<|end_of_thought|>"):].strip()
            return "", text
        reasoning = text[:idx].strip()
        content = text[idx + len(marker):].strip()
        return reasoning, content

    # ---- OpenAI/DeepSeek 请求体处理 ----

    def adapt_chat_body(self, body):
        """改写 chat 请求并保留标准 OpenAI 工具定义（原生能力优先）。"""
        if not isinstance(body, dict):
            return body, False
        body = dict(body)
        body["model"] = self.resolve_model(body.get("model"))
        body["messages"] = self.normalize_message_history(body.get("messages"))
        # mlx_lm.server 未指定时默认只生成 512 tokens，长篇 Agent 回复会稳定截断。
        # 仅补充缺省值；客户端显式设置的上限仍按原值执行。
        explicit_limit = body.get("max_tokens") is not None or \
            body.get("max_completion_tokens") is not None
        if not explicit_limit:
            body["max_tokens"] = DEFAULT_MAX_OUTPUT_TOKENS
        elif body.get("max_tokens") is None and body.get("max_completion_tokens") is not None:
            # 部分本地后端尚不识别新版 OpenAI 字段，转为广泛支持的字段。
            body["max_tokens"] = body["max_completion_tokens"]
        # 两个字段同时存在会让后端收到互相冲突的预算；始终移除新版字段。
        body.pop("max_completion_tokens", None)
        if self.engine == "mlx":
            body.setdefault("temperature", 0.2)
            body.setdefault("top_p", 0.9)
            body.setdefault("repetition_penalty", 1.08)
            body.setdefault("repetition_context_size", 128)
        raw_tools = body.get("tools")
        # 兼容旧版 Chat Completions 的 functions/function_call。部分 Agent SDK 仍使用
        # 该结构；若直接丢弃，模型虽能从系统提示看到工具，却没有 renderer 元数据。
        legacy_functions = body.pop("functions", None)
        if not raw_tools and isinstance(legacy_functions, list):
            raw_tools = [{"type": "function", "function": fn}
                         for fn in legacy_functions if isinstance(fn, dict)]
        legacy_choice = body.pop("function_call", None)
        if body.get("tool_choice") is None and legacy_choice is not None:
            if legacy_choice in ("auto", "none"):
                body["tool_choice"] = legacy_choice
            elif isinstance(legacy_choice, dict) and isinstance(legacy_choice.get("name"), str):
                body["tool_choice"] = {"type": "function",
                                       "function": {"name": legacy_choice["name"]}}
        tools = self.normalize_tools(raw_tools)
        if tools:
            body["tools"] = tools
            choice = body.get("tool_choice")
            if isinstance(choice, dict) and choice.get("type") == "function" \
                    and isinstance(choice.get("name"), str):
                body["tool_choice"] = {
                    "type": "function", "function": {"name": choice["name"]}}
            # 工具模型常先输出思考/计划，再生成参数 JSON。过小预算会让 Ollama 的
            # renderer 在对象闭合前结束，并直接返回“can't find closing }”错误。
            current_limit = body.get("max_tokens")
            if not isinstance(current_limit, int) or isinstance(current_limit, bool) \
                    or current_limit < MIN_TOOL_OUTPUT_TOKENS:
                # 工具模型常先输出思考/计划再生成参数 JSON；客户端显式给的小预算
                # 同样会让 Ollama renderer 在对象闭合前截断，因此统一抬到安全下限。
                body["max_tokens"] = MIN_TOOL_OUTPUT_TOKENS
        else:
            body.pop("tools", None)
            body.pop("tool_choice", None)
        return body, self.is_reasoning_model()

    @staticmethod
    def normalize_message_history(messages):
        """规范历史 tool_calls，避免后端模板因 object 或截断 JSON arguments 返回 400。"""
        if not isinstance(messages, list):
            return []
        normalized = []
        for message in messages:
            if not isinstance(message, dict):
                continue
            clean = dict(message)
            content = clean.get("content")
            if isinstance(content, list):
                # OpenAI/MLX-Serve 风格的多模态消息：只接受内联 data URL，
                # 避免后端自行抓取任意远程地址形成 SSRF；限制单图大小防止内存耗尽。
                parts = []
                for part in content:
                    if not isinstance(part, dict):
                        continue
                    if part.get("type") == "image_url":
                        image = part.get("image_url")
                        url = image.get("url") if isinstance(image, dict) else None
                        if not isinstance(url, str) or not url.startswith("data:image/") \
                                or ";base64," not in url or len(url) > MAX_IMAGE_DATA_URL_BYTES:
                            continue
                    parts.append(part)
                clean["content"] = parts
            calls = clean.get("tool_calls")
            if isinstance(calls, list):
                valid_calls = []
                for call in calls:
                    if not isinstance(call, dict):
                        continue
                    item = dict(call)
                    fn = item.get("function")
                    if not isinstance(fn, dict) or not isinstance(fn.get("name"), str):
                        continue
                    fn = dict(fn)
                    arguments = fn.get("arguments", {})
                    if isinstance(arguments, str):
                        try:
                            parsed = json.loads(arguments)
                            if not isinstance(parsed, (dict, list)):
                                arguments = json.dumps({"value": parsed}, ensure_ascii=False,
                                                       separators=(",", ":"))
                        except (ValueError, TypeError):
                            # 截断参数无法可靠恢复；历史调用保留标识但使用合法空对象，
                            # 防止 Ollama/llama.cpp 的模板 JSON 解析器拒绝整轮请求。
                            arguments = "{}"
                    elif isinstance(arguments, (dict, list)):
                        arguments = json.dumps(arguments, ensure_ascii=False, separators=(",", ":"))
                    else:
                        arguments = "{}"
                    fn["arguments"] = arguments
                    item["function"] = fn
                    item.setdefault("type", "function")
                    valid_calls.append(item)
                if valid_calls:
                    clean["tool_calls"] = valid_calls
                else:
                    clean.pop("tool_calls", None)
            function_call = clean.get("function_call")
            if isinstance(function_call, dict):
                legacy = dict(function_call)
                arguments = legacy.get("arguments", {})
                if not isinstance(arguments, str):
                    arguments = json.dumps(arguments if isinstance(arguments, (dict, list)) else {},
                                           ensure_ascii=False, separators=(",", ":"))
                else:
                    try:
                        json.loads(arguments)
                    except (ValueError, TypeError):
                        arguments = "{}"
                legacy["arguments"] = arguments
                clean["function_call"] = legacy
            normalized.append(clean)
        return normalized

    @staticmethod
    def normalize_tools(tools):
        """同时接受 Chat 嵌套格式与 Responses 扁平格式，并输出 Chat 格式。"""
        result = []
        for tool in tools or []:
            if not isinstance(tool, dict):
                continue
            kind = tool.get("type", "function")
            fn = tool.get("function")
            if not isinstance(fn, dict) and isinstance(tool.get("name"), str):
                fn = tool
            if kind == "custom" and isinstance(tool.get("name"), str):
                fn = {"name": tool["name"], "description": tool.get("description", ""),
                      "parameters": {"type": "object", "properties": {
                          "input": {"type": "string", "description": "Custom tool input"}},
                          "required": ["input"]}}
            if kind not in ("function", "custom"):
                continue
            if not isinstance(fn, dict) or not isinstance(fn.get("name"), str) or not fn["name"]:
                continue
            clean = {"name": fn["name"]}
            if isinstance(fn.get("description"), str):
                clean["description"] = fn["description"]
            parameters = fn.get("parameters") or fn.get("input_schema")
            clean["parameters"] = parameters if isinstance(parameters, dict) else {"type": "object", "properties": {}}
            if isinstance(fn.get("strict"), bool):
                clean["strict"] = fn["strict"]
            result.append({"type": "function", "function": clean})
        return result

    def responses_to_chat(self, payload):
        """OpenAI Responses create 请求 → Chat Completions 请求。"""
        messages = []
        instructions = payload.get("instructions")
        if isinstance(instructions, str) and instructions:
            messages.append({"role": "developer", "content": instructions})
        input_ = payload.get("input", "")
        if isinstance(input_, str):
            messages.append({"role": "user", "content": input_})
        elif isinstance(input_, list):
            for item in input_:
                if not isinstance(item, dict):
                    continue
                kind = item.get("type")
                if kind == "function_call":
                    messages.append({
                        "role": "assistant", "content": None,
                        "tool_calls": [{
                            "id": item.get("call_id") or item.get("id") or "call_" + uuid.uuid4().hex[:24],
                            "type": "function",
                            "function": {"name": item.get("name", ""),
                                         "arguments": item.get("arguments", "{}")},
                        }],
                    })
                    continue
                if kind == "function_call_output":
                    output = item.get("output", "")
                    if not isinstance(output, str):
                        output = json.dumps(output, ensure_ascii=False)
                    messages.append({"role": "tool", "tool_call_id": item.get("call_id", ""),
                                     "content": output})
                    continue
                if kind not in (None, "message"):
                    continue
                role = item.get("role", "user")
                content = item.get("content", "")
                if isinstance(content, str):
                    messages.append({"role": role, "content": content})
                    continue
                parts = []
                for part in content if isinstance(content, list) else []:
                    if not isinstance(part, dict):
                        continue
                    ptype = part.get("type")
                    if ptype in ("input_text", "output_text", "text"):
                        parts.append({"type": "text", "text": part.get("text", "")})
                    elif ptype == "input_image" and part.get("image_url"):
                        parts.append({"type": "image_url",
                                      "image_url": {"url": part["image_url"]}})
                messages.append({"role": role, "content": parts})
        tools = []
        for tool in payload.get("tools") or []:
            if not isinstance(tool, dict) or tool.get("type") != "function" or not tool.get("name"):
                continue
            fn = {"name": tool["name"],
                  "parameters": tool.get("parameters") or {"type": "object", "properties": {}}}
            if isinstance(tool.get("description"), str):
                fn["description"] = tool["description"]
            if isinstance(tool.get("strict"), bool):
                fn["strict"] = tool["strict"]
            tools.append({"type": "function", "function": fn})
        body = {"model": self.resolve_model(payload.get("model")), "messages": messages,
                "stream": False}
        if tools:
            body["tools"] = tools
        choice = payload.get("tool_choice")
        if choice in ("auto", "none", "required"):
            body["tool_choice"] = choice
        elif isinstance(choice, dict) and choice.get("type") == "function" and choice.get("name"):
            body["tool_choice"] = {"type": "function", "function": {"name": choice["name"]}}
        max_tokens = payload.get("max_output_tokens")
        body["max_tokens"] = (max_tokens if isinstance(max_tokens, int) and max_tokens > 0
                              else DEFAULT_MAX_OUTPUT_TOKENS)
        for key in ("temperature", "top_p", "parallel_tool_calls"):
            if payload.get(key) is not None:
                body[key] = payload[key]
        text = payload.get("text")
        if isinstance(text, dict) and isinstance(text.get("format"), dict):
            fmt = text["format"]
            if fmt.get("type") == "json_object":
                body["response_format"] = {"type": "json_object"}
            elif fmt.get("type") == "json_schema":
                body["response_format"] = {"type": "json_schema", "json_schema": {
                    "name": fmt.get("name", "response"), "schema": fmt.get("schema", {}),
                    "strict": fmt.get("strict", False)}}
        return self.adapt_chat_body(body)[0]

    def chat_to_responses(self, payload, requested_model, request_payload=None):
        """Chat Completions 响应 → OpenAI Responses 响应；回填原始请求的控制字段。"""
        request = request_payload if isinstance(request_payload, dict) else {}
        reasoning_request = request.get("reasoning") \
            if isinstance(request.get("reasoning"), dict) else {}
        max_output = request.get("max_output_tokens")
        if max_output is None:
            max_output = request.get("max_tokens")
        choice = (payload.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        output = []
        reasoning = message.get("reasoning_content") or message.get("reasoning")
        if isinstance(reasoning, str) and reasoning:
            output.append({"id": "rs_" + uuid.uuid4().hex, "type": "reasoning",
                           "summary": [{"type": "summary_text", "text": reasoning}]})
        text = message.get("content")
        if isinstance(text, str) and text:
            output.append({"id": "msg_" + uuid.uuid4().hex, "type": "message",
                           "status": "completed", "role": "assistant",
                           "content": [{"type": "output_text", "text": text, "annotations": []}]})
        for call in message.get("tool_calls") or []:
            fn = call.get("function") or {}
            output.append({"id": "fc_" + uuid.uuid4().hex,
                           "call_id": call.get("id") or "call_" + uuid.uuid4().hex[:24],
                           "type": "function_call", "status": "completed",
                           "name": fn.get("name", ""), "arguments": fn.get("arguments", "{}")})
        usage = payload.get("usage") or {}
        input_tokens = int(usage.get("prompt_tokens", 0) or 0)
        output_tokens = int(usage.get("completion_tokens", 0) or 0)
        finish = choice.get("finish_reason")
        status = "incomplete" if finish == "length" else "completed"
        return {"id": "resp_" + uuid.uuid4().hex, "object": "response",
                "created_at": int(time.time()), "status": status, "error": None,
                "incomplete_details": {"reason": "max_output_tokens"} if status == "incomplete" else None,
                "instructions": request.get("instructions"),
                "max_output_tokens": max_output, "model": requested_model,
                "output": output,
                "parallel_tool_calls": request.get("parallel_tool_calls", True),
                "previous_response_id": None,
                "reasoning": {"effort": reasoning_request.get("effort"),
                              "summary": reasoning_request.get("summary")},
                "store": False, "temperature": request.get("temperature"),
                "text": {"format": {"type": "text"}},
                "tool_choice": request.get("tool_choice") or "auto",
                "tools": request.get("tools") or [], "top_p": request.get("top_p"),
                "truncation": request.get("truncation") or "disabled",
                "usage": {"input_tokens": input_tokens, "output_tokens": output_tokens,
                          "total_tokens": int(usage.get("total_tokens", input_tokens + output_tokens) or 0),
                          "input_tokens_details": {"cached_tokens": 0},
                          "output_tokens_details": {"reasoning_tokens": 0}}}

    def fallback_tool_body(self, body):
        """把工具请求降级为所有 chat 后端都能理解的严格 JSON 提示协议。"""
        body = dict(body)
        tools = self.normalize_tools(body.pop("tools", None))
        choice = body.pop("tool_choice", "auto")
        if not tools:
            return body
        if choice == "none":
            body["stream"] = False
            return body
        specs = [tool["function"] for tool in tools]
        instruction = (
            "You can call tools. Available tools (JSON): "
            + json.dumps(specs, ensure_ascii=False, separators=(",", ":"))
            + "\nIf a tool is needed, respond ONLY with JSON in this exact form: "
              '{"tool_calls":[{"name":"tool_name","arguments":{}}]}. '
              "Otherwise answer normally. Never invent a tool name."
        )
        if choice == "required" or isinstance(choice, dict):
            instruction += " A tool call is required for this turn."
        messages = list(body.get("messages") or [])
        messages.insert(0, {"role": "system", "content": instruction})
        body["messages"] = messages
        body["stream"] = False
        body["response_format"] = {
            "type": "json_schema",
            "json_schema": {
                "name": "tool_router",
                "strict": True,
                "schema": self.structured_tool_schema(
                    tools, self.tool_choice_requires_call(choice)),
            },
        }
        return body

    def structured_tool_schema(self, tools, requires_call=False):
        """按工具逐项生成参数 Schema，避免 name 与 arguments 错配。"""
        variants = []
        for tool in self.normalize_tools(tools):
            fn = tool["function"]
            parameters = fn.get("parameters")
            if not isinstance(parameters, dict):
                parameters = {"type": "object", "properties": {}}
            variants.append({
                "type": "object",
                "properties": {
                    "name": {"type": "string", "enum": [fn["name"]]},
                    "arguments": parameters,
                },
                "required": ["name", "arguments"],
                "additionalProperties": False,
            })
        item_schema = {"oneOf": variants} if len(variants) > 1 else \
            (variants[0] if variants else {"type": "object"})
        calls = {"type": "array", "items": item_schema}
        if requires_call:
            calls["minItems"] = 1
        return {
            "type": "object",
            "properties": {
                "tool_calls": calls,
                "content": {"type": "string"},
            },
            "required": ["tool_calls", "content"],
            "additionalProperties": False,
        }

    @staticmethod
    def _json_object_from_text(text):
        if not isinstance(text, str):
            return None
        candidate = text.strip()
        if candidate.startswith("```"):
            candidate = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate,
                               flags=re.IGNORECASE | re.DOTALL).strip()
        try:
            value = json.loads(candidate)
            return value if isinstance(value, dict) else None
        except ValueError:
            return None

    @staticmethod
    def _json_tool_calls_from_text(text):
        """读取常见 JSON 工具信封，但不从任意正文片段中猜测调用。"""
        if not isinstance(text, str):
            return []
        candidate = text.strip()
        if candidate.startswith("```"):
            candidate = re.sub(r"^```(?:json)?\s*|\s*```$", "", candidate,
                               flags=re.IGNORECASE | re.DOTALL).strip()
        marker = re.match(r"^(?:<\|tool_call\|>|\[TOOL_CALLS\])\s*", candidate,
                          flags=re.IGNORECASE)
        if marker:
            candidate = candidate[marker.end():].strip()

        def calls_from_value(value):
            if isinstance(value, list):
                return value
            if not isinstance(value, dict):
                return []
            if isinstance(value.get("tool_calls"), list):
                return value["tool_calls"]
            if isinstance(value.get("function_call"), dict):
                return [value["function_call"]]
            return [value] if isinstance(value.get("name"), str) else []

        # Qwen-family templates may emit one wrapper per call rather than one JSON array.
        wrappers = re.findall(r"<tool_call>\s*(.*?)\s*</tool_call>", candidate,
                              flags=re.IGNORECASE | re.DOTALL)
        if wrappers:
            residue = re.sub(r"<tool_call>\s*.*?\s*</tool_call>", "", candidate,
                             flags=re.IGNORECASE | re.DOTALL)
            if residue.strip():
                return []
            result = []
            try:
                for item in wrappers:
                    result.extend(calls_from_value(json.loads(item)))
            except (ValueError, TypeError):
                return []
            return result
        try:
            return calls_from_value(json.loads(candidate))
        except (ValueError, TypeError):
            return []

    @staticmethod
    def _coerce_xml_param(value, schema):
        """按工具 JSON Schema 恢复 XML 模板丢失的基本参数类型。"""
        expected = schema.get("type") if isinstance(schema, dict) else None
        text = value.strip()
        if expected in ("object", "array"):
            try:
                parsed = json.loads(text)
                if expected == "object" and isinstance(parsed, dict):
                    return parsed
                if expected == "array" and isinstance(parsed, list):
                    return parsed
            except (ValueError, TypeError):
                pass
            return value
        if expected == "integer" and re.fullmatch(r"[-+]?\d+", text):
            return int(text)
        if expected == "number":
            try:
                return float(text)
            except ValueError:
                return value
        if expected == "boolean" and text.lower() in ("true", "false"):
            return text.lower() == "true"
        if expected == "null" and text.lower() == "null":
            return None
        return value

    def _xml_tool_calls_from_text(self, text, tools):
        """解析 MiniCPM 风格 function/param XML，并绑定请求声明的 Schema。"""
        if not isinstance(text, str) or "<function" not in text.lower():
            return []
        candidate = text.strip()
        wrapper = re.fullmatch(r"<tool_call>\s*(.*?)\s*</tool_call>", candidate,
                               flags=re.IGNORECASE | re.DOTALL)
        if wrapper:
            candidate = wrapper.group(1).strip()
        if "<!DOCTYPE" in candidate.upper() or "<!ENTITY" in candidate.upper():
            return []
        specs = {tool["function"]["name"]: tool["function"]
                 for tool in self.normalize_tools(tools)}
        try:
            root = ET.fromstring("<tool_calls>" + candidate + "</tool_calls>")
        except ET.ParseError:
            return []
        if (root.text or "").strip():
            return []
        calls = []
        for element in list(root):
            if element.tag != "function" or set(element.attrib) != {"name"}:
                return []
            if (element.tail or "").strip():
                return []
            name = element.attrib.get("name")
            spec = specs.get(name)
            if not spec:
                return []
            properties = (spec.get("parameters") or {}).get("properties") or {}
            arguments = {}
            for param in list(element):
                if param.tag != "param" or set(param.attrib) != {"name"} or list(param):
                    return []
                param_name = param.attrib.get("name")
                if not param_name or param_name in arguments:
                    return []
                arguments[param_name] = self._coerce_xml_param(
                    param.text or "", properties.get(param_name, {}))
            calls.append({"name": name, "arguments": arguments})
        return calls

    def normalize_tool_response(self, payload, tools):
        """统一原生/回退工具响应为 OpenAI tool_calls，且只允许请求中声明的工具。"""
        if not isinstance(payload, dict) or not payload.get("choices"):
            return payload
        normalized_tools = self.normalize_tools(tools)
        specs = {t["function"]["name"]: t["function"] for t in normalized_tools}
        allowed = set(specs)
        if not allowed:
            return payload
        choice = dict(payload["choices"][0])
        message = dict(choice.get("message") or {})
        calls = message.get("tool_calls") or []
        if not calls and isinstance(message.get("function_call"), dict):
            calls = [{"type": "function", "function": message["function_call"]}]
        if not calls:
            calls = self._json_tool_calls_from_text(message.get("content"))
        if not calls:
            calls = self._xml_tool_calls_from_text(message.get("content"), tools)
        normalized = []
        for call in calls:
            if not isinstance(call, dict):
                continue
            fn = call.get("function") if isinstance(call.get("function"), dict) else call
            name = fn.get("name")
            if name not in allowed:
                continue
            arguments = fn.get("arguments", {})
            if not isinstance(arguments, str):
                parsed_arguments = arguments
            else:
                try:
                    parsed_arguments = json.loads(arguments)
                except ValueError:
                    # 不猜测或包装截断参数，否则客户端可能误执行一个语义错误的调用。
                    continue
                if not isinstance(parsed_arguments, dict):
                    continue
            schema = specs[name].get("parameters") or {"type": "object"}
            if validate_json_schema(parsed_arguments, schema) is not None:
                continue
            arguments = json.dumps(parsed_arguments, ensure_ascii=False, separators=(",", ":"))
            normalized.append({
                "id": call.get("id") or "call_" + uuid.uuid4().hex[:24],
                "type": "function",
                "function": {"name": name, "arguments": arguments},
            })
        if not normalized:
            # 原生调用即使畸形也绝不能原样泄漏给 Agent 的 JSON 参数解析器。
            if calls:
                message.pop("tool_calls", None)
                message.pop("function_call", None)
                choice["message"] = message
                choice["finish_reason"] = "stop"
                result = dict(payload)
                result["choices"] = [choice] + list(payload["choices"][1:])
                return result
            return payload
        message.pop("function_call", None)
        message["content"] = None
        message["tool_calls"] = normalized
        choice["message"] = message
        choice["finish_reason"] = "tool_calls"
        result = dict(payload)
        result["choices"] = [choice] + list(payload["choices"][1:])
        return result

    def has_malformed_tool_response(self, payload, tools):
        """判断模型是否尝试输出了不可安全执行的工具调用。"""
        if not isinstance(payload, dict) or not payload.get("choices"):
            return False
        message = payload["choices"][0].get("message") or {}
        calls = message.get("tool_calls") or []
        if not calls and isinstance(message.get("function_call"), dict):
            calls = [message["function_call"]]
        specs = {t["function"]["name"]: t["function"]
                 for t in self.normalize_tools(tools)}
        allowed = set(specs)
        for call in calls:
            if not isinstance(call, dict):
                return True
            fn = call.get("function") if isinstance(call.get("function"), dict) else call
            if fn.get("name") not in allowed:
                return True
            arguments = fn.get("arguments", {})
            if isinstance(arguments, dict):
                parsed_arguments = arguments
            elif not isinstance(arguments, str):
                return True
            else:
                try:
                    parsed_arguments = json.loads(arguments)
                except (ValueError, TypeError):
                    return True
                if not isinstance(parsed_arguments, dict):
                    return True
            if validate_json_schema(parsed_arguments,
                                    specs[fn.get("name")].get("parameters") or
                                    {"type": "object"}) is not None:
                return True
        return False

    @staticmethod
    def tool_choice_requires_call(choice):
        return choice == "required" or isinstance(choice, dict)

    @staticmethod
    def is_tool_compatibility_error(status, data):
        """只识别可安全降级的工具协议错误，不吞掉普通后端 500。"""
        if status in (400, 404, 405, 415, 422, 501):
            return True
        if status != 500:
            return False
        text = data.decode("utf-8", "replace").lower() if isinstance(data, bytes) else str(data).lower()
        markers = (
            "xml syntax error", "element <function", "closed by </parameter>",
            "tool calling", "tool_call", "tools are not supported",
            "does not support tools", "chat template", "function calling",
        )
        return any(marker in text for marker in markers)

    @staticmethod
    def is_incomplete_tool_json_error(status, data):
        """识别 Ollama/Qwen 在生成后解析截断工具参数时返回的错误。"""
        if status not in (400, 422, 500):
            return False
        text = data.decode("utf-8", "replace").lower() if isinstance(data, bytes) else str(data).lower()
        markers = (
            "value looks like object, but can't find closing '}' symbol",
            "value is array, but can't find closing ']' symbol",
            "value is string, but can't find closing",
            "invalid tool call: no matching closing",
            "tool call closing tag found but no content",
        )
        return any(marker in text for marker in markers)

    @staticmethod
    def expanded_tool_budget(body):
        """为一次截断重试提高输出预算，并降低再次产生不完整 JSON 的概率。"""
        retry = dict(body)
        current = retry.get("max_tokens")
        if not isinstance(current, int) or isinstance(current, bool):
            current = 0
        retry["max_tokens"] = min(
            max(current * 2, MIN_TOOL_OUTPUT_TOKENS), MAX_RETRY_OUTPUT_TOKENS)
        retry["temperature"] = min(float(retry.get("temperature", 0.2) or 0.2), 0.2)
        retry["stream"] = False
        return retry

    @staticmethod
    def _prompt_content(content):
        if isinstance(content, str):
            return content
        if content is None:
            return ""
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(item["text"])
                else:
                    parts.append(json.dumps(item, ensure_ascii=False, separators=(",", ":")))
            return "\n".join(parts)
        return json.dumps(content, ensure_ascii=False, separators=(",", ":"))

    def ollama_generate_body(self, chat_body, tools, requires_call=False):
        """构造带 JSON Schema 的 /api/generate 请求，绕开 Ollama 工具解析器。"""
        turns = []
        for message in chat_body.get("messages") or []:
            if not isinstance(message, dict):
                continue
            role = str(message.get("role") or "user").upper()
            content = self._prompt_content(message.get("content"))
            if message.get("tool_calls"):
                content += ("\n" if content else "") + json.dumps(
                    {"tool_calls": message["tool_calls"]},
                    ensure_ascii=False, separators=(",", ":"))
            turns.append("{}:\n{}".format(role, content))
        turns.append("ASSISTANT:\n")
        maximum = chat_body.get("max_tokens", DEFAULT_MAX_OUTPUT_TOKENS)
        if not isinstance(maximum, int) or isinstance(maximum, bool):
            maximum = DEFAULT_MAX_OUTPUT_TOKENS
        options = {
            "num_predict": min(
                max(maximum, MIN_TOOL_OUTPUT_TOKENS), MAX_RETRY_OUTPUT_TOKENS),
            "temperature": min(float(chat_body.get("temperature", 0.2) or 0.2), 0.2),
        }
        if isinstance(chat_body.get("top_p"), (int, float)):
            options["top_p"] = chat_body["top_p"]
        return {
            "model": self.model_id,
            "prompt": "\n\n".join(turns),
            "format": self.structured_tool_schema(tools, requires_call),
            "stream": False,
            "raw": False,
            "options": options,
        }

    def ollama_generate_to_openai(self, payload):
        if not isinstance(payload, dict):
            return payload
        prompt_tokens = int(payload.get("prompt_eval_count", 0) or 0)
        completion_tokens = int(payload.get("eval_count", 0) or 0)
        return {
            "id": "chatcmpl_" + uuid.uuid4().hex,
            "object": "chat.completion",
            "created": int(time.time()),
            "model": self.model_id,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": payload.get("response", "")},
                "finish_reason": "length" if payload.get("done_reason") == "length" else "stop",
            }],
            "usage": {"prompt_tokens": prompt_tokens,
                      "completion_tokens": completion_tokens,
                      "total_tokens": prompt_tokens + completion_tokens},
        }

    def ollama_chat_to_openai_body(self, payload):
        """Ollama 原生 /api/chat 请求 → 统一 Chat Completions 请求。"""
        body = {
            "model": payload.get("model"),
            "messages": payload.get("messages") or [],
            "stream": False,
        }
        if payload.get("tools"):
            body["tools"] = payload["tools"]
        options = payload.get("options") if isinstance(payload.get("options"), dict) else {}
        if isinstance(options.get("num_predict"), int) and options["num_predict"] > 0:
            body["max_tokens"] = options["num_predict"]
        for key in ("temperature", "top_p", "frequency_penalty", "presence_penalty", "stop"):
            if options.get(key) is not None:
                body[key] = options[key]
        return self.adapt_chat_body(body)[0]

    def openai_to_ollama_chat(self, payload):
        """统一响应 → /api/chat；arguments 使用兼容客户端可增量累积的 JSON 字符串。"""
        choice = (payload.get("choices") or [{}])[0]
        source = choice.get("message") or {}
        message = {"role": "assistant", "content": source.get("content") or ""}
        thinking = source.get("reasoning_content") or source.get("reasoning")
        if isinstance(thinking, str) and thinking:
            message["thinking"] = thinking
        calls = []
        for call in source.get("tool_calls") or []:
            fn = call.get("function") or {}
            try:
                parsed_arguments = json.loads(fn.get("arguments") or "{}")
            except (ValueError, TypeError):
                continue
            if not isinstance(parsed_arguments, dict):
                continue
            # Aiguru 的 Ollama NDJSON 客户端与 OpenAI 流客户端共用字符串累加器；
            # 若返回 Ollama SDK 常见的 object，这一字段会被忽略并最终变成空参数。
            arguments = json.dumps(parsed_arguments, ensure_ascii=False, separators=(",", ":"))
            calls.append({"function": {"name": fn.get("name", ""),
                                        "arguments": arguments}})
        if calls:
            message["tool_calls"] = calls
        usage = payload.get("usage") or {}
        return {
            "model": self.model_id,
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "message": message,
            "done": True,
            "done_reason": "length" if choice.get("finish_reason") == "length" else "stop",
            "prompt_eval_count": int(usage.get("prompt_tokens", 0) or 0),
            "eval_count": int(usage.get("completion_tokens", 0) or 0),
        }

    def adapt_other_body(self, body):
        if isinstance(body, dict) and "model" in body:
            body = dict(body)
            body["model"] = self.resolve_model(body.get("model"))
        return body

    # ---- Anthropic → OpenAI 转换 ----

    def anthropic_to_openai(self, payload):
        """Anthropic Messages 请求 → OpenAI chat 请求（转发给后端）。"""
        messages = []
        system = payload.get("system")
        if system:
            if isinstance(system, str):
                messages.append({"role": "system", "content": system})
            elif isinstance(system, list):
                parts = [b.get("text", "") for b in system
                         if isinstance(b, dict) and b.get("type") == "text"]
                messages.append({"role": "system", "content": "".join(parts)})

        for msg in payload.get("messages", []):
            role = msg.get("role")
            content = msg.get("content")
            if isinstance(content, str):
                messages.append({"role": role, "content": content})
                continue
            if not isinstance(content, list):
                continue
            if role == "assistant":
                text_parts = []
                tool_calls = []
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    btype = block.get("type")
                    if btype == "text":
                        text_parts.append(block.get("text", ""))
                    elif btype == "tool_use":
                        tool_calls.append({
                            "id": block.get("id", "toolu_" + uuid.uuid4().hex[:8]),
                            "type": "function",
                            "function": {
                                "name": block.get("name", ""),
                                "arguments": json.dumps(block.get("input", {}), ensure_ascii=False),
                            },
                        })
                entry = {"role": "assistant",
                         "content": "".join(text_parts) if text_parts else None}
                if tool_calls:
                    entry["tool_calls"] = tool_calls
                messages.append(entry)
            elif role == "user":
                text_parts = []
                tool_results = []
                images = []
                for block in content:
                    if not isinstance(block, dict):
                        continue
                    btype = block.get("type")
                    if btype == "text":
                        text_parts.append(block.get("text", ""))
                    elif btype == "image":
                        src = block.get("source", {})
                        if src.get("type") == "base64" and src.get("data"):
                            images.append({
                                "type": "image_url",
                                "image_url": {
                                    "url": "data:{0};base64,{1}".format(
                                        src.get("media_type", "image/png"), src["data"]),
                                },
                            })
                    elif btype == "tool_result":
                        raw = block.get("content", "")
                        if isinstance(raw, list):
                            raw = "".join(b.get("text", "") for b in raw
                                          if isinstance(b, dict) and b.get("type") == "text")
                        elif isinstance(raw, (dict, list)):
                            raw = json.dumps(raw, ensure_ascii=False)
                        tool_results.append({
                            "role": "tool",
                            "content": str(raw),
                            "tool_call_id": block.get("tool_use_id", ""),
                        })
                if text_parts or images:
                    # OpenAI content 数组要求文本块为 {"type":"text","text":...}；
                    # 裸字符串混入会让多文本/图文请求丢文本或被后端拒绝。
                    blocks = [{"type": "text", "text": part} for part in text_parts] + images
                    if len(blocks) == 1 and blocks[0].get("type") == "text":
                        content = blocks[0]["text"]
                    else:
                        content = blocks
                    messages.append({"role": "user", "content": content})
                messages.extend(tool_results)

        openai_body = {
            "model": self.resolve_model(payload.get("model")),
            "messages": messages,
            "stream": bool(payload.get("stream", False)),
        }
        if payload.get("max_tokens") is not None:
            openai_body["max_tokens"] = payload["max_tokens"]
        else:
            openai_body["max_tokens"] = DEFAULT_MAX_OUTPUT_TOKENS
        if payload.get("temperature") is not None:
            openai_body["temperature"] = payload["temperature"]
        if payload.get("top_p") is not None:
            openai_body["top_p"] = payload["top_p"]
        stop = payload.get("stop_sequences")
        if stop:
            openai_body["stop"] = stop if len(stop) > 1 else stop[0]
        tools = []
        for tool in payload.get("tools") or []:
            if not isinstance(tool, dict) or not tool.get("name"):
                continue
            tools.append({"type": "function", "function": {
                "name": tool["name"],
                "description": tool.get("description", ""),
                "parameters": tool.get("input_schema") or {"type": "object", "properties": {}},
            }})
        if tools:
            openai_body["tools"] = tools
            choice = payload.get("tool_choice")
            if isinstance(choice, dict):
                ctype = choice.get("type")
                if ctype == "any":
                    openai_body["tool_choice"] = "required"
                elif ctype == "tool" and choice.get("name"):
                    openai_body["tool_choice"] = {"type": "function", "function": {"name": choice["name"]}}
                elif ctype == "none":
                    openai_body["tool_choice"] = "none"
        return openai_body

    # ---- OpenAI → Anthropic 转换 ----

    def openai_to_anthropic(self, payload, requested_model, request_id):
        """OpenAI chat 响应 → Anthropic Messages 响应。"""
        choice = (payload.get("choices") or [{}])[0]
        message = choice.get("message", {})
        content_blocks = []
        reasoning = message.get("reasoning_content") or message.get("reasoning")
        if isinstance(reasoning, str) and reasoning:
            content_blocks.append({"type": "thinking", "thinking": reasoning, "signature": ""})
        text = message.get("content")
        if text:
            content_blocks.append({"type": "text", "text": text})
        for call in message.get("tool_calls") or []:
            fn = call.get("function", {})
            try:
                input_ = json.loads(fn.get("arguments") or "{}")
            except ValueError:
                input_ = {}
            content_blocks.append({
                "type": "tool_use",
                "id": call.get("id", "toolu_" + uuid.uuid4().hex[:8]),
                "name": fn.get("name", ""),
                "input": input_,
            })
        finish = choice.get("finish_reason")
        stop_map = {
            "stop": "end_turn",
            "tool_calls": "tool_use",
            "length": "max_tokens",
        }
        usage = payload.get("usage") or {}
        return {
            "id": "msg_" + uuid.uuid4().hex,
            "type": "message",
            "role": "assistant",
            "model": requested_model,
            "content": content_blocks,
            "stop_reason": stop_map.get(finish, "end_turn"),
            "stop_sequence": None,
            "usage": {
                "input_tokens": int(usage.get("prompt_tokens", 0) or 0),
                "output_tokens": int(usage.get("completion_tokens", 0) or 0),
                "cache_creation_input_tokens": 0,
                "cache_read_input_tokens": int(
                    (usage.get("prompt_tokens_details") or {}).get("cached_tokens", 0) or 0),
            },
        }

    # ---- 推理拆分（DeepSeek 风格） ----

    def openai_with_reasoning(self, payload):
        """在 OpenAI 响应上附加 reasoning_content（推理模型适配）。"""
        choice = (payload.get("choices") or [{}])[0]
        message = choice.get("message") or {}
        native_reasoning = message.get("reasoning") or message.get("reasoning_content")
        if not message.get("content"):
            if isinstance(native_reasoning, str) and native_reasoning.strip():
                message = dict(message)
                message["reasoning_content"] = native_reasoning
                message["content"] = native_reasoning
                payload = dict(payload)
                payload["choices"] = [dict(choice, message=message)] \
                    + list(payload.get("choices", [])[1:])
            return payload
        reasoning, content = self.split_reasoning(message["content"])
        if reasoning == "" and content == message["content"]:
            # 无思考段标记：reasoning_content 置空，兼容 deepseek-reasoner 字段
            message = dict(message)
            message["reasoning_content"] = ""
            payload = dict(payload)
            payload["choices"] = [dict(choice, message=message)]
            return payload
        message = dict(message)
        message["reasoning_content"] = reasoning
        # 部分本地推理模型在达到 token 上限时只返回思考段。不能把原本非空的
        # 输出转换为空，否则 Agent 会把成功响应误认为无回复。
        message["content"] = content if content else reasoning
        payload = dict(payload)
        payload["choices"] = [dict(choice, message=message)]
        return payload


def atomic_json_write(path, value):
    temporary = path + ".tmp-" + uuid.uuid4().hex
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class ClusterControlPlane:
    """持久化的局域网控制面：节点目录、能力心跳和带 TTL 的服务租约。"""

    def __init__(self, directory):
        self.directory = directory
        self.path = os.path.join(directory, "state.json")
        self.lock = threading.RLock()
        os.makedirs(directory, mode=0o700, exist_ok=True)
        os.chmod(directory, 0o700)
        self.state = {"nodes": {}, "locks": {}}
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                value = json.load(handle)
            if isinstance(value, dict):
                self.state.update(value)
        except (OSError, ValueError):
            pass

    @staticmethod
    def _valid_node_id(value):
        return isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value)

    @staticmethod
    def _valid_base_url(value):
        try:
            parsed = urlsplit(value or "")
            port = parsed.port
        except ValueError:
            return False
        if parsed.scheme not in ("http", "https") or not parsed.hostname or \
                parsed.username or parsed.password or parsed.path or parsed.query or parsed.fragment or \
                (port is not None and not 1 <= port <= 65535):
            return False
        try:
            address = ipaddress.ip_address(parsed.hostname)
        except ValueError:
            return False
        # link-local 段包含云元数据服务（169.254.169.254 / fe80::），一旦被注册为
        # 节点地址，探针与推理转发都会把请求打到本机元数据接口上。
        if address.is_link_local:
            return False
        return address.is_private or address.is_loopback

    def _save(self):
        atomic_json_write(self.path, self.state)

    def register(self, payload):
        node_id = payload.get("node_id") if isinstance(payload, dict) else None
        base_url = payload.get("base_url") if isinstance(payload, dict) else None
        models = payload.get("models") if isinstance(payload, dict) else None
        if not self._valid_node_id(node_id) or not self._valid_base_url(base_url):
            raise ValueError("node_id or private base_url is invalid")
        if not isinstance(models, list) or len(models) > 256:
            raise ValueError("models must be an array with at most 256 entries")
        normalized = []
        for model in models:
            if not isinstance(model, dict) or not isinstance(model.get("id"), str):
                raise ValueError("each model needs an id")
            normalized.append({
                "id": model["id"][:512], "name": str(model.get("name") or model["id"])[:512],
                "family": str(model.get("family") or "other")[:64],
                "engine": str(model.get("engine") or "other")[:32],
                "reasoning": bool(model.get("reasoning")),
                "tools": bool(model.get("tools", True)),
                "max_context": max(0, int(model.get("max_context", 0) or 0)),
            })
        now = time.time()
        with self.lock:
            previous = self.state["nodes"].get(node_id, {})
            node = {"node_id": node_id, "base_url": base_url.rstrip("/"), "models": normalized,
                    "capacity": max(1, min(1024, int(payload.get("capacity", 1) or 1))),
                    "active_requests": max(0, int(payload.get("active_requests", 0) or 0)),
                    "locked": bool(payload.get("locked", False)), "last_seen": now,
                    "created_at": previous.get("created_at", now)}
            self.state["nodes"][node_id] = node
            self._save()
            return dict(node)

    def heartbeat(self, node_id, payload):
        with self.lock:
            node = self.state["nodes"].get(node_id)
            if node is None:
                raise KeyError(node_id)
            node["last_seen"] = time.time()
            if isinstance(payload, dict):
                node["active_requests"] = max(0, int(payload.get("active_requests", node["active_requests"])))
                node["locked"] = bool(payload.get("locked", node["locked"]))
            self._save()
            return dict(node)

    def nodes(self):
        now = time.time()
        with self.lock:
            result = []
            for node in self.state["nodes"].values():
                item = dict(node)
                item["online"] = now - float(item.get("last_seen", 0)) <= 45
                result.append(item)
            return result

    def routes(self, requested_model, requirements=None):
        requirements = requirements or {}
        if requested_model in ("auto", "cluster", "cluster-large", "large"):
            requested_model = None
        minimum_context = max(0, int(requirements.get("min_context", 0) or 0))
        output_budget = max(0, int(requirements.get("max_output_tokens", 0) or 0))
        required_context = max(minimum_context, output_budget)
        needs_tools = bool(requirements.get("tools"))
        candidates = []
        for node in self.nodes():
            if not node["online"] or node.get("locked"):
                continue
            for model in node.get("models", []):
                if requested_model and requested_model not in (model["id"], model["name"]):
                    continue
                if needs_tools and not model.get("tools"):
                    continue
                if required_context and model.get("max_context", 0) < required_context:
                    continue
                load = node.get("active_requests", 0) / max(1, node.get("capacity", 1))
                candidates.append((load, -model.get("max_context", 0), node, model))
        candidates.sort(key=lambda item: (item[0], item[1], item[2]["node_id"]))
        return candidates

    def lease(self, name, owner, ttl, action, lease_id=None):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}", name or "") or \
                not self._valid_node_id(owner):
            raise ValueError("lock name or owner is invalid")
        ttl = max(5, min(300, int(ttl or 30)))
        now = time.time()
        with self.lock:
            current = self.state["locks"].get(name)
            if current and current.get("expires_at", 0) <= now:
                current = None
                self.state["locks"].pop(name, None)
            if action == "acquire":
                if current and current.get("owner") != owner:
                    return None
                current = {"name": name, "owner": owner, "lease_id": uuid.uuid4().hex,
                           "expires_at": now + ttl}
                self.state["locks"][name] = current
            elif action == "renew":
                if not current or current.get("owner") != owner or current.get("lease_id") != lease_id:
                    return None
                current["expires_at"] = now + ttl
            elif action == "release":
                if not current or current.get("owner") != owner or current.get("lease_id") != lease_id:
                    return None
                self.state["locks"].pop(name, None)
                self._save()
                return {"released": True, "name": name}
            self._save()
            return dict(current)


class ModelRouteRegistry:
    """线程安全的只读模型路由表；一个公网关可连接多个内部推理服务。"""

    def __init__(self, routes, cluster=None):
        self.routes = list(routes)
        self.cluster = cluster

    @classmethod
    def load(cls, path, default_gateway, default_port):
        routes = [{"gateway": default_gateway, "target_port": default_port,
                   "aliases": {default_gateway.model_id, default_gateway.model_name, "local"}}]
        if path:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            for item in payload.get("models", []) if isinstance(payload, dict) else []:
                if not isinstance(item, dict):
                    continue
                model_id = item.get("id")
                target_port = item.get("target_port")
                if not isinstance(model_id, str) or not model_id or \
                        not isinstance(target_port, int) or not 1 <= target_port <= 65535:
                    continue
                gateway = ApiGateway(
                    model_name=item.get("name") or model_id,
                    model_id=model_id,
                    family=item.get("family") or "other",
                    reasoning=bool(item.get("reasoning")),
                    engine=item.get("engine") or "other",
                    capabilities=[value for value in item.get("capabilities", [])
                                  if isinstance(value, str)])
                aliases = {model_id, gateway.model_name}
                aliases.update(alias for alias in item.get("aliases", [])
                               if isinstance(alias, str) and alias)
                routes.append({"gateway": gateway, "target_port": target_port,
                               "aliases": aliases})
        # 后登记的同名模型覆盖默认项，便于运行时生成的注册表替换启动参数。
        deduplicated = []
        seen = set()
        for route in reversed(routes):
            key = route["gateway"].model_id
            if key not in seen:
                seen.add(key)
                deduplicated.append(route)
        return cls(list(reversed(deduplicated)))

    def select(self, requested_model, requirements=None):
        if self.cluster:
            candidates = self.cluster.routes(requested_model, requirements)
            if candidates:
                _, _, node, model = candidates[0]
                gateway = ApiGateway(model_name=model["name"], model_id=model["id"],
                                     family=model["family"], reasoning=model["reasoning"],
                                     engine=model["engine"], capabilities=model.get("capabilities", []))
                return {"gateway": gateway, "base_url": node["base_url"],
                        "aliases": {model["id"], model["name"]}, "node_id": node["node_id"]}
        if isinstance(requested_model, str) and requested_model:
            for route in self.routes:
                if requested_model in route["aliases"]:
                    return route
        return self.routes[0] if len(self.routes) == 1 else None

    def models(self):
        gateways = [route["gateway"] for route in self.routes]
        if self.cluster:
            for node in self.cluster.nodes():
                if node["online"]:
                    gateways.extend(ApiGateway(model_name=model["name"], model_id=model["id"],
                                               family=model["family"], reasoning=model["reasoning"],
                                               engine=model["engine"], capabilities=model.get("capabilities", []))
                                    for model in node.get("models", []))
        return gateways


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def setup(self):
        super().setup()
        registry = getattr(self.server, "model_registry", None)
        if registry is None:
            registry = ModelRouteRegistry([{
                "gateway": self.server.gateway,
                "target_port": self.server.target_port,
                "aliases": {self.server.gateway.model_id,
                            self.server.gateway.model_name, "local"},
            }])
            self.server.model_registry = registry
        route = registry.routes[0]
        self.gateway = route["gateway"]
        self.target_port = route["target_port"]
        self.backend_base_url = route.get("base_url")
        # 防止只建立连接却不发送完整 headers/body 的慢连接长期占用并发槽。
        self.connection.settimeout(30)
        self.vapi = None
        self._vapi_slot_acquired = False
        self.actual_model = None
        self._response_headers = {}
        self._model_rejection_message = None
        self.original_path = ""

    def handle_one_request(self):
        """keep-alive 连接上每个请求结束都必须归还该 Agent 的并发配额。"""
        try:
            super().handle_one_request()
        finally:
            self._release_vapi_slot()

    def _release_vapi_slot(self):
        if getattr(self, "_vapi_slot_acquired", False):
            self._vapi_slot_acquired = False
            try:
                # 绑定获取瞬间的槽对象：编辑并发配额时旧请求仍归还到旧槽。
                self._vapi_slot.release()
            except (ValueError, AttributeError):
                pass

    def _begin_request(self):
        """解析虚拟 API 身份、归一化路径，并申请该 Agent 独占的并发配额。"""
        self.vapi = None
        self._vapi_slot_acquired = False
        self._vapi_slot = None
        self.actual_model = None
        self._response_headers = {}
        self._model_rejection_message = None
        self.original_path = self.path
        # 管理面由 control token 单独鉴权，不属于任何 Agent 的推理命名空间，
        # 也不该占用推理槽；否则 strict 模式下带控制凭据的管理请求会被
        # resolve() 当成匿名推理调用而返回 401，凭证管理界面随之失效。
        if is_vapi_management_path(self.original_path.split("?", 1)[0]):
            return True
        registry = getattr(self.server, "vapi_registry", None)
        if registry is None:
            return True
        api, normalized, error = registry.resolve(
            self.headers, self.path, self.client_address[0],
            getattr(self.server, "api_token", ""))
        if error:
            status, message, kind = error
            self._json_error(status, message, kind)
            return False
        self.path = normalized
        self.vapi = api
        self._response_headers["X-Aiguru-Virtual-API"] = api.id
        clean = normalized.split("?", 1)[0]
        # /api/ 管理面可读写本机文件、拉取/删除模型。虚拟身份一律禁用；legacy 共享
        # 端点仅允许回环访问，且强隔离或已配置控制凭据时必须出示 control token，
        # 否则攻击者可以绕开虚拟化边界直接操作推理引擎。
        if clean.startswith("/api/") and clean not in VAPI_SAFE_API_PATHS:
            if not api.is_legacy:
                self._json_error(403, "Virtual APIs cannot use engine management endpoints",
                                 "forbidden")
                return False
            if self.client_address[0] not in ("127.0.0.1", "::1"):
                self._json_error(403, "Engine management endpoints are local-only",
                                 "forbidden")
                return False
            # control token 现在始终存在（凭证管理面依赖它），因此这里只按用户
            # 主动开启的强隔离模式收紧；普通本机模式下 Ollama CLI 的 /api/pull
            # 仍可经回环使用，且始终禁止虚拟身份与远端来源。
            if registry.strict and not self._authorize_deployment():
                return False
        if not api.slot.acquire(blocking=False):
            self._json_error(429, "Virtual API concurrency quota exceeded", "rate_limit_error")
            return False
        self._vapi_slot_acquired = True
        self._vapi_slot = api.slot
        api.note_request()
        return True

    def _vapi_id(self):
        api = getattr(self, "vapi", None)
        return api.id if api else LEGACY_VAPI_ID

    def _record_usage(self, usage, kind):
        record_usage(getattr(self.server, "stats_path", ""), usage, kind, self._vapi_id())
        if getattr(self, "vapi", None) and not self.vapi.is_legacy:
            self.vapi.note_usage(usage)

    @staticmethod
    def _scoped_session(vapi_id, raw):
        """会话键按虚拟 API 分区，A 的注入违规绝不隔离 B。"""
        clean = raw if re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", raw or "") else "default"
        return "{}:{}".format(vapi_id, clean)

    def _model_error(self, anthropic=False):
        message = self._model_rejection_message or "Requested model is not registered"
        if anthropic:
            self._anthropic_error(404, message, "not_found_error")
        else:
            self._json_error(404, message, "model_not_found")

    def _visible_models(self):
        """返回 [(展示名, gateway)]；虚拟 API 只能看到自己命名空间里的模型。"""
        registry = self.server.model_registry
        api = getattr(self, "vapi", None)
        if api is None or api.is_legacy or not api.models:
            return [(item.model_id, item) for item in registry.models()]
        by_id = {route["gateway"].model_id: route["gateway"]
                 for route in registry.routes}
        return [(name, by_id[actual]) for name, actual in api.models.items()
                if actual in by_id]

    def _select_gateway(self, requested_model, requirements=None):
        requested = requested_model
        api = getattr(self, "vapi", None)
        if api is not None and not api.is_legacy:
            mapped = api.actual_model(requested_model)
            if mapped is None:
                self._model_rejection_message = \
                    "Model is not exposed by this virtual API: {}".format(requested_model)
                return False
            requested = mapped
        route = self.server.model_registry.select(requested, requirements)
        if route is None:
            self._model_rejection_message = \
                "Requested model is not registered: {}".format(requested)
            return False
        self.gateway = route["gateway"]
        self.target_port = route.get("target_port", self.server.target_port)
        self.backend_base_url = route.get("base_url")
        # 响应必须声明真实执行的模型，而不是回显客户端请求里的别名。
        self.actual_model = self.gateway.model_id
        self._response_headers["X-Aiguru-Actual-Model"] = self.actual_model
        if isinstance(requested_model, str) and requested_model:
            self._response_headers["X-Aiguru-Virtual-Model"] = requested_model
        return True

    @staticmethod
    def _routing_requirements(payload):
        requested_value = payload.pop("routing", None)
        requested = requested_value if isinstance(requested_value, dict) else {}
        try:
            minimum = max(int(requested.get("min_context", 0) or 0), 0)
        except (ValueError, TypeError):
            minimum = 0
        try:
            # max_tokens 是输出预算，不是输入上下文长度；单独传给路由层，
            # 由路由层按「上下文需求 vs 输出预算」取较大值判断节点容量。
            output_budget = max(int(payload.get("max_tokens", 0) or 0), 0)
        except (ValueError, TypeError):
            output_budget = 0
        return {"min_context": minimum, "max_output_tokens": output_budget,
                "tools": bool(payload.get("tools"))}

    def _authorize(self):
        clean_path = self.path.split("?", 1)[0]
        if is_vapi_management_path(clean_path):
            return self._authorize_vapi_management()
        # 已认证的虚拟 API 可以解除自己命名空间内的隔离，但绝不能影响其他 Agent。
        if clean_path == SECURITY_RESET_PATH and getattr(self, "vapi", None) \
                and not self.vapi.is_legacy:
            return True
        if clean_path == "/v1/deployments/models" or \
                clean_path.startswith("/v1/deployments/models/") or \
                clean_path.startswith(CLUSTER_PREFIX) or \
                clean_path == SECURITY_RESET_PATH:
            return self._authorize_deployment()
        allowed, status = request_is_authorized(
            self.client_address[0], self.headers, self.server.api_token, self.server.cors_origins)
        if allowed:
            return True
        message = "CORS origin is not allowed" if status == 403 else "Missing or invalid API token"
        if self.path.split("?", 1)[0] == "/v1/messages":
            self._anthropic_error(status, message, "authentication_error")
        else:
            self._json_error(status, message, "authentication_error")
        return False

    def _authorize_deployment(self):
        """部署控制面始终要求显式 Token，回环请求也不例外。"""
        # 推理凭据不能控制节点、锁或部署；控制面使用独立凭据。
        token = getattr(self.server, "control_token", "")
        authorization = self.headers.get("Authorization", "")
        bearer = authorization[7:].strip() if authorization.lower().startswith("bearer ") else ""
        api_key = self.headers.get("X-API-Key", "")
        if token and hmac.compare_digest(bearer or api_key, token):
            return True
        self._json_error(401, "Missing or invalid deployment token", "authentication_error")
        return False

    def _authorize_vapi_management(self):
        """管理面优先使用独立 control token；纯本机部署允许回环直接管理。"""
        # 浏览器里任意页面都能对回环端口发 POST；管理面可签发/撤销 Agent 凭据，
        # 必须拒绝未登记的跨站 Origin，避免 CSRF/DNS rebinding 拿走控制权。
        origin = self.headers.get("Origin", "")
        if origin and origin not in (getattr(self.server, "cors_origins", []) or []):
            self._json_error(403, "CORS origin is not allowed", "forbidden")
            return False
        if getattr(self.server, "control_token", ""):
            return self._authorize_deployment()
        if self.client_address[0] in ("127.0.0.1", "::1"):
            return True
        self._json_error(401, "Virtual API management requires a control token",
                         "authentication_error")
        return False

    def _management_body(self):
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
        except ValueError:
            return None
        if length < 0 or length > 64 * 1024:
            return None
        try:
            return json.loads(self.rfile.read(length) if length else b"{}")
        except ValueError:
            return None

    def _registered_model_ids(self):
        catalog = getattr(self.server, "model_registry", None)
        if catalog is None:
            return []
        try:
            return sorted({entry.model_id for entry in catalog.models()})
        except (AttributeError, TypeError):
            return []

    def _vapi_connection(self, item):
        """生成可直接交给 Agent 的接入契约；不含明文凭据（服务端也读不到）。"""
        host = (self.headers.get("Host") or "").strip()
        if not host or host.startswith("0.0.0.0") or "]" in host:
            try:
                number = self.server.server_address[1]
            except (AttributeError, IndexError, TypeError):
                number = 11434
            host = "127.0.0.1:{}".format(number)
        base = "http://" + host + VAPI_PREFIX + item["id"]
        aliases = item.get("models") if isinstance(item.get("models"), dict) else {}
        registered = self._registered_model_ids()
        visible = sorted(set(registered) | set(aliases)) \
            if item.get("allow_default_model") else sorted(set(aliases))
        return {
            "id": item["id"], "label": item["label"], "enabled": item["enabled"],
            "base_url": base,
            "endpoints": {
                "chat_completions": base + "/v1/chat/completions",
                "responses": base + "/v1/responses",
                "messages": base + "/v1/messages",
                "models": base + "/v1/models",
            },
            "model_aliases": aliases, "visible_models": visible,
            "max_concurrent": item.get("max_concurrent"),
            "credential": {
                "headers": ["Authorization: Bearer <api_key>", "X-API-Key: <api_key>"],
                "key_hint": item.get("key_hint"),
            },
            "key_policy": "API key 仅在签发/轮换时返回一次；服务端只保存 SHA-256 摘要，无法再次读取或找回。",
        }

    def _vapi_management(self, method, clean_path):
        """虚拟 API 的签发、轮换、撤销、编辑、开关、接入契约与按 Agent 用量查询。"""
        if not self._authorize_vapi_management():
            return
        registry = getattr(self.server, "vapi_registry", None)
        if registry is None:
            self._json_error(503, "Virtual API registry is not configured",
                             "configuration_error")
            return
        parts = [part for part in
                 clean_path[len(VAPI_MANAGEMENT_PREFIX):].strip("/").split("/") if part]
        api_id = parts[0] if parts else None
        action = parts[1] if len(parts) > 1 else None
        status = 200
        if method == "GET":
            item = registry.get(api_id) if api_id else None
            if api_id is None:
                value = {"object": "list", "data": registry.list(),
                         "available_models": self._registered_model_ids(),
                         "usage_24h": usage_summary_by_vapi(
                             getattr(self.server, "stats_path", ""))}
            elif action == "usage" and item:
                value = {"vapi": item,
                         "usage_24h": usage_summary(
                             getattr(self.server, "stats_path", ""),
                             24 * 60 * 60, api_id)}
            elif action == "connection" and item:
                value = self._vapi_connection(item)
            elif action is None and item:
                value = item
            else:
                self._json_error(404, "Virtual API not found", "not_found")
                return
        else:
            payload = self._management_body()
            if not isinstance(payload, dict):
                self._json_error(400, "Invalid virtual API management body")
                return
            if api_id is None:
                try:
                    api, key = registry.create(payload)
                except (ValueError, TypeError) as exc:
                    self._json_error(400, str(exc), "invalid_request_error")
                    return
                value = dict(api.public())
                value["api_key"] = key          # 明文只在创建/轮换时返回一次
                value["base_path"] = VAPI_PREFIX + api.id
                value["connection"] = self._vapi_connection(value)
                value["key_policy"] = "请立即保存：此凭据不会再次显示，服务端仅保留 SHA-256 摘要。"
                status = 201
            elif action == "rotate":
                key = registry.rotate(api_id)
                if not key:
                    self._json_error(404, "Virtual API not found", "not_found")
                    return
                value = {"id": api_id, "api_key": key, "rotated": True,
                         "key_policy": "旧凭据已立即失效；请更新所有 Agent 配置。"}
            elif action == "update":
                try:
                    updated = registry.update(api_id, payload)
                except (ValueError, TypeError) as exc:
                    self._json_error(400, str(exc), "invalid_request_error")
                    return
                if not updated:
                    self._json_error(404, "Virtual API not found", "not_found")
                    return
                value = updated
            elif action == "revoke":
                if not registry.revoke(api_id):
                    self._json_error(404, "Virtual API not found", "not_found")
                    return
                value = {"id": api_id, "revoked": True}
            elif action in ("enable", "disable"):
                if not registry.set_enabled(api_id, action == "enable"):
                    self._json_error(404, "Virtual API not found", "not_found")
                    return
                value = {"id": api_id, "enabled": action == "enable"}
            else:
                self._json_error(404, "Unsupported virtual API management action",
                                 "not_found")
                return
        self._write_response(status, {"Content-Type": "application/json"},
                             json.dumps(value, ensure_ascii=False).encode("utf-8"))

    def _security_guard(self):
        guard = getattr(self.server, "security_guard", None)
        if guard is None:
            guard = SemanticSecurityGuard()
            self.server.security_guard = guard
        return guard

    def _enforce_semantic_security(self, payload, anthropic=False):
        """在模型看到不可信内容前执行确定性检测；只对可调用工具的请求阻断。"""
        metadata = payload.get("metadata") if isinstance(payload.get("metadata"), dict) else {}
        raw_session = self.headers.get("X-Aiguru-Session-ID") or \
            metadata.get("aiguru_session_id") or ""
        decision = self._security_guard().inspect(
            payload, self._scoped_session(self._vapi_id(), raw_session))
        if not decision["blocked"]:
            return True
        status = 423 if decision["reason"] == "session_quarantined" else 409
        message = "Session quarantined by semantic security policy" if status == 423 else \
            "Untrusted content matched the prompt-injection security policy"
        if anthropic:
            self._anthropic_error(status, message, "permission_error")
        else:
            self._json_error(status, message, "security_policy_error")
        return False

    def _deployment_directory(self):
        value = getattr(self.server, "deployment_dir", None)
        return value if value and os.path.isdir(value) else None

    def _proposal_paths(self):
        directory = self._deployment_directory()
        if directory is None:
            return []
        return [os.path.join(directory, name) for name in os.listdir(directory)
                if re.fullmatch(r"proposal-[0-9a-f]{32}\.json", name)]

    @staticmethod
    def _capability_hash(secret):
        return hashlib.sha256(secret.encode("utf-8")).hexdigest()

    def _prune_installation_proposals(self):
        """移除过期提案，防止历史终态永久占满审批队列。"""
        now = time.time()
        active = []
        for path in self._proposal_paths():
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    value = json.load(handle)
                expires_at = float(value.get("expires_at", 0) or 0)
                status = value.get("status")
                # 已获批且正在执行的大模型安装可能超过审批窗口，不能中途删除。
                if expires_at <= now and status != "running":
                    os.unlink(path)
                    continue
                if status == "awaiting_approval":
                    active.append(path)
            except (OSError, ValueError, TypeError, AttributeError):
                continue
        return active

    def _create_installation_proposal(self, payload):
        try:
            address = ipaddress.ip_address(self.client_address[0])
        except ValueError:
            self._json_error(403, "Installation proposals require a private network")
            return
        if not (address.is_private or address.is_loopback or address.is_link_local):
            self._json_error(403, "Installation proposals require a private network")
            return
        if self._deployment_directory() is None:
            self._json_error(404, "Installation approval is disabled")
            return
        kind = payload.get("kind") if isinstance(payload, dict) else None
        artifact = payload.get("artifact_id") if isinstance(payload, dict) else None
        controller_id = payload.get("controller_id") if isinstance(payload, dict) else None
        controller_name = payload.get("controller_name") if isinstance(payload, dict) else None
        if kind not in ("model", "gateway", "service") or \
                not valid_installation_artifact(kind, artifact) or \
                not ClusterControlPlane._valid_node_id(controller_id) or \
                not isinstance(controller_name, str) or not 1 <= len(controller_name) <= 128:
            self._json_error(400, "Invalid installation proposal")
            return
        if not installation_rate_allowed(self.client_address[0]):
            self._json_error(429, "Too many installation proposals", "rate_limit_error")
            return
        with INSTALLATION_PROPOSAL_LOCK:
            if len(self._prune_installation_proposals()) >= 100:
                self._json_error(429, "Installation proposal queue is full", "rate_limit_error")
                return
            proposal_id = uuid.uuid4().hex
            capability_secret = secrets.token_urlsafe(32)
            now = time.time()
            timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
            proposal = {"id": proposal_id, "controller_id": controller_id,
                        "controller_name": controller_name, "kind": kind,
                        "artifact_id": artifact, "status": "awaiting_approval",
                        "created_at": timestamp, "updated_at": timestamp,
                        "expires_at": now + INSTALLATION_PROPOSAL_TTL_SECONDS,
                        "capability_hash": self._capability_hash(capability_secret), "error": None}
            atomic_json_write(os.path.join(self._deployment_directory(),
                                          "proposal-" + proposal_id + ".json"), proposal)
        response = {key: value for key, value in proposal.items() if key != "capability_hash"}
        response["capability_secret"] = capability_secret
        self._write_response(202, {"Content-Type": "application/json"},
                             json.dumps(response, ensure_ascii=False).encode("utf-8"))

    def _installation_proposals(self, proposal_id=None):
        with INSTALLATION_PROPOSAL_LOCK:
            self._prune_installation_proposals()
            paths = self._proposal_paths()
            if proposal_id:
                paths = [path for path in paths if path.endswith("proposal-" + proposal_id + ".json")]
            values = []
            for path in paths:
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        value = json.load(handle)
                    if isinstance(value, dict):
                        values.append(value)
                except (OSError, ValueError):
                    continue
        if proposal_id and not values:
            self._json_error(404, "Installation proposal not found")
            return
        sanitized = [{key: value for key, value in item.items() if key != "capability_hash"}
                     for item in values]
        payload = sanitized[0] if proposal_id else {"object": "list", "data": sanitized}
        self._write_response(200, {"Content-Type": "application/json"},
                             json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    def _authorize_installation_capability(self, proposal_id):
        secret = self.headers.get("X-Aiguru-Install-Capability", "")
        if not secret or len(secret) > 256:
            self._json_error(401, "Missing or invalid installation capability", "authentication_error")
            return False
        path = os.path.join(self._deployment_directory() or "",
                            "proposal-" + proposal_id + ".json")
        with INSTALLATION_PROPOSAL_LOCK:
            self._prune_installation_proposals()
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    proposal = json.load(handle)
                expected = proposal.get("capability_hash", "")
            except (OSError, ValueError, AttributeError):
                self._json_error(404, "Installation proposal not found")
                return False
        if not expected or not hmac.compare_digest(self._capability_hash(secret), expected):
            self._json_error(401, "Missing or invalid installation capability", "authentication_error")
            return False
        return True

    def _deployment_response(self, job_id=None):
        directory = self._deployment_directory()
        if directory is None:
            self._json_error(404, "Remote deployment is disabled")
            return
        if job_id:
            if not re.fullmatch(r"[0-9a-f]{32}", job_id):
                self._json_error(404, "Deployment job not found")
                return
            paths = [os.path.join(directory, job_id + ".json")]
        else:
            paths = [os.path.join(directory, name) for name in sorted(os.listdir(directory))[-100:]
                     if re.fullmatch(r"[0-9a-f]{32}\.json", name)]
        jobs = []
        for path in paths:
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    value = json.load(handle)
                if isinstance(value, dict):
                    jobs.append(value)
            except (OSError, ValueError):
                continue
        if job_id and not jobs:
            self._json_error(404, "Deployment job not found")
            return
        payload = jobs[0] if job_id else {"object": "list", "data": jobs}
        self._write_response(200, {"Content-Type": "application/json"},
                             json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    def _create_deployment(self, payload):
        """创建本机部署任务；跨节点部署必须走 Installer Agent 审批。

        不带 node_id 的本机任务由控制面直接下发（control token 鉴权），
        带 node_id 的跨节点安装一律拒绝，避免跳过目标设备的用户同意。
        """
        directory = self._deployment_directory()
        if directory is None:
            self._json_error(404, "Remote deployment is disabled")
            return
        if not isinstance(payload, dict) or set(payload) - {"model_id", "node_id"}:
            self._json_error(400, "Deployment accepts model_id and node_id only")
            return
        model_id = payload.get("model_id")
        if not isinstance(model_id, str) or not (1 <= len(model_id) <= 512) \
                or any(ord(ch) < 32 for ch in model_id):
            self._json_error(400, "model_id must be a non-empty registered model ID")
            return
        node_id = payload.get("node_id")
        if node_id:
            self._json_error(409, "Remote installation requires an approved installation proposal",
                             "approval_required")
            return
        pending = 0
        existing = None
        for name in os.listdir(directory):
            if not name.endswith(".json"):
                continue
            try:
                with open(os.path.join(directory, name), "r", encoding="utf-8") as handle:
                    current = json.load(handle)
                    if current.get("status") in ("pending", "running"):
                        pending += 1
                        if current.get("model_id") == model_id:
                            existing = current
            except (OSError, ValueError, AttributeError):
                continue
        if existing is not None:
            self._write_response(202, {"Content-Type": "application/json",
                                       "Location": "/v1/deployments/models/" + existing["id"]},
                                 json.dumps(existing, ensure_ascii=False).encode("utf-8"))
            return
        if pending >= 100:
            self._json_error(429, "Deployment queue is full", "rate_limit_error")
            return
        job_id = uuid.uuid4().hex
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        job = {"id": job_id, "model_id": model_id, "status": "pending",
               "created_at": timestamp, "updated_at": timestamp, "error": None}
        final_path = os.path.join(directory, job_id + ".json")
        temporary = final_path + ".tmp-" + uuid.uuid4().hex
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
                json.dump(job, handle, ensure_ascii=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, final_path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
        self._write_response(202, {"Content-Type": "application/json",
                                   "Location": "/v1/deployments/models/" + job_id},
                             json.dumps(job, ensure_ascii=False).encode("utf-8"))

    def send_header(self, keyword, value):
        # 所有 SSE/NDJSON 流都声明 Connection: close；必须同步告知 BaseHTTPRequestHandler
        # 关闭 socket，否则 HTTP/1.1 会在同一连接上继续解析不存在的下一个请求。
        if keyword.lower() == "connection" and str(value).lower() == "close":
            self.close_connection = True
        super().send_header(keyword, value)

    def end_headers(self):
        origin = self.headers.get("Origin", "")
        if origin and origin in self.server.cors_origins:
            self.send_header("Access-Control-Allow-Origin", origin)
            self.send_header("Vary", "Origin")
            self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
            self.send_header("Access-Control-Allow-Headers",
                             "Authorization, X-API-Key, Content-Type, Anthropic-Version, "
                             "Anthropic-Beta, X-Aiguru-Session-ID, X-Aiguru-Agent-ID")
            self.send_header("Access-Control-Expose-Headers",
                             "X-Aiguru-Virtual-API, X-Aiguru-Actual-Model, "
                             "X-Aiguru-Virtual-Model")
            self.send_header("Access-Control-Max-Age", "600")
        super().end_headers()

    # ---- 基础转发 ----

    def _clean_headers(self):
        headers = {
            k: v
            for k, v in self.headers.items()
            if k.lower() not in ("host", "content-length", "connection",
                                 "transfer-encoding", "accept-encoding", "authorization",
                                 "x-api-key", "cookie", "origin")
        }
        # 本机网关 Token 只允许注入到回环目标。集群 base_url 由注册方提供，
        # 无条件注入会把凭据发给第三方节点（含被 DNS/元数据地址诱导的风险）。
        if self.backend_base_url and self.server.api_token:
            try:
                host = urlsplit(self.backend_base_url).hostname or ""
                if ipaddress.ip_address(host).is_loopback:
                    headers["Authorization"] = "Bearer " + self.server.api_token
            except ValueError:
                pass
        return headers

    def _cluster(self):
        return getattr(self.server, "cluster", None)

    def _cluster_get(self, clean_path):
        cluster = self._cluster()
        if cluster is None:
            self._json_error(404, "Cluster control is disabled")
            return
        if clean_path == "/v1/cluster/nodes":
            self._write_response(200, {"Content-Type": "application/json"}, json.dumps(
                {"object": "list", "data": cluster.nodes()}, ensure_ascii=False).encode("utf-8"))
            return
        self._json_error(404, "Unsupported cluster route")

    def _cluster_post(self, clean_path, payload):
        cluster = self._cluster()
        if cluster is None:
            self._json_error(404, "Cluster control is disabled")
            return
        try:
            if clean_path == "/v1/cluster/nodes/register":
                value = cluster.register(payload)
                status = 201
            else:
                heartbeat = re.fullmatch(r"/v1/cluster/nodes/([^/]+)/heartbeat", clean_path)
                lease = re.fullmatch(r"/v1/cluster/locks/([^/]+)/(acquire|renew|release)", clean_path)
                if heartbeat:
                    value = cluster.heartbeat(heartbeat.group(1), payload)
                    status = 200
                elif lease:
                    value = cluster.lease(lease.group(1), payload.get("owner"),
                                          payload.get("ttl_seconds", 30), lease.group(2),
                                          payload.get("lease_id"))
                    if value is None:
                        self._json_error(409, "Service lock is held or lease is invalid", "conflict")
                        return
                    status = 200 if lease.group(2) != "acquire" else 201
                else:
                    self._json_error(404, "Unsupported cluster route")
                    return
        except KeyError:
            self._json_error(404, "Cluster node not found")
            return
        except (ValueError, TypeError, OverflowError) as exc:
            self._json_error(400, str(exc))
            return
        self._write_response(status, {"Content-Type": "application/json"},
                             json.dumps(value, ensure_ascii=False).encode("utf-8"))

    def _backend_url(self, path):
        if self.backend_base_url:
            return self.backend_base_url.rstrip("/") + path
        return "http://127.0.0.1:{}{}".format(self.target_port, path)

    def _forward_buffered(self, body, path=None, headers=None, method=None):
        """整体转发：返回 (status, response_headers, data)。"""
        req = Request(self._backend_url(path or self.path),
                      data=body, method=method or self.command,
                      headers=headers or self._clean_headers())
        try:
            resp = urlopen(req, timeout=600)
            try:
                data = read_limited(resp)
                return resp.status, resp.headers, data
            finally:
                resp.close()
        except HTTPError as exc:
            try:
                try:
                    data = read_limited(exc)
                    return exc.code, exc.headers, data
                except ResponseTooLarge:
                    data = json.dumps({"error": {"message": "Backend response is too large",
                                                 "type": "response_too_large"}}).encode("utf-8")
                    return 502, {"Content-Type": "application/json"}, data
            finally:
                exc.close()
        except ResponseTooLarge:
            data = json.dumps({"error": {"message": "Backend response is too large",
                                         "type": "response_too_large"}}).encode("utf-8")
            return 502, {"Content-Type": "application/json"}, data
        except (URLError, OSError) as exc:
            return 502, {}, str(exc).encode("utf-8", "replace")

    def _write_response(self, status, headers, data, extra_headers=None):
        try:
            self.send_response(status)
            seen = set()
            for k, v in headers.items():
                lk = k.lower()
                if lk in ("content-length", "connection", "transfer-encoding"):
                    continue
                if lk in seen:
                    continue
                seen.add(lk)
                self.send_header(k, v)
            merged = dict(extra_headers or {})
            merged.update(getattr(self, "_response_headers", {}) or {})
            for k, v in merged.items():
                if k.lower() in ("content-length", "connection", "transfer-encoding"):
                    continue
                if k.lower() in seen:
                    continue
                seen.add(k.lower())
                self.send_header(k, v)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        except (BrokenPipeError, OSError):
            self.close_connection = True

    def _json_error(self, status, message, error_type="invalid_request_error"):
        data = json.dumps({"error": {"message": message, "type": error_type}},
                          ensure_ascii=False).encode("utf-8")
        self._write_response(status, {"Content-Type": "application/json"}, data)

    def _status_probe(self, route):
        gateway = route.get("gateway")
        base_url = route.get("base_url")
        target_port = route.get("target_port")
        if base_url:
            url = base_url.rstrip("/") + "/health"
        elif isinstance(target_port, int):
            url = "http://127.0.0.1:{}/health".format(target_port)
        else:
            return {"status": "unknown", "reason": "no backend endpoint"}
        started = time.monotonic()
        try:
            with urlopen(Request(url, method="GET"), timeout=0.8) as response:
                code = int(response.status)
            if 200 <= code < 300:
                return {"status": "operational", "latency_ms": round((time.monotonic() - started) * 1000, 1),
                        "http_status": code}
            return {"status": "degraded", "latency_ms": round((time.monotonic() - started) * 1000, 1),
                    "http_status": code}
        except (HTTPError, URLError, OSError, ValueError):
            return {"status": "major_outage", "latency_ms": round((time.monotonic() - started) * 1000, 1)}

    def _status_snapshot(self):
        registry = getattr(self.server, "model_registry", None)
        routes = list(getattr(registry, "routes", []) or [])
        components = [{"id": "gateway", "name": "Unified local gateway", "status": "operational",
                       "engine": "gateway", "updated_at": status_timestamp()}]
        for route in routes[:32]:
            gateway = route.get("gateway")
            probe = self._status_probe(route)
            component = {
                "id": "engine:{}:{}".format(getattr(gateway, "engine", "other"), getattr(gateway, "model_id", "local")),
                "name": getattr(gateway, "model_name", getattr(gateway, "model_id", "local")),
                "status": probe.pop("status", "unknown"), "engine": getattr(gateway, "engine", "other"),
                "model_id": getattr(gateway, "model_id", "local"), "updated_at": status_timestamp(),
            }
            if isinstance(route.get("target_port"), int):
                component["target_port"] = route["target_port"]
            component.update(probe)
            components.append(component)
        engine_components = components[1:]
        states = [item["status"] for item in engine_components]
        if not states:
            overall = "unknown"
        elif all(state == "operational" for state in states):
            overall = "operational"
        elif all(state == "major_outage" for state in states):
            overall = "major_outage"
        else:
            overall = "degraded"
        events = getattr(getattr(self.server, "status_events", None), "list", lambda: [])()
        return {
            "object": "status", "version": 1, "service": "aiguru-local-engine",
            "status": overall, "updated_at": status_timestamp(),
            "started_at": status_timestamp(getattr(self.server, "started_at", None)),
            "components": components, "events": events,
            "usage_24h": usage_summary(getattr(self.server, "stats_path", "")),
        }

    def _status_endpoint(self, clean_path):
        snapshot = self._status_snapshot()
        if clean_path == "/v1/status/components":
            payload = {"object": "list", "data": snapshot["components"], "updated_at": snapshot["updated_at"]}
        elif clean_path == "/v1/status/events":
            payload = {"object": "list", "data": snapshot["events"], "updated_at": snapshot["updated_at"]}
        elif clean_path == "/v1/status/history":
            # 进程内历史：重启前的时间段明确标为 unknown，不伪造可用率。
            try:
                days = max(7, min(90, int((urlsplit(self.path).query or "days=90").split("days=", 1)[-1])))
            except (TypeError, ValueError):
                days = 90
            current = snapshot["status"]
            started = getattr(self.server, "started_at", time.time())
            now = time.time()
            data = []
            for index in range(days):
                end = now - index * 86400
                state = current if end >= started - 86400 else "unknown"
                data.append({"date": datetime.fromtimestamp(end, timezone.utc).date().isoformat(), "status": state})
            data.reverse()
            known = [item for item in data if item["status"] != "unknown"]
            uptime = round(sum(item["status"] == "operational" for item in known) / len(known) * 100, 2) if known else None
            payload = {"object": "history", "days": days, "uptime_percent": uptime,
                       "components": len(snapshot["components"]), "data": data,
                       "updated_at": snapshot["updated_at"]}
        else:
            payload = snapshot
        self._write_response(200, {"Content-Type": "application/json; charset=utf-8"},
                             json.dumps(payload, ensure_ascii=False).encode("utf-8"))

    def _anthropic_error(self, status, message, error_type="invalid_request_error"):
        data = json.dumps({"type": "error", "error": {"type": error_type, "message": message},
                           "request_id": "req_" + uuid.uuid4().hex},
                          ensure_ascii=False).encode("utf-8")
        self._write_response(status, {"Content-Type": "application/json"}, data)

    def _chat_buffered(self, body):
        """原生工具调用失败后，依次使用 JSON Schema 和 Ollama generate 降级。"""
        original_tools = body.get("tools")
        requires_call = self.gateway.tool_choice_requires_call(body.get("tool_choice"))
        used_fallback = False

        def forward_json(request_body, path="/v1/chat/completions"):
            return self._forward_buffered(
                json.dumps(request_body, ensure_ascii=False).encode("utf-8"), path=path)

        def forward_fallback(request_body):
            """结构化输出不受支持时再退回纯提示；Ollama 截断则绕过 chat parser。"""
            fallback_body = self.gateway.expanded_tool_budget(
                self.gateway.fallback_tool_body(request_body))
            next_status, next_headers, next_data = forward_json(fallback_body)
            if self.gateway.engine == "ollama" and \
                    self.gateway.is_incomplete_tool_json_error(next_status, next_data):
                generate = self.gateway.ollama_generate_body(
                    fallback_body, original_tools, requires_call)
                next_status, next_headers, next_data = forward_json(generate, "/api/generate")
                if next_status in (200, 201):
                    try:
                        generated = json.loads(next_data)
                    except (ValueError, TypeError):
                        return next_status, next_headers, next_data
                    converted = self.gateway.ollama_generate_to_openai(generated)
                    next_headers = {"Content-Type": "application/json"}
                    next_data = json.dumps(converted, ensure_ascii=False).encode("utf-8")
                return next_status, next_headers, next_data
            if next_status not in (200, 201) and fallback_body.get("response_format") \
                    and self.gateway.is_tool_compatibility_error(next_status, next_data):
                prompt_only = dict(fallback_body)
                prompt_only.pop("response_format", None)
                return forward_json(prompt_only)
            return next_status, next_headers, next_data

        encoded = json.dumps(body, ensure_ascii=False).encode("utf-8")
        status, headers, data = self._forward_buffered(encoded, path="/v1/chat/completions")
        # Ollama 的 Qwen/GLM renderer 会在模型输出被截断时直接返回 400，客户端看不到
        # 可供规范化的响应。先扩大生成预算原生重试，再考虑兼容协议降级。
        if original_tools and self.gateway.is_incomplete_tool_json_error(status, data):
            body = self.gateway.expanded_tool_budget(body)
            status, headers, data = self._forward_buffered(
                json.dumps(body, ensure_ascii=False).encode("utf-8"),
                path="/v1/chat/completions")
        if self.gateway.is_incomplete_tool_json_error(status, data) and not original_tools:
            error = {"error": {
                "message": "模型尝试调用工具，但请求未携带可识别的工具元数据；请使用 tools 或 functions 字段",
                "type": "invalid_tool_definition",
                "code": "missing_tool_metadata",
            }}
            return 502, {"Content-Type": "application/json"}, \
                json.dumps(error, ensure_ascii=False).encode("utf-8"), None
        if original_tools and body.get("tool_choice") != "none" \
                and self.gateway.is_tool_compatibility_error(status, data):
            status, headers, data = forward_fallback(body)
            used_fallback = True
        try:
            payload = json.loads(data)
        except (ValueError, TypeError):
            return status, headers, data, None
        malformed_tools = bool(original_tools) and self.gateway.has_malformed_tool_response(
            payload, original_tools)
        if status in (200, 201) and malformed_tools and not used_fallback:
            status, headers, data = forward_fallback(body)
            used_fallback = True
            try:
                payload = json.loads(data)
            except (ValueError, TypeError):
                return status, headers, data, None
            malformed_tools = self.gateway.has_malformed_tool_response(
                payload, original_tools)
        payload = self.gateway.normalize_tool_response(payload, original_tools)
        if status in (200, 201) and original_tools and requires_call and not used_fallback:
            choices = payload.get("choices") if isinstance(payload, dict) else None
            message = choices[0].get("message") if choices and isinstance(choices[0], dict) else None
            if not isinstance(message, dict) or not message.get("tool_calls"):
                status, headers, data = forward_fallback(body)
                used_fallback = True
                try:
                    payload = json.loads(data)
                except (ValueError, TypeError):
                    return status, headers, data, None
                malformed_tools = self.gateway.has_malformed_tool_response(
                    payload, original_tools)
                payload = self.gateway.normalize_tool_response(payload, original_tools)
        if status in (200, 201) and malformed_tools:
            error = {"error": {
                "message": "模型返回了不完整的工具参数 JSON；网关已阻止执行，请重试或增加 max_tokens",
                "type": "invalid_tool_arguments",
                "code": "malformed_tool_call",
            }}
            return 502, {"Content-Type": "application/json"}, \
                json.dumps(error, ensure_ascii=False).encode("utf-8"), None
        if status in (200, 201) and original_tools and requires_call:
            choices = payload.get("choices") if isinstance(payload, dict) else None
            message = choices[0].get("message") if choices and isinstance(choices[0], dict) else None
            if not isinstance(message, dict) or not message.get("tool_calls"):
                error = {"error": {
                    "message": "模型未生成有效的必需工具调用；结构化降级已用尽",
                    "type": "invalid_tool_arguments",
                    "code": "required_tool_call_missing",
                }}
                return 502, {"Content-Type": "application/json"}, \
                    json.dumps(error, ensure_ascii=False).encode("utf-8"), None
        if self.gateway.is_reasoning_model():
            payload = self.gateway.openai_with_reasoning(payload)
        if status in (200, 201):
            choices = payload.get("choices") if isinstance(payload, dict) else None
            message = choices[0].get("message") if choices and isinstance(choices[0], dict) else None
            has_text = isinstance(message, dict) and isinstance(message.get("content"), str) \
                and bool(message["content"].strip())
            has_calls = isinstance(message, dict) and bool(message.get("tool_calls"))
            if not has_text and not has_calls:
                error = {"error": {"message": "本地模型返回了空响应，请增加 max_tokens 或检查模型模板",
                                   "type": "empty_model_response"}}
                return 502, {"Content-Type": "application/json"}, \
                    json.dumps(error, ensure_ascii=False).encode("utf-8"), None
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        return status, {"Content-Type": "application/json"}, data, payload

    def _write_openai_stream(self, payload):
        """把规范化后的完整响应编码为合法 OpenAI SSE（工具回退路径）。"""
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            choice = (payload.get("choices") or [{}])[0]
            message = choice.get("message") or {}
            chunk_id = payload.get("id") or "chatcmpl_" + uuid.uuid4().hex
            base = {"id": chunk_id, "object": "chat.completion.chunk",
                    "created": int(time.time()), "model": self.gateway.model_id}
            delta = {"role": "assistant"}
            if message.get("tool_calls"):
                delta["tool_calls"] = [dict(call, index=i)
                                       for i, call in enumerate(message["tool_calls"])]
            elif message.get("content") is not None:
                delta["content"] = message.get("content")
            first = dict(base, choices=[{"index": 0, "delta": delta, "finish_reason": None}])
            final = dict(base, choices=[{"index": 0, "delta": {},
                                        "finish_reason": choice.get("finish_reason") or "stop"}])
            if payload.get("usage"):
                final["usage"] = payload["usage"]
            for item in (first, final):
                self.wfile.write(("data: " + json.dumps(item, ensure_ascii=False) + "\n\n").encode("utf-8"))
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, OSError):
            pass

    def _write_responses_stream(self, response):
        """将完整 Responses 对象编码为官方命名的 SSE 生命周期事件。"""
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            sequence = 0

            def emit(kind, value):
                nonlocal sequence
                value = dict(value, type=kind, sequence_number=sequence)
                sequence += 1
                self.wfile.write(("event: " + kind + "\ndata: "
                                  + json.dumps(value, ensure_ascii=False) + "\n\n").encode("utf-8"))

            initial = dict(response, status="in_progress", output=[], usage=None)
            emit("response.created", {"response": initial})
            emit("response.in_progress", {"response": initial})
            for output_index, item in enumerate(response.get("output") or []):
                emit("response.output_item.added", {"output_index": output_index, "item": item})
                if item.get("type") == "message":
                    for content_index, part in enumerate(item.get("content") or []):
                        emit("response.content_part.added", {
                            "item_id": item["id"], "output_index": output_index,
                            "content_index": content_index,
                            "part": {"type": "output_text", "text": "", "annotations": []}})
                        emit("response.output_text.delta", {
                            "item_id": item["id"], "output_index": output_index,
                            "content_index": content_index, "delta": part.get("text", "")})
                        emit("response.output_text.done", {
                            "item_id": item["id"], "output_index": output_index,
                            "content_index": content_index, "text": part.get("text", "")})
                        emit("response.content_part.done", {
                            "item_id": item["id"], "output_index": output_index,
                            "content_index": content_index, "part": part})
                elif item.get("type") == "function_call":
                    emit("response.function_call_arguments.delta", {
                        "item_id": item["id"], "output_index": output_index,
                        "delta": item.get("arguments", "")})
                    emit("response.function_call_arguments.done", {
                        "item_id": item["id"], "output_index": output_index,
                        "arguments": item.get("arguments", "")})
                emit("response.output_item.done", {"output_index": output_index, "item": item})
            final_event = "response.completed" if response.get("status") == "completed" \
                else "response.incomplete"
            emit(final_event, {"response": response})
            self.wfile.flush()
        except (BrokenPipeError, OSError):
            pass

    def _write_anthropic_stream(self, payload):
        """把完整 Anthropic 响应编码为 Messages SSE，确保 tool_use 可流式消费。"""
        def emit(event, value):
            self.wfile.write(("event: {}\ndata: {}\n\n".format(
                event, json.dumps(value, ensure_ascii=False))).encode("utf-8"))
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            start = dict(payload, content=[], stop_reason=None, stop_sequence=None)
            emit("message_start", {"type": "message_start", "message": start})
            for index, block in enumerate(payload.get("content") or []):
                if block.get("type") == "tool_use":
                    initial = dict(block, input={})
                    emit("content_block_start", {"type": "content_block_start", "index": index,
                                                  "content_block": initial})
                    emit("content_block_delta", {"type": "content_block_delta", "index": index,
                                                  "delta": {"type": "input_json_delta",
                                                            "partial_json": json.dumps(block.get("input") or {}, ensure_ascii=False)}})
                else:
                    emit("content_block_start", {"type": "content_block_start", "index": index,
                                                  "content_block": {"type": "text", "text": ""}})
                    emit("content_block_delta", {"type": "content_block_delta", "index": index,
                                                  "delta": {"type": "text_delta", "text": block.get("text", "")}})
                emit("content_block_stop", {"type": "content_block_stop", "index": index})
            emit("message_delta", {"type": "message_delta",
                                   "delta": {"stop_reason": payload.get("stop_reason"), "stop_sequence": None},
                                   "usage": {"output_tokens": (payload.get("usage") or {}).get("output_tokens", 0)}})
            emit("message_stop", {"type": "message_stop"})
            self.wfile.flush()
        except (BrokenPipeError, OSError):
            pass

    def _forward_stream(self, body, path=None):
        """流式转发（OpenAI SSE 直通），并从最终 usage 块更新统计。"""
        req = Request(self._backend_url(path or self.path),
                      data=body, method=self.command, headers=self._clean_headers())
        try:
            resp = urlopen(req, timeout=600)
        except HTTPError as exc:
            data = exc.read()
            self._write_response(exc.code, exc.headers, data)
            return
        except (URLError, OSError) as exc:
            self._write_response(502, {}, str(exc).encode("utf-8", "replace"))
            return
        try:
            self.send_response(resp.status)
            seen = set()
            for k, v in resp.headers.items():
                lk = k.lower()
                if lk in ("content-length", "connection", "transfer-encoding"):
                    continue
                if lk in seen:
                    continue
                seen.add(lk)
                self.send_header(k, v)
            self.send_header("Connection", "close")
            self.end_headers()
            read_chunk = getattr(resp, "read1", resp.read)
            pending = b""
            stream_usage = None
            while True:
                chunk = read_chunk(8192)
                if not chunk:
                    break
                self.wfile.write(chunk)
                self.wfile.flush()
                pending += chunk
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    line = line.strip()
                    if not line.startswith(b"data:"):
                        continue
                    payload = line[5:].strip()
                    if not payload or payload == b"[DONE]":
                        continue
                    try:
                        parsed = json.loads(payload)
                        if isinstance(parsed, dict) and parsed.get("usage"):
                            stream_usage = parsed["usage"]
                    except (ValueError, TypeError):
                        pass
            if stream_usage:
                self._record_usage(stream_usage, path or self.path)
        except (BrokenPipeError, OSError):
            pass
        finally:
            resp.close()

    # ---- Anthropic 流式转换 ----

    def _anthropic_stream(self, openai_body, requested_model):
        """转发 OpenAI 流 → Anthropic SSE 事件流。"""
        req = Request(self._backend_url("/v1/chat/completions"),
                      data=json.dumps(openai_body).encode("utf-8"),
                      method="POST", headers=self._clean_headers())
        try:
            resp = urlopen(req, timeout=600)
        except HTTPError as exc:
            data = exc.read()
            self._write_response(exc.code, exc.headers, data)
            return
        except (URLError, OSError) as exc:
            self._write_response(502, {}, str(exc).encode("utf-8", "replace"))
            return

        message_id = "msg_" + uuid.uuid4().hex
        # 工具调用跟踪：index → {"id","name","started","arg_parts"}
        tools = {}
        thinking_started = False
        text_started = False
        stop_reason = "end_turn"
        output_tokens = 0

        def emit(event, data):
            try:
                self.wfile.write(
                    "event: {0}\ndata: {1}\n\n".format(
                        event, json.dumps(data, ensure_ascii=False)).encode("utf-8"))
                self.wfile.flush()
            except (BrokenPipeError, OSError):
                raise ClientDisconnected()

        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "close")
            self.end_headers()
            emit("message_start", {
                "type": "message_start",
                "message": {
                    "id": message_id,
                    "type": "message",
                    "role": "assistant",
                    "model": requested_model,
                    "content": [],
                    "stop_reason": None,
                    "stop_sequence": None,
                    "usage": {"input_tokens": 0, "output_tokens": 0},
                },
            })
            for raw in resp:
                line = raw.decode("utf-8", "replace").strip()
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                try:
                    chunk = json.loads(data)
                except ValueError:
                    continue
                choices = chunk.get("choices") or []
                if not choices:
                    usage = chunk.get("usage")
                    if usage:
                        output_tokens = int(usage.get("completion_tokens", 0) or 0)
                    continue
                delta = choices[0].get("delta") or {}
                finish = choices[0].get("finish_reason")
                if finish:
                    stop_map = {"stop": "end_turn", "tool_calls": "tool_use",
                                "length": "max_tokens"}
                    stop_reason = stop_map.get(finish, "end_turn")
                usage = chunk.get("usage")
                if usage:
                    output_tokens = int(usage.get("completion_tokens", 0) or 0)
                text = delta.get("content")
                thinking = delta.get("reasoning_content") or delta.get("reasoning")
                if thinking:
                    if not thinking_started:
                        thinking_started = True
                        emit("content_block_start", {
                            "type": "content_block_start", "index": 0,
                            "content_block": {"type": "thinking", "thinking": "", "signature": ""},
                        })
                    emit("content_block_delta", {
                        "type": "content_block_delta", "index": 0,
                        "delta": {"type": "thinking_delta", "thinking": thinking},
                    })
                if text:
                    if not text_started:
                        text_started = True
                        emit("content_block_start", {
                            "type": "content_block_start",
                            "index": 1 if thinking_started else 0,
                            "content_block": {"type": "text", "text": ""},
                        })
                    emit("content_block_delta", {
                        "type": "content_block_delta",
                        "index": 1 if thinking_started else 0,
                        "delta": {"type": "text_delta", "text": text},
                    })
                for call in delta.get("tool_calls") or []:
                    idx = call.get("index", 0)
                    entry = tools.setdefault(idx, {"id": "", "name": "", "started": False,
                                                   "arg_parts": []})
                    if call.get("id"):
                        entry["id"] = call["id"]
                    fn = call.get("function") or {}
                    if fn.get("name"):
                        entry["name"] = fn["name"]
                    if fn.get("arguments"):
                        entry["arg_parts"].append(fn["arguments"])
                    if not entry["started"]:
                        entry["started"] = True
                        emit("content_block_start", {
                            "type": "content_block_start",
                            "index": idx + int(thinking_started) + int(text_started),
                            "content_block": {"type": "tool_use",
                                              "id": entry["id"] or "toolu_" + uuid.uuid4().hex[:8],
                                              "name": entry["name"] or ""},
                        })
                    emit("content_block_delta", {
                        "type": "content_block_delta",
                        "index": idx + int(thinking_started) + int(text_started),
                        "delta": {"type": "input_json_delta",
                                  "partial_json": fn.get("arguments", "")},
                    })
            # 收尾
            if thinking_started:
                emit("content_block_stop", {"type": "content_block_stop", "index": 0})
            if text_started:
                emit("content_block_stop", {"type": "content_block_stop",
                                            "index": 1 if thinking_started else 0})
            for idx in sorted(tools):
                emit("content_block_stop", {"type": "content_block_stop",
                                            "index": idx + int(thinking_started) + int(text_started)})
            emit("message_delta", {
                "type": "message_delta",
                "delta": {"stop_reason": stop_reason, "stop_sequence": None},
                "usage": {"output_tokens": output_tokens},
            })
            emit("message_stop", {"type": "message_stop"})
        except (BrokenPipeError, OSError, ClientDisconnected):
            pass
        finally:
            resp.close()

    # ---- 各协议入口 ----

    def do_GET(self):
        if not self._begin_request():
            return
        clean_path = self.path.split("?", 1)[0]
        if is_vapi_management_path(clean_path):
            self._vapi_management("GET", clean_path)
            return
        proposal_match = re.fullmatch(r"/v1/installations/proposals/([0-9a-f]{32})", clean_path)
        if proposal_match:
            if not self._authorize_installation_capability(proposal_match.group(1)):
                return
            self._installation_proposals(proposal_match.group(1))
            return
        if not self._authorize():
            return
        if not is_allowed_route("GET", self.path):
            self._json_error(404, "Unsupported gateway route")
            return
        if clean_path == SECURITY_STATUS_PATH:
            self._write_response(200, {"Content-Type": "application/json"},
                                 json.dumps(self._security_guard().status(
                                     self._vapi_id())).encode("utf-8"))
            return
        if clean_path == "/v1/status/test-event":
            # 仅允许回环调用，用于本地状态页视觉/交互验收；不会开放到局域网。
            if self.client_address[0] not in ("127.0.0.1", "::1"):
                self._json_error(403, "Test events are loopback-only", "forbidden")
                return
            query = dict(item.split("=", 1) for item in self.path.split("?", 1)[1].split("&")
                         if "=" in item) if "?" in self.path else {}
            status = query.get("status", "degraded")
            if status not in ("unknown", "degraded", "major_outage"):
                self._json_error(400, "Unsupported test event status")
                return
            summary = query.get("summary", "本地测试事件")[:160]
            event = self.server.status_events.record("test", status, summary)
            self._write_response(201, {"Content-Type": "application/json"},
                                 json.dumps(event, ensure_ascii=False).encode("utf-8"))
            return
        if clean_path in ("/v1/status", "/v1/status/components", "/v1/status/events", "/v1/status/history"):
            self._status_endpoint(clean_path)
            return
        if clean_path == "/status":
            page = os.path.join(os.path.dirname(__file__), "status.html")
            try:
                with open(page, "rb") as handle:
                    data = handle.read(512 * 1024 + 1)
                if len(data) > 512 * 1024:
                    self._json_error(500, "Status page is too large", "configuration_error")
                    return
            except OSError:
                self._json_error(404, "Status page is not installed", "not_found")
                return
            self._write_response(200, {"Content-Type": "text/html; charset=utf-8",
                                       "Cache-Control": "no-store"}, data)
            return
        if clean_path == INSTALLATION_PREFIX:
            if not self._authorize_deployment():
                return
            self._installation_proposals()
            return
        if clean_path.startswith(CLUSTER_PREFIX):
            if not self._authorize_deployment():
                return
            self._cluster_get(clean_path)
            return
        if clean_path == "/v1/deployments/models" or clean_path.startswith("/v1/deployments/models/"):
            if not self._authorize_deployment():
                return
            job_id = clean_path.rsplit("/", 1)[-1] if clean_path != "/v1/deployments/models" else None
            self._deployment_response(job_id)
            return
        body = None
        if clean_path == "/v1/models":
            # 网关只暴露当前已加载模型；无需调用后端的缓存扫描接口。MLX 在缓存目录
            # 尚未创建时会让该接口异常断开，表现为 Agent 看到 200 但响应体为空。
            # 虚拟 API 只列出自己命名空间里的别名，并显式标注真实模型，避免身份幻觉。
            payload = {"object": "list", "data": [{
                "id": model_id, "object": "model", "owned_by": "local",
                "metadata": {
                    "engine": item.engine, "family": item.family,
                    "capabilities": item.capabilities or ["text-generation"],
                    "actual_model": item.model_id,
                    "virtual_api": self._vapi_id(),
                    "interfaces": ["/v1/chat/completions", "/v1/responses", "/v1/messages"]
                    + (["/v1/images/generations"]
                       if "image-generation" in (item.capabilities or []) else [])
                    + (["/v1/vision/ground"]
                       if "grounding" in (item.capabilities or []) else []),
                },
            } for model_id, item in self._visible_models()]}
            self._write_response(200, {"Content-Type": "application/json"},
                                 json.dumps(payload, ensure_ascii=False).encode("utf-8"))
            return
        # /health /metrics 及其余 GET 直通
        status, headers, data = self._forward_buffered(body)
        self._write_response(status, headers, data)

    def do_OPTIONS(self):
        origin = self.headers.get("Origin", "")
        if not origin or origin not in self.server.cors_origins:
            self._json_error(403, "CORS origin is not allowed")
            return
        self.send_response(204)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        if not self._begin_request():
            return
        # HTTP/1.1 keep-alive 下未知编码会残留 body 污染同一连接的下一个请求。
        if "chunked" in self.headers.get("Transfer-Encoding", "").lower():
            self._json_error(400, "Chunked request bodies are not supported")
            return
        clean_path = self.path.split("?", 1)[0]
        if is_vapi_management_path(clean_path):
            self._vapi_management("POST", clean_path)
            return
        if clean_path == INSTALLATION_PREFIX:
            # 提案会写入本地审批队列；LAN 调用方必须持有网关凭据，不能匿名提交。
            if not self._authorize():
                return
            try:
                length = int(self.headers.get("Content-Length", 0) or 0)
            except ValueError:
                self._json_error(400, "Invalid Content-Length")
                return
            if length < 0 or length > 64 * 1024:
                self._json_error(413, "Request body is too large")
                return
            try:
                payload = json.loads(self.rfile.read(length) if length else b"{}")
            except ValueError:
                self._json_error(400, "Invalid JSON request body")
                return
            self._create_installation_proposal(payload)
            return
        if not self._authorize():
            return
        if not is_allowed_route("POST", self.path):
            self._json_error(404, "Unsupported gateway route")
            return
        try:
            length = int(self.headers.get("Content-Length", 0) or 0)
        except ValueError:
            self._json_error(400, "Invalid Content-Length")
            return
        if length < 0 or length > MAX_REQUEST_BYTES:
            self._json_error(413, "Request body is too large")
            return
        raw = self.rfile.read(length) if length else None
        gateway = self.gateway

        if clean_path == "/v1/images/generations":
            self._image_generation(raw)
            return
        if clean_path == "/v1/vision/ground":
            self._vision_ground(raw)
            return

        if clean_path == SECURITY_RESET_PATH:
            try:
                payload = json.loads(raw or b"{}")
            except ValueError:
                self._json_error(400, "Invalid JSON request body")
                return
            session_id = payload.get("session_id") if isinstance(payload, dict) else None
            if not isinstance(session_id, str) or not session_id:
                self._json_error(400, "session_id is required")
                return
            scoped = self._scoped_session(self._vapi_id(), session_id)
            reset = self._security_guard().reset(scoped)
            self._security_guard().audit("session_reset", {
                "session_id_sha256": hashlib.sha256(scoped.encode()).hexdigest(),
                "vapi": self._vapi_id(), "reset": reset,
            })
            self._write_response(200, {"Content-Type": "application/json"},
                                 json.dumps({"reset": reset}).encode("utf-8"))
            return

        if clean_path.startswith(CLUSTER_PREFIX):
            if not self._authorize_deployment():
                return
            try:
                payload = json.loads(raw or b"{}")
            except ValueError:
                self._json_error(400, "Invalid JSON request body")
                return
            if not isinstance(payload, dict):
                self._json_error(400, "JSON request body must be an object")
                return
            self._cluster_post(clean_path, payload)
            return

        if self.path.split("?", 1)[0] == "/v1/deployments/models":
            if not self._authorize_deployment():
                return
            try:
                payload = json.loads(raw or b"{}")
            except ValueError:
                self._json_error(400, "Invalid JSON request body")
                return
            self._create_deployment(payload)
            return

        # ---- Ollama 原生 Chat ----
        # 原生 SDK 会调用 /api/chat；带 tools 的请求必须进入与 OpenAI/Anthropic 相同的
        # 规范化链，不能直通 Ollama 的 Qwen parser 后把未闭合 JSON 400 泄漏给 Agent。
        if self.path.split("?", 1)[0] == "/api/chat":
            try:
                payload = json.loads(raw or b"{}")
            except ValueError:
                self._json_error(400, "Invalid JSON request body")
                return
            if not isinstance(payload, dict):
                self._json_error(400, "JSON request body must be an object")
                return
            if not self._enforce_semantic_security(payload):
                return
            if not self._select_gateway(payload.get("model"), self._routing_requirements(payload)):
                self._model_error()
                return
            gateway = self.gateway
            # 无工具请求保留 Ollama 原生 format/raw/keep_alive 等完整语义。
            if not payload.get("tools"):
                payload["model"] = gateway.model_id
                status, headers, data = self._forward_buffered(
                    json.dumps(payload, ensure_ascii=False).encode("utf-8"))
                self._write_response(status, headers, data)
                return
            wants_stream = bool(payload.get("stream", True))
            body = gateway.ollama_chat_to_openai_body(payload)
            status, headers, data, chat_payload = self._chat_buffered(body)
            if status not in (200, 201) or not isinstance(chat_payload, dict):
                self._write_response(status, headers, data)
                return
            response = gateway.openai_to_ollama_chat(chat_payload)
            usage = chat_payload.get("usage")
            if usage:
                self._record_usage(usage, "/api/chat")
            encoded = json.dumps(response, ensure_ascii=False).encode("utf-8")
            if wants_stream:
                encoded += b"\n"
                self._write_response(200, {"Content-Type": "application/x-ndjson"}, encoded)
            else:
                self._write_response(200, {"Content-Type": "application/json"}, encoded)
            return

        # ---- OpenAI Responses ----
        if self.path == "/v1/responses":
            try:
                payload = json.loads(raw or b"{}")
            except ValueError:
                self._json_error(400, "Invalid JSON request body")
                return
            if not isinstance(payload, dict):
                self._json_error(400, "JSON request body must be an object")
                return
            if not self._enforce_semantic_security(payload):
                return
            if not self._select_gateway(payload.get("model"), self._routing_requirements(payload)):
                self._model_error()
                return
            gateway = self.gateway
            unsupported_tools = [tool.get("type") for tool in payload.get("tools") or []
                                 if not isinstance(tool, dict) or tool.get("type") != "function"]
            if unsupported_tools:
                self._json_error(400, "Local gateway currently supports function tools only")
                return
            if payload.get("background") or payload.get("conversation") \
                    or payload.get("previous_response_id"):
                self._json_error(400, "Local gateway is stateless; background and stored conversations are unsupported")
                return
            requested_model = self.actual_model or gateway.model_id
            body = gateway.responses_to_chat(payload)
            status, headers, data, chat_payload = self._chat_buffered(body)
            if status not in (200, 201) or not isinstance(chat_payload, dict):
                self._write_response(status, headers, data)
                return
            response = gateway.chat_to_responses(chat_payload, requested_model, payload)
            usage = chat_payload.get("usage")
            if usage:
                self._record_usage(usage, "/v1/responses")
            if payload.get("stream"):
                self._write_responses_stream(response)
            else:
                self._write_response(200, {"Content-Type": "application/json"},
                                     json.dumps(response, ensure_ascii=False).encode("utf-8"))
            return

        # ---- Anthropic Messages ----
        if self.path == "/v1/messages":
            try:
                payload = json.loads(raw or b"{}")
            except ValueError:
                self._anthropic_error(400, "Invalid JSON request body")
                return
            if not isinstance(payload, dict):
                self._anthropic_error(400, "JSON request body must be an object")
                return
            if not self._enforce_semantic_security(payload, anthropic=True):
                return
            if not self._select_gateway(payload.get("model"), self._routing_requirements(payload)):
                self._model_error(anthropic=True)
                return
            gateway = self.gateway
            if not isinstance(payload.get("max_tokens"), int) or payload["max_tokens"] <= 0:
                self._anthropic_error(400, "max_tokens is required and must be a positive integer")
                return
            requested_model = self.actual_model or gateway.model_id
            openai_body = gateway.anthropic_to_openai(payload)
            # 带工具时先规范化完整响应，再合成 SSE；这样原生与回退路径语义一致。
            if payload.get("stream") and not openai_body.get("tools") \
                    and gateway.family not in ("qwen", "glm"):
                self._anthropic_stream(openai_body, requested_model)
                return
            openai_body["stream"] = False
            status, headers, data, resp_payload = self._chat_buffered(openai_body)
            try:
                if status not in (200, 201) or not isinstance(resp_payload, dict):
                    raise ValueError("backend request failed")
                usage = resp_payload.get("usage")
                if usage:
                    self._record_usage(usage, "/v1/messages")
                anthropic = gateway.openai_to_anthropic(resp_payload, requested_model, None)
                if payload.get("stream") and status in (200, 201):
                    self._write_anthropic_stream(anthropic)
                    return
                data = json.dumps(anthropic, ensure_ascii=False).encode("utf-8")
                headers = {"Content-Type": "application/json"}
                status = 200 if status in (200, 201) else status
            except (ValueError, AttributeError, TypeError):
                # Anthropic SDK 只接受 {"type":"error","error":{"type","message"}}；
                # 后端 500/502、非 JSON 响应或转换失败都不能原样透传 OpenAI 错误体。
                message = "Backend request failed"
                detail = resp_payload if isinstance(resp_payload, dict) else None
                if detail is None:
                    try:
                        parsed = json.loads(data)
                        detail = parsed if isinstance(parsed, dict) else None
                    except (ValueError, TypeError):
                        detail = None
                if isinstance(detail, dict):
                    error = detail.get("error")
                    if isinstance(error, dict):
                        message = str(error.get("message") or message)
                    elif isinstance(error, str):
                        message = error
                    elif isinstance(detail.get("message"), str):
                        message = detail["message"]
                code = status if 400 <= status <= 599 else 502
                self._anthropic_error(code, message, "api_error")
                return
            self._write_response(status, headers, data)
            return

        # ---- OpenAI / DeepSeek chat ----
        if self.path in ("/v1/chat/completions", "/chat/completions"):
            try:
                payload = json.loads(raw or b"{}")
            except ValueError:
                self._json_error(400, "Invalid JSON request body")
                return
            if not isinstance(payload, dict):
                self._json_error(400, "JSON request body must be an object")
                return
            if not self._enforce_semantic_security(payload):
                return
            if not self._select_gateway(payload.get("model"), self._routing_requirements(payload)):
                self._model_error()
                return
            gateway = self.gateway
            body, reasoning = gateway.adapt_chat_body(payload)
            forward_path = "/v1/chat/completions"  # DeepSeek 无 /v1 前缀 → 归一化
            # 工具流使用完整响应规范化后合成 SSE，兼容不支持原生 tools 的后端。
            if body.get("stream") and not body.get("tools") \
                    and gateway.family not in ("qwen", "glm"):
                self._forward_stream(json.dumps(body).encode("utf-8"), path=forward_path)
                return
            wants_stream = bool(body.get("stream"))
            if wants_stream:
                body["stream"] = False
            status, headers, data, resp_payload = self._chat_buffered(body)
            try:
                usage = resp_payload.get("usage")
                if usage:
                    self._record_usage(usage, self.path)
                if reasoning:
                    resp_payload = gateway.openai_with_reasoning(resp_payload)
                    data = json.dumps(resp_payload, ensure_ascii=False).encode("utf-8")
                    headers = {"Content-Type": "application/json"}
                if wants_stream and status in (200, 201):
                    self._write_openai_stream(resp_payload)
                    return
            except (ValueError, AttributeError):
                pass
            self._write_response(status, headers, data)
            return

        # ---- 其余（completions / embeddings / models 等）：改写 model 后直通 ----
        body = raw
        try:
            payload = json.loads(raw or b"{}")
            if isinstance(payload, dict) and not self._select_gateway(payload.get("model"), self._routing_requirements(payload)):
                self._model_error()
                return
            gateway = self.gateway
            payload = gateway.adapt_other_body(payload)
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        except (ValueError, TypeError):
            pass
        status, headers, data = self._forward_buffered(body)
        try:
            resp_payload = json.loads(data)
            usage = resp_payload.get("usage")
            if usage:
                self._record_usage(usage, self.path)
        except (ValueError, AttributeError):
            pass
        self._write_response(status, headers, data)

    def _image_generation(self, raw):
        command = getattr(self.server, "image_command", "")
        if not command:
            self._json_error(501, "image generation runtime is not installed", "not_implemented")
            return
        try:
            payload = json.loads(raw or b"{}")
        except ValueError:
            self._json_error(400, "Invalid JSON request body")
            return
        if not isinstance(payload, dict) or not isinstance(payload.get("prompt"), str) \
                or not payload["prompt"].strip():
            self._json_error(400, "prompt is required")
            return
        try:
            width = int(payload.get("width", 1024)); height = int(payload.get("height", 1024))
            if not (64 <= width <= 2048 and 64 <= height <= 2048):
                raise ValueError
            seed = payload.get("seed")
            args = [command, payload["prompt"], "--width", str(width), "--height", str(height)]
            if isinstance(seed, int) and not isinstance(seed, bool): args += ["--seed", str(seed)]
            if payload.get("turbo") is True: args.append("--turbo")
            with tempfile.TemporaryDirectory(prefix="aiguru-image-") as directory:
                output = os.path.join(directory, "output.png")
                proc = subprocess.Popen(args + ["-o", output], stdout=subprocess.DEVNULL,
                                        stderr=subprocess.DEVNULL)
                if payload.get("stream") is True:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/event-stream")
                    self.send_header("Cache-Control", "no-cache")
                    self.send_header("Connection", "close")
                    self.end_headers()
                    started = time.time()
                    while proc.poll() is None:
                        elapsed = time.time() - started
                        if elapsed >= 900:
                            proc.kill(); proc.wait()
                            # 流已开始，不能再写第二个 HTTP 响应；只发 SSE 错误事件。
                            self.wfile.write(
                                b'data: {"error":{"message":"image generation timed out"}}\n\n'
                                b'data: [DONE]\n\n')
                            self.wfile.flush()
                            return
                        # moss-mlx does not expose a stable machine-readable progress API;
                        # emit a bounded heartbeat so clients can keep the task alive.
                        progress = min(0.95, elapsed / 900.0)
                        self.wfile.write(("data: " + json.dumps({
                            "type": "progress", "progress": progress
                        }) + "\n\n").encode("utf-8"))
                        self.wfile.flush()
                        time.sleep(1)
                    if proc.returncode != 0 or not os.path.isfile(output):
                        self.wfile.write(b'data: {"error":{"message":"image runtime failed"}}\n\n')
                        self.wfile.flush()
                        return
                else:
                    try:
                        proc.wait(timeout=900)
                    except subprocess.TimeoutExpired:
                        proc.kill(); proc.wait()
                        raise
                    if proc.returncode != 0 or not os.path.isfile(output):
                        self._json_error(502, "image runtime failed", "backend_error")
                        return
                with open(output, "rb") as handle:
                    encoded = base64.b64encode(handle.read()).decode("ascii")
                if payload.get("stream") is True:
                    self.wfile.write(("data: " + json.dumps({
                        "type": "complete", "b64_json": encoded
                    }) + "\n\ndata: [DONE]\n\n").encode("utf-8"))
                    self.wfile.flush()
                    return
        except ValueError:
            self._json_error(400, "invalid image dimensions", "invalid_request")
            return
        except subprocess.TimeoutExpired:
            self._json_error(504, "image generation timed out", "backend_timeout")
            return
        self._write_response(200, {"Content-Type": "application/json"}, json.dumps({
            "created": int(time.time()), "data": [{"b64_json": encoded}]
        }).encode("utf-8"))

    def _vision_ground(self, raw):
        """Stable VLX-Seek API surface; the optional PyTorch/MPS worker is separate."""
        self._json_error(501,
                         "VLX-Seek worker is not installed; install the PyTorch MPS runtime",
                         "not_implemented")


    def log_message(self, *args):
        pass


class StableThreadingHTTPServer(ThreadingHTTPServer):
    """请求线程不阻止退出，并扩大突发连接排队容量。"""
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 128

    def __init__(self, address, handler, max_concurrent=8):
        super().__init__(address, handler)
        self.security_guard = SemanticSecurityGuard()
        self.status_events = StatusEventStore()
        self.started_at = time.time()
        self.stats_path = ""
        self._request_slots = threading.BoundedSemaphore(max(1, max_concurrent))
        self._pending_slots = threading.BoundedSemaphore(self.request_queue_size)

    def process_request(self, request, client_address):
        if not self._pending_slots.acquire(blocking=False):
            self._reject_overloaded(request)
            return
        threading.Thread(target=self._dispatch_when_available,
                         args=(request, client_address), daemon=True).start()

    def _dispatch_when_available(self, request, client_address):
        try:
            if not self._request_slots.acquire(timeout=1):
                self._reject_overloaded(request)
                return
            try:
                super().process_request(request, client_address)
            except Exception:
                self._request_slots.release()
                raise
        finally:
            self._pending_slots.release()

    def _reject_overloaded(self, request):
        try:
            request.sendall(
                b"HTTP/1.1 503 Service Unavailable\r\n"
                b"Content-Type: application/json\r\n"
                b"Retry-After: 1\r\n"
                b"Connection: close\r\n"
                b"Content-Length: 62\r\n\r\n"
                b'{"error":{"message":"gateway overloaded","type":"overloaded"}}')
        except OSError:
            pass
        finally:
            self.shutdown_request(request)

    def process_request_thread(self, request, client_address):
        try:
            super().process_request_thread(request, client_address)
        finally:
            self._request_slots.release()


def main():
    parser = argparse.ArgumentParser(description="本地推理 API 网关 + 统计代理")
    parser.add_argument("--listen", type=int, required=True)
    parser.add_argument("--target", type=int, required=True)
    parser.add_argument("--stats", required=True)
    parser.add_argument("--model", default="local", help="本地模型展示名（自动适配用）")
    parser.add_argument("--model-id", default="local", help="后端模型 id（请求 model 改写目标）")
    parser.add_argument("--family", default="other",
                        help="模型家族 deepseek/qwen/glm/grok/other")
    parser.add_argument("--engine", default="other",
                        choices=("llama", "ollama", "mlx", "colibri", "other"),
                        help="后端类型，用于注入后端专用参数")
    parser.add_argument("--reasoning", type=int, default=0,
                        help="推理型模型（1 = 响应带 reasoning_content）")
    parser.add_argument("--max-concurrent", type=int, default=8,
                        help="最大并发转发请求数（默认 8）")
    parser.add_argument("--bind", choices=("127.0.0.1", "0.0.0.0"), default="127.0.0.1",
                        help="网关监听地址；局域网模式使用 0.0.0.0")
    parser.add_argument("--api-token-file", help="远端访问令牌文件（不通过进程参数暴露令牌）")
    parser.add_argument("--control-token-file",
                        help="集群、部署与服务锁独立控制令牌文件")
    parser.add_argument("--cors-origin", action="append", default=[],
                        help="允许的浏览器 Origin，可重复；不支持通配符")
    parser.add_argument("--registry-file",
                        help="可选模型注册表 JSON；用于一个网关路由多个常驻内部服务")
    parser.add_argument("--deployment-dir",
                        help="可选远程部署任务目录；未配置时部署控制面完全关闭")
    parser.add_argument("--cluster-dir",
                        help="可选集群状态目录；启用节点心跳、服务租约与跨节点模型路由")
    parser.add_argument("--security-audit",
                        help="语义安全审计 JSONL；仅记录哈希、信号和策略结果")
    parser.add_argument("--vapi-file",
                        help="虚拟 API 注册表 JSON；每个 Agent 独立凭据、模型别名与并发配额")
    parser.add_argument("--vapi-strict", type=int, default=0,
                        help="1 = 回环请求也必须使用虚拟 API 凭据（多用户强隔离模式）")
    parser.add_argument("--image-command",
                        help="可选图像运行时命令（例如 moss-mlx）；启用 /v1/images/generations")
    parser.add_argument("cmd", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if not args.cmd or args.cmd[0] != "--":
        sys.exit("用法: stats_proxy.py --listen N --target N --stats FILE [--model M] "
                 "[--model-id ID] [--family F] [--reasoning 0|1] -- cmd...")
    command = args.cmd[1:]
    # `-m 模块` 形式（如 -m mlx_lm.server）需要前置当前解释器；直接可执行文件则原样运行
    if command and command[0] == "-m":
        command = [sys.executable] + command

    child = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=sys.stderr)

    api_token = ""
    if args.api_token_file:
        try:
            with open(args.api_token_file, "r", encoding="utf-8") as token_file:
                api_token = token_file.read().strip()
        except OSError as exc:
            child.terminate()
            sys.exit("无法读取 API token: {}".format(exc))
        if len(api_token) < 32:
            child.terminate()
            sys.exit("API token 无效")
    if args.bind != "127.0.0.1" and not api_token:
        child.terminate()
        sys.exit("远端监听必须配置 API token")

    control_token = ""
    if args.control_token_file:
        try:
            with open(args.control_token_file, "r", encoding="utf-8") as token_file:
                control_token = token_file.read().strip()
        except OSError as exc:
            child.terminate()
            sys.exit("无法读取控制 token: {}".format(exc))
        if len(control_token) < 32:
            child.terminate()
            sys.exit("控制 token 无效")
    if (args.deployment_dir or args.cluster_dir) and not control_token:
        child.terminate()
        sys.exit("启用集群或部署控制面必须配置独立控制 token")

    try:
        server = StableThreadingHTTPServer((args.bind, args.listen), ProxyHandler,
                                           max_concurrent=args.max_concurrent)
    except OSError as exc:
        # 监听端口被占用等：先终止子进程再退出，避免遗留孤儿引擎
        try:
            child.terminate()
            child.wait(timeout=5)
        except Exception:
            child.kill()
        sys.exit("监听端口 {} 不可用: {}".format(args.listen, exc))
    server.target_port = args.target
    server.api_token = api_token
    server.control_token = control_token
    server.cors_origins = set(args.cors_origin)
    server.stats_path = args.stats
    server.image_command = args.image_command or ""
    audit_path = os.path.realpath(args.security_audit or (args.stats + ".security.jsonl"))
    os.makedirs(os.path.dirname(audit_path), mode=0o700, exist_ok=True)
    server.security_guard = SemanticSecurityGuard(audit_path)
    server.deployment_dir = None
    if args.deployment_dir:
        deployment_dir = os.path.realpath(args.deployment_dir)
        os.makedirs(deployment_dir, mode=0o700, exist_ok=True)
        os.chmod(deployment_dir, 0o700)
        server.deployment_dir = deployment_dir
    server.cluster = ClusterControlPlane(os.path.realpath(args.cluster_dir)) \
        if args.cluster_dir else None
    server.gateway = ApiGateway(
        model_name=args.model, model_id=args.model_id,
        family=args.family, reasoning=args.reasoning, engine=args.engine)
    server.vapi_registry = VirtualApiRegistry(
        os.path.realpath(args.vapi_file) if args.vapi_file else None,
        strict=bool(args.vapi_strict))
    try:
        server.model_registry = ModelRouteRegistry.load(
            args.registry_file, server.gateway, args.target)
        server.model_registry.cluster = server.cluster
    except (OSError, ValueError, TypeError) as exc:
        child.terminate()
        server.server_close()
        sys.exit("无法读取模型注册表: {}".format(exc))
    server.status_events.record("gateway", "operational", "Gateway initialized",
                                {"models": len(server.model_registry.routes), "bind": args.bind})

    def shutdown_handler(*_):
        threading.Thread(target=server.shutdown, daemon=True).start()
        try:
            child.terminate()
            child.wait(timeout=5)
        except Exception:
            child.kill()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    threading.Thread(target=server.serve_forever, daemon=True).start()
    # 子进程（引擎）退出 → 代理退出，插件据此判定引擎状态
    while child.poll() is None:
        time.sleep(1)
    threading.Thread(target=server.shutdown, daemon=True).start()
    sys.exit(child.returncode)


if __name__ == "__main__":
    main()
