#!/usr/bin/env python3
"""Aiguru 局域网安装审批 Agent。

该进程与推理网关完全独立：不代理推理、不执行命令，也不接受下载 URL
或本地路径。它只将经过校验的安装提案原子写入私有数据目录，由桌面应用
向本机用户展示并决定是否执行。
"""

import argparse
import hashlib
import hmac
import ipaddress
import json
import os
import re
import secrets
import shutil
import stat
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


PROPOSAL_PREFIX = "/v1/installations/proposals"
PROPOSAL_TTL_SECONDS = 10 * 60
MAX_PROPOSALS = 100
MAX_BODY_BYTES = 64 * 1024


def atomic_json_write(path, value):
    temporary = path + ".tmp-" + uuid.uuid4().hex
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass


class ProposalStore:
    def __init__(self, directory, ttl_seconds=PROPOSAL_TTL_SECONDS, artifact_installer=None):
        self.directory = os.path.realpath(directory)
        self.ttl_seconds = max(30, min(3600, int(ttl_seconds)))
        self.lock = threading.RLock()
        self.artifact_installer = artifact_installer
        os.makedirs(self.directory, mode=0o700, exist_ok=True)
        os.chmod(self.directory, 0o700)

    def _paths(self):
        return [os.path.join(self.directory, name) for name in os.listdir(self.directory)
                if re.fullmatch(r"proposal-[0-9a-f]{32}\.json", name)]

    def _prune(self):
        now = time.time()
        active = []
        for path in self._paths():
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    value = json.load(handle)
                status = value.get("status")
                transactional = {"running", "staging", "verifying", "activating", "rolling_back"}
                if float(value.get("expires_at", 0) or 0) <= now and status not in transactional:
                    os.unlink(path)
                elif status == "awaiting_approval":
                    active.append(path)
            except (OSError, ValueError, TypeError, AttributeError):
                continue
        return active

    @staticmethod
    def _valid_node_id(value):
        return isinstance(value, str) and bool(
            re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", value))

    @staticmethod
    def _valid_artifact(kind, value):
        if not isinstance(value, str) or not 1 <= len(value) <= 512:
            return False
        if any(ord(character) < 32 for character in value) or "\\" in value:
            return False
        if value.startswith("/") or "://" in value or any(
                part in (".", "..") for part in value.split("/")):
            return False
        if kind in ("gateway", "service"):
            return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value))
        # 模型使用受控注册表 ID，例如 organization/repository。
        return bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:@+/-]{0,511}", value))

    @staticmethod
    def _public(value):
        return {key: item for key, item in value.items() if key != "capability_hash"}

    def create(self, payload):
        allowed = {"controller_id", "controller_name", "kind", "artifact_id"}
        if not isinstance(payload, dict) or set(payload) - allowed:
            raise ValueError("proposal contains unsupported fields")
        kind = payload.get("kind")
        artifact = payload.get("artifact_id")
        controller_id = payload.get("controller_id")
        controller_name = payload.get("controller_name")
        if kind not in ("model", "gateway", "service") or \
                not self._valid_artifact(kind, artifact) or \
                not self._valid_node_id(controller_id) or \
                not isinstance(controller_name, str) or not 1 <= len(controller_name) <= 128 or \
                any(ord(character) < 32 for character in controller_name):
            raise ValueError("invalid installation proposal")
        if kind in ("gateway", "service") and (
                self.artifact_installer is None or
                not self.artifact_installer.contains(kind, artifact)):
            raise ValueError("artifact is not in the built-in signed registry")
        with self.lock:
            if len(self._prune()) >= MAX_PROPOSALS:
                raise OverflowError("installation proposal queue is full")
            proposal_id = uuid.uuid4().hex
            capability = secrets.token_urlsafe(32)
            now = time.time()
            timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now))
            value = {
                "id": proposal_id, "controller_id": controller_id,
                "controller_name": controller_name, "kind": kind,
                "artifact_id": artifact, "status": "awaiting_approval",
                "created_at": timestamp, "updated_at": timestamp,
                "expires_at": now + self.ttl_seconds,
                "capability_hash": hashlib.sha256(capability.encode("utf-8")).hexdigest(),
                "error": None,
            }
            atomic_json_write(os.path.join(
                self.directory, "proposal-" + proposal_id + ".json"), value)
        response = self._public(value)
        response["capability_secret"] = capability
        return response

    def get(self, proposal_id, capability):
        if not re.fullmatch(r"[0-9a-f]{32}", proposal_id or ""):
            return None
        with self.lock:
            self._prune()
            path = os.path.join(self.directory, "proposal-" + proposal_id + ".json")
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    value = json.load(handle)
            except (OSError, ValueError):
                return None
            supplied = hashlib.sha256((capability or "").encode("utf-8")).hexdigest()
            if not hmac.compare_digest(supplied, value.get("capability_hash", "")):
                raise PermissionError("invalid installation capability")
            return self._public(value)

    def approve_and_install(self, proposal_id):
        if self.artifact_installer is None:
            raise ValueError("artifact installer is disabled")
        path = os.path.join(self.directory, "proposal-" + proposal_id + ".json")

        def transition(status, error=None):
            with self.lock:
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        value = json.load(handle)
                except (OSError, ValueError):
                    raise FileNotFoundError(proposal_id)
                value["status"] = status
                value["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                value["error"] = error
                value.setdefault("stages", []).append({"stage": status,
                                                        "at": value["updated_at"]})
                atomic_json_write(path, value)
                return value

        with self.lock:
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    proposal = json.load(handle)
            except (OSError, ValueError):
                raise FileNotFoundError(proposal_id)
            if proposal.get("status") != "awaiting_approval":
                raise ValueError("proposal is not awaiting approval")
            if proposal.get("kind") not in ("gateway", "service"):
                raise ValueError("this artifact kind uses its dedicated model installer")
        try:
            result = self.artifact_installer.install(proposal, transition)
            return self._public(result)
        except Exception as exc:
            # 错误仅报告事务阶段和类型，不泄露本地路径。
            try:
                # 只回传事务阶段与异常类型；str(exc) 常含本机绝对路径，
                # 会经状态查询接口泄露给远端 controller。
                value = transition("failed", "{}: {}".format(
                    getattr(exc, "stage", "install"), type(exc).__name__))
            except FileNotFoundError:
                raise
            return self._public(value)

    def list_local(self):
        with self.lock:
            self._prune()
            values = []
            for path in self._paths():
                try:
                    with open(path, "r", encoding="utf-8") as handle:
                        values.append(self._public(json.load(handle)))
                except (OSError, ValueError, AttributeError):
                    continue
            return sorted(values, key=lambda value: value.get("created_at", ""))


class ArtifactInstallError(Exception):
    def __init__(self, stage, message):
        super().__init__(message)
        self.stage = stage


class SignedArtifactInstaller:
    """只安装本地内置注册表中的版本化制品，远端无法提供任何路径。"""

    def __init__(self, registry_path, signing_key_path, install_root):
        self.registry_path = os.path.realpath(registry_path)
        self.registry_root = os.path.dirname(self.registry_path)
        self.install_root = os.path.realpath(install_root)
        key_info = os.lstat(signing_key_path)
        if not stat.S_ISREG(key_info.st_mode) or stat.S_IMODE(key_info.st_mode) & 0o077:
            raise ValueError("artifact signing key must be a private regular file")
        with open(signing_key_path, "rb") as handle:
            self.signing_key = handle.read().strip()
        if len(self.signing_key) < 32:
            raise ValueError("artifact signing key is invalid")
        with open(self.registry_path, "r", encoding="utf-8") as handle:
            registry = json.load(handle)
        artifacts = registry.get("artifacts") if isinstance(registry, dict) else None
        if not isinstance(artifacts, list):
            raise ValueError("artifact registry is invalid")
        self.artifacts = {}
        for item in artifacts:
            self._validate_entry(item)
            key = (item["kind"], item["id"])
            if key in self.artifacts:
                raise ValueError("duplicate artifact registry entry")
            self.artifacts[key] = dict(item)
        os.makedirs(self.install_root, mode=0o700, exist_ok=True)
        os.chmod(self.install_root, 0o700)
        self.lock = threading.RLock()

    @staticmethod
    def signed_payload(item):
        return json.dumps({key: item[key] for key in
                           ("id", "kind", "version", "sha256", "bundle")},
                          sort_keys=True, separators=(",", ":")).encode("utf-8")

    def _validate_entry(self, item):
        if not isinstance(item, dict) or item.get("builtin") is not True or \
                item.get("kind") not in ("gateway", "service") or \
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", item.get("id", "")) or \
                not re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z_.+-]{0,63}", item.get("version", "")) or \
                not re.fullmatch(r"[0-9a-f]{64}", item.get("sha256", "")) or \
                not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", item.get("bundle", "")) or \
                not re.fullmatch(r"[0-9a-f]{64}", item.get("signature", "")):
            raise ValueError("artifact registry entry is invalid")
        expected = hmac.new(self.signing_key, self.signed_payload(item), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, item["signature"]):
            raise ValueError("artifact registry signature is invalid")

    def contains(self, kind, artifact_id):
        return (kind, artifact_id) in self.artifacts

    @staticmethod
    def _sha256(path):
        digest = hashlib.sha256()
        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def install(self, proposal, transition):
        item = self.artifacts.get((proposal.get("kind"), proposal.get("artifact_id")))
        if item is None:
            raise ArtifactInstallError("registry", "artifact is not registered")
        source = os.path.realpath(os.path.join(self.registry_root, "artifacts", item["bundle"]))
        artifact_root = os.path.realpath(os.path.join(self.registry_root, "artifacts"))
        if os.path.commonpath((source, artifact_root)) != artifact_root or not os.path.isfile(source):
            raise ArtifactInstallError("staging", "built-in artifact payload is missing")
        safe_id = item["kind"] + "-" + item["id"]
        target_root = os.path.join(self.install_root, safe_id)
        staging_root = os.path.join(self.install_root, ".staging-" + uuid.uuid4().hex)
        staged = os.path.join(staging_root, "payload")
        current = os.path.join(target_root, "current.json")
        previous = None
        current_changed = False
        with self.lock:
            try:
                transition("staging")
                os.makedirs(staging_root, mode=0o700)
                shutil.copyfile(source, staged)
                os.chmod(staged, 0o700 if os.access(source, os.X_OK) else 0o600)
                transition("verifying")
                if not hmac.compare_digest(self._sha256(staged), item["sha256"]):
                    raise ArtifactInstallError("verify", "artifact SHA-256 mismatch")
                # 再次校验注册表签名，避免实例创建后条目被篡改。
                self._validate_entry(item)
                transition("activating")
                os.makedirs(target_root, mode=0o700, exist_ok=True)
                version_dir = os.path.join(target_root, item["version"])
                if not os.path.exists(version_dir):
                    os.replace(staging_root, version_dir)
                    staging_root = None
                elif not hmac.compare_digest(
                        self._sha256(os.path.join(version_dir, "payload")), item["sha256"]):
                    raise ArtifactInstallError("activate", "installed version payload is corrupted")
                if os.path.exists(current):
                    with open(current, "rb") as handle:
                        previous = handle.read()
                atomic_json_write(current, {"version": item["version"],
                                            "payload": "payload", "sha256": item["sha256"]})
                current_changed = True
                return transition("completed")
            except Exception:
                try:
                    transition("rolling_back")
                except (OSError, ValueError, FileNotFoundError):
                    pass
                if previous is not None:
                    temporary = current + ".rollback-" + uuid.uuid4().hex
                    with open(temporary, "wb") as handle:
                        handle.write(previous)
                        handle.flush()
                        os.fsync(handle.fileno())
                    os.chmod(temporary, 0o600)
                    os.replace(temporary, current)
                elif current_changed:
                    try:
                        os.unlink(current)
                    except FileNotFoundError:
                        pass
                raise
            finally:
                if staging_root:
                    shutil.rmtree(staging_root, ignore_errors=True)


class InstallerHandler(BaseHTTPRequestHandler):
    server_version = "AiguruInstaller/1"

    def _json(self, status, value):
        data = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _error(self, status, message):
        self._json(status, {"error": {"message": message, "type": "installer_error"}})

    def _private_client(self):
        try:
            address = ipaddress.ip_address(self.client_address[0])
            if isinstance(address, ipaddress.IPv6Address) and address.ipv4_mapped:
                address = address.ipv4_mapped
            return address.is_private or address.is_loopback or address.is_link_local
        except ValueError:
            return False

    def do_GET(self):
        path = self.path.split("?", 1)[0]
        if path == "/health":
            self._json(200, {"status": "ok"})
            return
        if path == "/v1/capabilities":
            kinds = ["model"]
            if self.server.store.artifact_installer is not None:
                kinds.extend(["gateway", "service"])
            self._json(200, {"protocol": "approval-v1", "installation_kinds": kinds,
                "requires_remote_approval": True,
                "accepts_urls": False, "accepts_paths": False, "executes_commands": False})
            return
        if path == PROPOSAL_PREFIX:
            if self.client_address[0] not in ("127.0.0.1", "::1"):
                self._error(403, "proposal listing is local-only")
                return
            self._json(200, {"object": "list", "data": self.server.store.list_local()})
            return
        match = re.fullmatch(PROPOSAL_PREFIX + r"/([0-9a-f]{32})", path)
        if match:
            try:
                value = self.server.store.get(
                    match.group(1), self.headers.get("X-Aiguru-Install-Capability", ""))
            except PermissionError:
                self._error(401, "missing or invalid installation capability")
                return
            if value is None:
                self._error(404, "installation proposal not found")
                return
            self._json(200, value)
            return
        self._error(404, "unsupported installer route")

    def do_POST(self):
        path = self.path.split("?", 1)[0]
        approval = re.fullmatch(PROPOSAL_PREFIX + r"/([0-9a-f]{32})/approve", path)
        if approval:
            if self.client_address[0] not in ("127.0.0.1", "::1"):
                self._error(403, "artifact approval is local-only")
                return
            # 回环来源不足以阻止恶意网页发起跨站请求触发本机审批；
            # 带 Origin 时只允许本机页面，无 Origin 的本地 App 请求保持兼容。
            origin = self.headers.get("Origin", "")
            if origin and not re.fullmatch(
                    r"https?://(?:127\.0\.0\.1|localhost|\[::1\])(?::\d{1,5})?", origin):
                self._error(403, "approval origin is not allowed")
                return
            try:
                value = self.server.store.approve_and_install(approval.group(1))
            except FileNotFoundError:
                self._error(404, "installation proposal not found")
                return
            except ValueError as exc:
                self._error(409, str(exc))
                return
            self._json(200 if value.get("status") == "completed" else 422, value)
            return
        if path != PROPOSAL_PREFIX:
            self._error(404, "unsupported installer route")
            return
        if not self._private_client():
            self._error(403, "installation proposals require a private network")
            return
        # 提案创建是配对前的引导接口，保留无令牌设计，但严格限速，避免局域网设备
        # 伪造大量 controller 请求填满队列或骚扰本机用户。
        now = time.time()
        client = self.client_address[0]
        with self.server.rate_limit_lock:
            if len(self.server.proposal_attempts) > 1024:
                self.server.proposal_attempts = {
                    key: values for key, values in self.server.proposal_attempts.items()
                    if values and now - values[-1] < 60
                }
            attempts = [stamp for stamp in self.server.proposal_attempts.get(client, [])
                        if now - stamp < 60]
            if len(attempts) >= 10:
                self._error(429, "too many installation proposals")
                return
            attempts.append(now)
            self.server.proposal_attempts[client] = attempts
        try:
            length = int(self.headers.get("Content-Length", "0") or 0)
        except ValueError:
            self._error(400, "invalid Content-Length")
            return
        if length < 0 or length > MAX_BODY_BYTES:
            self._error(413, "request body is too large")
            return
        try:
            payload = json.loads(self.rfile.read(length) if length else b"{}")
            value = self.server.store.create(payload)
        except ValueError as exc:
            self._error(400, str(exc))
            return
        except OverflowError as exc:
            self._error(429, str(exc))
            return
        self._json(202, value)

    def do_PUT(self):
        self._error(405, "method not allowed")

    do_PATCH = do_PUT
    do_DELETE = do_PUT

    def log_message(self, *_):
        pass


class InstallerServer(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 64

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.rate_limit_lock = threading.RLock()
        self.proposal_attempts = {}


def main():
    parser = argparse.ArgumentParser(description="Aiguru LAN installer approval agent")
    parser.add_argument("--bind", choices=("127.0.0.1", "0.0.0.0"), default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--data-dir", required=True)
    parser.add_argument("--proposal-ttl", type=int, default=PROPOSAL_TTL_SECONDS)
    parser.add_argument("--artifact-registry")
    parser.add_argument("--artifact-signing-key-file")
    parser.add_argument("--install-root")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535:
        parser.error("--port must be between 1 and 65535")
    artifact_options = (args.artifact_registry, args.artifact_signing_key_file, args.install_root)
    if any(artifact_options) and not all(artifact_options):
        parser.error("artifact registry, signing key and install root must be configured together")
    artifact_installer = SignedArtifactInstaller(*artifact_options) if all(artifact_options) else None
    server = InstallerServer((args.bind, args.port), InstallerHandler)
    server.store = ProposalStore(args.data_dir, args.proposal_ttl, artifact_installer)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
