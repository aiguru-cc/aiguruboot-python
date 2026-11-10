from __future__ import annotations
import os, shutil, sys
from pathlib import Path
from .runtime import data_dir
from .vllm_env import detect_environment

MIGRATED = {"gateway": True, "installer_agent": True, "virtual_api": True,
            "configuration": True, "credentials": True, "operation_log": True,
            "engine_lifecycle": True, "vllm_runtime": True, "hardware_info": True,
            "model_authorization": True, "cluster_compute": "partial: node registration and health checks",
            "model_downloader": "partial: artifact download with checksum"}
PENDING = {"fine_tune": "partial: configuration only, training runner absent",
           "lan_discovery": "portable static discovery + HTTP health check",
           "cluster_compute": "partial: node registration and health checks",
           "model_downloader": "partial: no resumable range download",
           "ui_orchestration": "not applicable to headless CLI"}

def linux_preflight() -> dict:
    root=data_dir(); root_ok=True
    try: root.mkdir(parents=True,exist_ok=True); probe=root/".write-test"; probe.write_text("ok"); probe.unlink()
    except OSError: root_ok=False
    return {"platform": sys.platform, "python": sys.version.split()[0],
            "data_dir": str(root), "data_dir_writable": root_ok,
            "gateway_resource": ((Path(__file__).resolve().parents[1]/"Sources/AiguruBootCore/Resources/stats_proxy.py").is_file() or (Path(__file__).resolve().parent/"resources/stats_proxy.py").is_file()),
            "pip": shutil.which("pip") or shutil.which("pip3"), "environment": detect_environment(),
            "migrated": MIGRATED, "pending": PENDING,
            "ready": root_ok and bool(MIGRATED["gateway"])}
