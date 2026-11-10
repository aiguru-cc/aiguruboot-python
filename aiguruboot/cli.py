"""Headless command line entry point.

The CLI deliberately uses only Python's standard library, so it runs on
Windows, macOS and Linux without a graphical session or platform SDK.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import runpy
import sys
from .runtime import JsonConfig, CredentialStore, OperationLog
from .vapi import VapiClient, VapiRuntimeClient, VapiError
from .engine import EngineManager
from .vllm import VLLMConfig, add_vllm_arguments
from .vllm_env import detect_environment, install_command, download_model
from .audit import linux_preflight, MIGRATED, PENDING
from .hardware import inspect
from .authorization import FamilyAuthorization
from .finetune import FineTuneConfig
from .discovery import discover_nodes
from .web import serve
from .capabilities import CAPABILITIES

ROOT = Path(__file__).resolve().parents[1]
RESOURCES = ROOT / "Sources" / "AiguruBootCore" / "Resources"


def _script(name: str) -> str:
    path = RESOURCES / name
    if not path.is_file():
        path = Path(__file__).resolve().parent / "resources" / name
    if not path.is_file():
        raise SystemExit(f"resource not found: {path}")
    return str(path)


def _run_script(name: str, argv: list[str]) -> int:
    old = sys.argv
    sys.argv = [_script(name), *argv]
    try:
        runpy.run_path(sys.argv[0], run_name="__main__")
    except SystemExit as exc:
        return int(exc.code or 0) if isinstance(exc.code, int) else 1
    finally:
        sys.argv = old
    return 0


def _parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="aiguruboot", description="AiguruBoot headless Python runtime")
    p.add_argument("--version", action="version", version="%(prog)s 1.0.0")
    sub = p.add_subparsers(dest="command")
    sub.add_parser("doctor", help="check runtime and resource availability")
    sub.add_parser("status", help="print runtime status as JSON")
    api=sub.add_parser("api", help="list CLI and HTTP capabilities"); api.add_argument("--json",action="store_true")
    sub.add_parser("audit", help="show Swift to Python migration coverage")
    sub.add_parser("preflight", help="check Linux deployment readiness")
    sub.add_parser("hardware", help="inspect CPU, memory and GPU")
    auth=sub.add_parser("family", help="manage model family authorization"); auth.add_argument("action",choices=("status","authorize")); auth.add_argument("family",nargs="?")
    ft=sub.add_parser("finetune",help="validate and print a LoRA fine-tune configuration")
    ft.add_argument("--iters",type=int,default=200); ft.add_argument("--learning-rate",type=float,default=1e-5); ft.add_argument("--batch-size",type=int,default=4); ft.add_argument("--rank",type=int,default=8); ft.add_argument("--max-seq-length",type=int,default=2048); ft.add_argument("--model"); ft.add_argument("--dataset")
    sub.add_parser("discover",help="discover configured cluster nodes and check health")
    web=sub.add_parser("web",help="serve the headless web management panel"); web.add_argument("--host",default="127.0.0.1"); web.add_argument("--port",type=int,default=8080)
    c = sub.add_parser("config", help="read or update persistent configuration")
    c.add_argument("action", choices=("show", "set")); c.add_argument("pairs", nargs="*")
    m = sub.add_parser("models", help="list models from a JSON catalog")
    m.add_argument("--file", type=Path); m.add_argument("--pretty", action="store_true")
    a = sub.add_parser("credentials", help="manage local agent credentials")
    a.add_argument("action", choices=("list", "create", "revoke")); a.add_argument("value", nargs="?")
    l = sub.add_parser("logs", help="read or append operation log"); l.add_argument("action", choices=("show", "append")); l.add_argument("values", nargs="*")
    v = sub.add_parser("vapi", help="manage gateway virtual APIs")
    v.add_argument("action", choices=("list","get","create","update","rotate","revoke","enable","disable","connection","usage"))
    v.add_argument("id", nargs="?"); v.add_argument("--base-url", default=os.environ.get("AIGURUBOOT_GATEWAY_URL", "http://127.0.0.1:11434")); v.add_argument("--token", default=os.environ.get("AIGURUBOOT_CONTROL_TOKEN"))
    v.add_argument("--label"); v.add_argument("--models", help="JSON object of public alias to backend model")
    call = sub.add_parser("vapi-call", help="send an inference request through a virtual API")
    call.add_argument("endpoint", choices=("chat/completions", "responses", "messages")); call.add_argument("--id", required=True); call.add_argument("--api-key", required=True); call.add_argument("--base-url", default="http://127.0.0.1:11434"); call.add_argument("--json", required=True)
    g = sub.add_parser("gateway", help="run the OpenAI-compatible local gateway")
    g.add_argument("args", nargs=argparse.REMAINDER, help="gateway options (see --help)")
    i = sub.add_parser("install-agent", help="run the LAN installation approval agent")
    i.add_argument("args", nargs=argparse.REMAINDER, help="agent options (see --help)")
    e = sub.add_parser("engine", help="manage headless engine processes")
    e.add_argument("action", choices=("status","start","stop")); e.add_argument("name")
    e.add_argument("engine_command", nargs=argparse.REMAINDER)
    vllm = sub.add_parser("vllm", help="start or inspect a vLLM OpenAI server")
    vllm_sub = vllm.add_subparsers(dest="vllm_action", required=True)
    start = vllm_sub.add_parser("start"); add_vllm_arguments(start)
    vllm_sub.add_parser("help", help="show vLLM integration details")
    vllm_sub.add_parser("doctor", help="inspect Python, GPU and vLLM availability")
    install = vllm_sub.add_parser("install", help="install vLLM with pip")
    install.add_argument("--cpu", action="store_true", help="force CPU dependency index")
    download = vllm_sub.add_parser("download", help="download a model artifact")
    download.add_argument("url"); download.add_argument("destination", type=Path); download.add_argument("--sha256")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "gateway":
        return _run_script("stats_proxy.py", args.args)
    if args.command == "install-agent":
        return _run_script("installer_agent.py", args.args)
    if args.command == "engine":
        manager=EngineManager()
        try:
            if args.action == "status": result=manager.status(args.name)
            elif args.action == "stop": result=manager.stop(args.name)
            elif not args.engine_command: print("engine start requires a command",file=sys.stderr); return 2
            else: result=manager.start(args.name,args.engine_command)
            print(json.dumps(result,ensure_ascii=False,indent=2)); return 0
        except (OSError,RuntimeError) as exc: print(str(exc),file=sys.stderr); return 1
    if args.command == "vllm":
        if args.vllm_action == "help":
            print("vllm start MODEL [--port PORT] [--tensor-parallel-size N] [--quantization METHOD]")
            return 0
        if args.vllm_action == "doctor":
            print(json.dumps(detect_environment(), indent=2)); return 0
        if args.vllm_action == "install":
            import subprocess
            return subprocess.call(install_command(False if args.cpu else None))
        if args.vllm_action == "download":
            try: print(download_model(args.url,args.destination,args.sha256)); return 0
            except (OSError, ValueError, RuntimeError) as exc: print(str(exc),file=sys.stderr); return 1
        try:
            cfg=VLLMConfig(args.model,args.host,args.port,args.served_model_name,args.tensor_parallel_size,args.max_model_len,args.quantization,args.api_key,args.extra)
            result=EngineManager().start("vllm",cfg.command()); print(json.dumps(result,indent=2)); return 0
        except (OSError,RuntimeError,ValueError) as exc: print(str(exc),file=sys.stderr); return 1
    if args.command == "doctor":
        checks = {"python": sys.version.split()[0], "platform": sys.platform,
                  "gateway": Path(_script("stats_proxy.py")).is_file(),
                  "install_agent": Path(_script("installer_agent.py")).is_file()}
        print(json.dumps(checks, ensure_ascii=False, indent=2))
        return 0 if all(v for k, v in checks.items() if k not in {"python", "platform"}) else 1
    if args.command == "status":
        print(json.dumps({"version": "1.0.0", "headless": True,
                          "platform": sys.platform}, ensure_ascii=False))
        return 0
    if args.command == "api":
        if args.json: print(json.dumps(CAPABILITIES,ensure_ascii=False,indent=2))
        else:
            for name,item in CAPABILITIES.items(): print(f"{name:12} {item['command']}\n             {item['description']}")
        return 0
    if args.command == "audit":
        print(json.dumps({"migrated":MIGRATED,"pending":PENDING},ensure_ascii=False,indent=2)); return 0
    if args.command == "preflight":
        result=linux_preflight(); print(json.dumps(result,ensure_ascii=False,indent=2)); return 0 if result["ready"] else 1
    if args.command == "hardware": print(json.dumps(inspect(),ensure_ascii=False,indent=2)); return 0
    if args.command == "family":
        store=FamilyAuthorization()
        if args.action=='status': print(json.dumps(store.status(),ensure_ascii=False)); return 0
        try: store.authorize(args.family or ''); return 0
        except ValueError as exc: print(str(exc),file=sys.stderr); return 2
    if args.command == "finetune":
        try:
            cfg=FineTuneConfig(args.iters,args.learning_rate,args.batch_size,4,args.rank,args.max_seq_length)
            print(json.dumps({'config':json.loads(cfg.to_json()),'command':cfg.command(args.model,args.dataset)} if args.model and args.dataset else json.loads(cfg.to_json()),ensure_ascii=False,indent=2)); return 0
        except ValueError as exc: print(str(exc),file=sys.stderr); return 2
    if args.command == "discover": print(json.dumps(discover_nodes(),ensure_ascii=False,indent=2)); return 0
    if args.command == "web": serve(args.host,args.port); return 0
    if args.command == "config":
        cfg=JsonConfig()
        if args.action == "show": print(json.dumps(cfg.load(), ensure_ascii=False, indent=2)); return 0
        values={k:v for k,v in (x.split("=",1) for x in args.pairs if "=" in x)}; print(json.dumps(cfg.update(values),ensure_ascii=False,indent=2)); return 0
    if args.command == "models":
        path=args.file or (RESOURCES/"configs"/"models.json")
        try:
            value=json.loads(path.read_text(encoding="utf-8"))
            if args.pretty and isinstance(value,list):
                for item in value:
                    size=float(item.get('sizeBytes',0))/1024**3; ram=item.get('minRAMGB','?'); vram=item.get('minVRAMGB',ram)
                    print(f"{item.get('name','unknown')} | 内存 {ram} GB | 显存 {vram} GB | 文件约 {size:.1f} GB | {item.get('backend','')}")
            else: print(json.dumps(value,ensure_ascii=False,indent=2))
            return 0
        except (OSError,ValueError) as exc: print(f"cannot read catalog: {exc}", file=sys.stderr); return 1
    if args.command == "credentials":
        store=CredentialStore()
        if args.action == "list": print(json.dumps(store.list(),ensure_ascii=False,indent=2)); return 0
        if args.action == "create": print(json.dumps(store.create(args.value or "Agent"),ensure_ascii=False,indent=2)); return 0
        return 0 if store.revoke(args.value or "") else 1
    if args.command == "logs":
        log=OperationLog()
        if args.action == "show": print(json.dumps(log.read(),ensure_ascii=False,indent=2)); return 0
        if len(args.values)<3: print("logs append ACTION TARGET RESULT [DETAIL]",file=sys.stderr); return 2
        log.append(*args.values[:4]); return 0
    if args.command == "vapi":
        client=VapiClient(args.base_url,args.token)
        try:
            if args.action not in ("list", "create") and not args.id:
                print("this action requires a virtual API id", file=sys.stderr); return 2
            if args.action == "list": result=client.list()
            elif args.action == "get": result=client.get(args.id)
            elif args.action == "create": result=client.create(args.label or "Agent", json.loads(args.models) if args.models else None)
            elif args.action == "update": result=client.update(args.id, label=args.label) if args.label else client.update(args.id)
            else: result=getattr(client,args.action)(args.id)
            print(json.dumps(result,ensure_ascii=False,indent=2)); return 0
        except (VapiError, ValueError) as exc: print(str(exc),file=sys.stderr); return 1
    if args.command == "vapi-call":
        try:
            result=VapiRuntimeClient(args.base_url,args.id,args.api_key).call(args.endpoint,json.loads(args.json))
            print(json.dumps(result,ensure_ascii=False,indent=2)); return 0
        except (VapiError, ValueError) as exc: print(str(exc),file=sys.stderr); return 1
    _parser().print_help()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
