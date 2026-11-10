#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="${AIGURUBOOT_DATA_DIR:-$HOME/.local/share/aiguruboot}"
VENV="$DATA_DIR/venv"
PY="$VENV/bin/python"
mkdir -p "$DATA_DIR"
if [[ ! -x "$PY" ]]; then python3 -m venv "$VENV"; "$PY" -m pip install --no-deps -e "$ROOT" >/dev/null || { rm -rf "$VENV"; PY="$(command -v python3)"; }; fi
cli(){ AIGURUBOOT_DATA_DIR="$DATA_DIR" "$PY" -m aiguruboot "$@"; }
choose_model(){
  echo "可用模型启动命令示例"
  echo "1) vLLM 模型 ID"
  echo "2) 本地引擎命令"
  read -r -p "选择 [1-2]: " n
  read -r -p "模型或命令: " value
  printf '%s\n' "$value" > "$DATA_DIR/selected-model"
  echo "已保存模型: $value"
}
menu(){
  while true; do
    echo; echo "AiguruBoot Linux 管理"
    echo "1) 选择安装模型  2) 启动模型  3) 停止模型  4) 设置网关"
    echo "5) 强制重启  6) 启动 Web  7) 帮助  8) 日志  0) 退出"
    read -r -p "> " action
    case "$action" in
      1) choose_model;;
      2) model="$(cat "$DATA_DIR/selected-model" 2>/dev/null || echo "local")"; cli vllm start "$model";;
      3) cli engine stop vllm || true;;
      4) read -r -p "网关地址 [http://127.0.0.1:11434]: " url; export AIGURUBOOT_GATEWAY_URL="${url:-http://127.0.0.1:11434}"; echo "$AIGURUBOOT_GATEWAY_URL" > "$DATA_DIR/gateway-url";;
      5) cli engine stop vllm || true; model="$(cat "$DATA_DIR/selected-model" 2>/dev/null || echo "local")"; cli vllm start "$model";;
      6) cli web --host 0.0.0.0 --port "${AIGURUBOOT_WEB_PORT:-8080}";;
      7) cli api;;
      8) cli logs show;;
      0) exit 0;;
      *) echo "无效选项";;
    esac
  done
}
case "${1:-menu}" in
  install) echo "安装完成: $PY";;
  model) choose_model;; start) model="${2:-$(cat "$DATA_DIR/selected-model" 2>/dev/null || echo local)}"; cli vllm start "$model";;
  stop) cli engine stop "${2:-vllm}";; restart|force-restart) cli engine stop vllm || true; exec "$0" start;;
  gateway) export AIGURUBOOT_GATEWAY_URL="${2:-http://127.0.0.1:11434}"; echo "$AIGURUBOOT_GATEWAY_URL" > "$DATA_DIR/gateway-url";;
  web) cli web --host "${2:-0.0.0.0}" --port "${3:-8080}";; help) cli api;; logs) cli logs show;; menu) menu;; *) echo "用法: $0 [install|model|start|stop|restart|gateway|web|help|logs]"; exit 2;;
esac
