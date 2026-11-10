#!/usr/bin/env bash
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$SCRIPT_DIR"
for candidate in "$SCRIPT_DIR/../.." "$SCRIPT_DIR/.." "$SCRIPT_DIR" "$HOME/aiguruboot-python"; do
  if [[ -f "$candidate/pyproject.toml" && -d "$candidate/aiguruboot" ]]; then ROOT="$(cd "$candidate" && pwd)"; break; fi
done
DATA_DIR="${AIGURUBOOT_DATA_DIR:-$HOME/.local/share/aiguruboot}"
VENV="$DATA_DIR/venv"
PY="$VENV/bin/python"
mkdir -p "$DATA_DIR"
check_deps(){ command -v python3 >/dev/null || { echo '缺少 python3'; return 1; }; python3 -m venv --help >/dev/null 2>&1 || { echo '缺少 python3-venv'; return 1; }; }
install_runtime(){
  check_deps; [[ -x "$PY" ]] && return
  if [[ ! -f "$ROOT/pyproject.toml" ]]; then
    echo "错误: 未找到 Python 项目根目录。请从包含 pyproject.toml 和 aiguruboot/ 的仓库运行此脚本。" >&2
    return 1
  fi
  tmp="$DATA_DIR/.install-backup"; rm -rf "$tmp"; mkdir -p "$tmp"
  trap 'rm -rf "$tmp"' ERR; python3 -m venv "$VENV"
  if ! "$PY" -m pip install --no-build-isolation --no-deps -e "$ROOT" >/dev/null; then
    rm -rf "$VENV"; PY="$(command -v python3)"
    export PYTHONPATH="$ROOT${PYTHONPATH:+:$PYTHONPATH}"
    echo "提示: 本地安装失败，已回退到仓库源码运行。" >&2
  fi
  trap - ERR
}
install_runtime
cli(){ AIGURUBOOT_DATA_DIR="$DATA_DIR" "$PY" -m aiguruboot "$@"; }
blue='\033[1;34m'; green='\033[1;32m'; yellow='\033[1;33m'; reset='\033[0m'
title(){ clear 2>/dev/null || true; echo -e "${blue}╔══════════════════════════════════════╗${reset}"; echo -e "${blue}║        AiguruBoot Linux 控制台        ║${reset}"; echo -e "${blue}╚══════════════════════════════════════╝${reset}"; }
pause(){ read -r -p '按回车继续...' _; }
model_menu(){ while true; do title; echo '模型管理'; echo '1) 选择模型'; echo '2) 下载模型'; echo '3) 查看模型目录'; echo '0) 返回'; read -r -p '> ' n; case $n in 1) choose_model; pause;; 2) read -r -p 'URL: ' u; read -r -p '目标文件: ' d; "$0" download "$u" "$d"; pause;; 3) cli models; pause;; 0) return;; esac; done; }
engine_menu(){ while true; do title; echo '引擎管理'; echo '1) 启动模型'; echo '2) 停止模型'; echo '3) 强制重启'; echo '4) 查看状态'; echo '0) 返回'; read -r -p '> ' n; case $n in 1) model="$(cat "$DATA_DIR/selected-model" 2>/dev/null || echo local)"; cli vllm start "$model"; pause;; 2) cli engine stop vllm || true; pause;; 3) cli engine stop vllm || true; model="$(cat "$DATA_DIR/selected-model" 2>/dev/null || echo local)"; cli vllm start "$model"; pause;; 4) cli engine status vllm; pause;; 0) return;; esac; done; }
system_menu(){ while true; do title; echo '系统服务'; echo '1) 设置网关'; echo '2) 启动 Web'; echo '3) 注册 systemd'; echo '4) systemd 状态'; echo '5) 卸载 systemd'; echo '6) 环境自检'; echo '7) 查看日志'; echo '0) 返回'; read -r -p '> ' n; case $n in 1) read -r -p '网关地址: ' url; export AIGURUBOOT_GATEWAY_URL="${url:-http://127.0.0.1:11434}"; echo "$AIGURUBOOT_GATEWAY_URL" > "$DATA_DIR/gateway-url"; pause;; 2) read -r -p "Web 端口 [${AIGURUBOOT_WEB_PORT:-8080}]: " p; "$0" web 0.0.0.0 "${p:-8080}";; 3) "$0" systemd; pause;; 4) "$0" systemd-status; pause;; 5) "$0" uninstall-systemd; pause;; 6) selfcheck; pause;; 7) cli logs show; pause;; 0) return;; esac; done; }
install_systemd(){
  command -v systemctl >/dev/null || { echo 'systemd 不可用'; return 1; }
  unit="$DATA_DIR/aiguruboot-web.service"; port="${AIGURUBOOT_WEB_PORT:-8080}"
  cat > "$unit" <<EOF
[Unit]
Description=AiguruBoot Web Management
After=network.target
[Service]
Type=simple
Environment=AIGURUBOOT_DATA_DIR=$DATA_DIR
ExecStart=$PY -m aiguruboot web --host 0.0.0.0 --port $port
Restart=on-failure
[Install]
WantedBy=default.target
EOF
  mkdir -p "$HOME/.config/systemd/user"; cp "$unit" "$HOME/.config/systemd/user/aiguruboot-web.service"
  systemctl --user daemon-reload; systemctl --user enable --now aiguruboot-web.service
}
remove_systemd(){
  command -v systemctl >/dev/null || { echo 'systemd 不可用'; return 1; }
  systemctl --user disable --now aiguruboot-web.service 2>/dev/null || true
  rm -f "$HOME/.config/systemd/user/aiguruboot-web.service" "$DATA_DIR/aiguruboot-web.service"
  systemctl --user daemon-reload
  echo 'systemd 服务已卸载'
}
systemd_status(){
  if command -v systemctl >/dev/null; then systemctl --user status aiguruboot-web.service --no-pager || true; else echo 'systemd 不可用'; fi
  echo "Web 端口: $(cat "$DATA_DIR/web-port" 2>/dev/null || echo "${AIGURUBOOT_WEB_PORT:-8080}")"
}
systemd_logs(){ command -v journalctl >/dev/null || return 1; journalctl --user -u aiguruboot-web.service -n "${1:-100}" --no-pager; }
uninstall_all(){ remove_systemd 2>/dev/null || true; cli engine stop vllm 2>/dev/null || true; read -r -p "删除数据目录 $DATA_DIR? [y/N] " answer; [[ "$answer" =~ ^[Yy]$ ]] && rm -rf "$DATA_DIR"; }
selfcheck(){ cli doctor >/dev/null; cli preflight || true; cli hardware >/dev/null; echo "安装自检完成，Web 端口 ${AIGURUBOOT_WEB_PORT:-8080}"; }
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
    title; echo -e "${green}数据目录${reset}: $DATA_DIR"; echo -e "${green}模型${reset}: $(cat "$DATA_DIR/selected-model" 2>/dev/null || echo '未选择')"; echo
    echo '1) 模型管理'; echo '2) 引擎管理'; echo '3) 系统服务'; echo '4) 帮助'; echo '0) 退出'
    read -r -p "> " action
    case "$action" in
      1) model_menu;; 2) engine_menu;; 3) system_menu;; 4) cli api; pause;;
      0) exit 0;;
      *) echo "无效选项";;
    esac
  done
}
case "${1:-menu}" in
  install) echo "安装完成: $PY"; selfcheck; [[ "${AIGURUBOOT_SYSTEMD:-0}" == 1 ]] && install_systemd;;
  download) command -v curl >/dev/null || { echo '缺少 curl'; exit 1; }; [[ -n "${2:-}" && -n "${3:-}" ]] || { echo "用法: $0 download URL DEST"; exit 2; }; mkdir -p "$(dirname "$3")"; curl -fL --retry 3 --progress-bar -C - "$2" -o "$3";;
  model) choose_model;; model-list) cli models --pretty;; start) model="${2:-$(cat "$DATA_DIR/selected-model" 2>/dev/null || echo local)}"; cli vllm start "$model";;
  stop) cli engine stop "${2:-vllm}";; restart|force-restart) cli engine stop vllm || true; exec "$0" start;;
  gateway) export AIGURUBOOT_GATEWAY_URL="${2:-http://127.0.0.1:11434}"; echo "$AIGURUBOOT_GATEWAY_URL" > "$DATA_DIR/gateway-url";;
  web) export AIGURUBOOT_WEB_PORT="${3:-${AIGURUBOOT_WEB_PORT:-8080}}"; echo "$AIGURUBOOT_WEB_PORT" > "$DATA_DIR/web-port"; cli web --host "${2:-0.0.0.0}" --port "$AIGURUBOOT_WEB_PORT";;
  systemd) install_systemd;; uninstall-systemd) remove_systemd;; uninstall) uninstall_all;; systemd-status) systemd_status;; systemd-logs) systemd_logs "${2:-100}";; help) cli api;; logs) cli logs show;; menu) menu;; *) echo "用法: $0 [install|model|model-list|start|stop|restart|gateway|web|systemd|systemd-status|systemd-logs|uninstall-systemd|uninstall|help|logs]"; exit 2;;
esac
