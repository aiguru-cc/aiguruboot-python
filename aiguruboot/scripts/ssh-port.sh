#!/usr/bin/env bash
#
# change_ssh_port.sh - Ubuntu 24.04 一键修改 SSH 端口
# 用法: sudo ./change_ssh_port.sh <新端口>
# 示例: sudo ./change_ssh_port.sh 2222
#

set -euo pipefail

# ---------- 基础检查 ----------
if [[ $EUID -ne 0 ]]; then
    echo "错误: 请使用 sudo 或 root 运行此脚本。" >&2
    exit 1
fi

if [[ $# -ne 1 ]]; then
    echo "用法: $0 <新端口>" >&2
    echo "示例: $0 2222" >&2
    exit 1
fi

NEW_PORT="$1"

# 端口合法性校验
if ! [[ "$NEW_PORT" =~ ^[0-9]+$ ]] || (( NEW_PORT < 1 || NEW_PORT > 65535 )); then
    echo "错误: 端口必须是 1-65535 之间的整数。" >&2
    exit 1
fi

if (( NEW_PORT < 1024 )); then
    echo "警告: 端口 $NEW_PORT 小于 1024，可能与其他系统服务冲突，建议使用 1024 以上端口。"
    read -rp "是否继续? [y/N] " ans
    [[ "$ans" =~ ^[Yy]$ ]] || exit 1
fi

# 检查端口是否被占用
if ss -tlnp 2>/dev/null | grep -qE "[:.]${NEW_PORT}[[:space:]]"; then
    echo "错误: 端口 $NEW_PORT 已被占用，请换一个。" >&2
    ss -tlnp | grep -E "[:.]${NEW_PORT}[[:space:]]" || true
    exit 1
fi

SSHD_CONFIG="/etc/ssh/sshd_config"
BACKUP="${SSHD_CONFIG}.bak.$(date +%Y%m%d%H%M%S)"

# ---------- 备份 ----------
echo "[1/6] 备份原配置到 $BACKUP"
cp -a "$SSHD_CONFIG" "$BACKUP"

# ---------- 修改 sshd_config ----------
echo "[2/6] 修改 $SSHD_CONFIG 中的 Port"
# 删除所有已有的 Port 行（含注释掉的），再追加新 Port
# 用临时文件避免 sed -i 在不同版本上的差异
tmp=$(mktemp)
grep -vE '^[[:space:]]*#?[[:space:]]*Port[[:space:]]' "$SSHD_CONFIG" > "$tmp"
echo "Port $NEW_PORT" >> "$tmp"
cat "$tmp" > "$SSHD_CONFIG"
rm -f "$tmp"

# 语法校验
if ! sshd -t; then
    echo "错误: sshd 配置语法校验失败，正在回滚。" >&2
    cp -a "$BACKUP" "$SSHD_CONFIG"
    exit 1
fi
echo "      配置语法校验通过。"

# ---------- UFW 放行（如启用） ----------
echo "[3/6] 检查 UFW"
if command -v ufw >/dev/null 2>&1 && ufw status | grep -q "Status: active"; then
    echo "      UFW 已启用，放行 $NEW_PORT/tcp"
    ufw allow "${NEW_PORT}/tcp" >/dev/null
    echo "      已放行。确认新端口可连接后，可执行: ufw delete allow 22/tcp"
else
    echo "      UFW 未启用或未安装，跳过。"
fi

# ---------- 处理 socket 激活 ----------
echo "[4/6] 处理 systemd socket 激活"
SOCKET_OVERRIDE_DIR="/etc/systemd/system/ssh.socket.d"
mkdir -p "$SOCKET_OVERRIDE_DIR"

# 如果 ssh.socket 存在且当前监听 22，则需要覆盖 ListenStream
if systemctl list-unit-files | grep -q '^ssh.socket'; then
    cat > "${SOCKET_OVERRIDE_DIR}/override.conf" <<EOF
[Socket]
ListenStream=
ListenStream=${NEW_PORT}
EOF
    echo "      已写入 socket 覆盖配置: ${SOCKET_OVERRIDE_DIR}/override.conf"
else
    echo "      未检测到 ssh.socket，使用传统 ssh.service 模式。"
fi

echo "[5/6] 重新加载 systemd 并重启 SSH"
systemctl daemon-reload

if systemctl list-unit-files | grep -q '^ssh.socket'; then
    systemctl restart ssh.socket
    # 部分场景下 service 也需要重启以应用 Port
    systemctl restart ssh.service 2>/dev/null || true
else
    systemctl restart ssh.service
fi

# ---------- 验证 ----------
echo "[6/6] 验证监听状态"
sleep 1
if ss -tlnp 2>/dev/null | grep -qE "[:.]${NEW_PORT}[[:space:]]"; then
    echo "      ✅ SSH 已在端口 $NEW_PORT 监听。"
else
    echo "      ⚠️  未检测到 $NEW_PORT 监听，请手动检查:" >&2
    ss -tlnp | grep ssh || true
    systemctl status ssh.socket --no-pager || true
    exit 1
fi

cat <<EOF

============================================================
完成！SSH 端口已修改为: $NEW_PORT

⚠️  重要：请勿关闭当前 SSH 会话！
请新开一个终端窗口测试连接：
    ssh <用户名>@<服务器IP> -p $NEW_PORT

测试成功后再关闭旧会话。若连接失败，可回滚：
    cp -a $BACKUP $SSHD_CONFIG
    rm -f ${SOCKET_OVERRIDE_DIR}/override.conf
    systemctl daemon-reload
    systemctl restart ssh.socket

配置文件备份: $BACKUP
============================================================
EOF