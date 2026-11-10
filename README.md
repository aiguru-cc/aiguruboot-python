# AiguruBoot（纯 Python、跨平台、无图形界面）

项目现在提供标准 Python 包和 `aiguruboot` CLI。运行时只依赖 Python 3.9+ 标准库，
可在 Windows、macOS、Linux 的服务器、容器或 SSH 会话中运行，不需要 Swift、Xcode 或图形窗口。

```bash
python -m aiguruboot doctor
python -m aiguruboot status
python -m aiguruboot gateway --help
python -m aiguruboot install-agent --help
```

网关和安装代理继续复用经过验证的 Python 实现；CLI 负责统一入口、状态检查和跨平台进程启动。
安装为命令后也可直接使用 `aiguruboot gateway ...`。

Linux 一键管理脚本：

```bash
chmod +x aiguruboot/scripts/aiguruboot-linux.sh
./aiguruboot/scripts/aiguruboot-linux.sh install
./aiguruboot/scripts/aiguruboot-linux.sh
```

脚本菜单支持模型选择、启动、停止、网关设置、强制重启、Web、帮助和日志。也支持直接命令：
`start`、`stop`、`restart`、`gateway`、`web`、`systemd`、`help`、`logs`。
服务运维命令还包括 `uninstall-systemd` 和 `systemd-status`。Web 端口会保存到
`$AIGURUBOOT_DATA_DIR/web-port`，安装或注册 systemd 时自动复用。

查看 systemd 日志和完整卸载：

```bash
./aiguruboot/scripts/aiguruboot-linux.sh systemd-logs 200
./aiguruboot/scripts/aiguruboot-linux.sh uninstall-systemd
./aiguruboot/scripts/aiguruboot-linux.sh uninstall
```

真实 Linux 测试建议在 Ubuntu 22.04+ 或 Debian 12+ 执行：

```bash
sudo apt install -y python3 python3-venv curl systemd
chmod +x aiguruboot/scripts/aiguruboot-linux.sh
./aiguruboot/scripts/aiguruboot-linux.sh install
./aiguruboot/scripts/aiguruboot-linux.sh systemd-status
./aiguruboot/scripts/aiguruboot-linux.sh systemd-logs 50
curl http://127.0.0.1:8080/api/preflight
```

模型规格列表：

```bash
./aiguruboot/scripts/aiguruboot-linux.sh model-list
```

输出模型名称、最低内存、建议显存、文件大小和后端。真实 Linux 流程建议依次执行安装、
模型列表、启动、状态、停止和 Web 自检：

```bash
./aiguruboot/scripts/aiguruboot-linux.sh install
./aiguruboot/scripts/aiguruboot-linux.sh model-list
./aiguruboot/scripts/aiguruboot-linux.sh start Qwen/Qwen3-8B
./aiguruboot/scripts/aiguruboot-linux.sh systemd-status
./aiguruboot/scripts/aiguruboot-linux.sh stop
./aiguruboot/scripts/aiguruboot-linux.sh web 127.0.0.1 8080 &
curl -fsS http://127.0.0.1:8080/api/preflight
```

安装后注册用户级 systemd 服务：

```bash
AIGURUBOOT_SYSTEMD=1 AIGURUBOOT_WEB_PORT=8090 ./aiguruboot/scripts/aiguruboot-linux.sh install
./aiguruboot/scripts/aiguruboot-linux.sh systemd
```

迁移审计和 Linux 实测前检查可使用：

```bash
python3 -m aiguruboot audit
python3 -m aiguruboot preflight

# 查看全部 CLI 能力和示例
python3 -m aiguruboot api
python3 -m aiguruboot api --json
```

`preflight` 会验证数据目录可写、Python/pip、网关资源和 GPU/CUDA 探测结果。

macOS 专用能力采用跨平台替代方案。硬件检测使用 Python 标准库，集群发现使用
`AIGURUBOOT_NODES` 和 HTTP 健康检查，持久化使用平台数据目录，服务进程使用标准
`subprocess` 和 PID 文件。Linux 不需要 Xcode、Swift、Bonjour 或图形会话。
