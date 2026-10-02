#!/usr/bin/env bash
# 把 HPC Job Dashboard 装成 systemd 服务（开机自启、挂了自动重启）。
#
# 用法：
#   ./install.sh                     # 安装 / 更新并启动
#   PYTHON=/path/to/python ./install.sh   # 指定 python
#   ./install.sh --uninstall         # 停止并删除服务
#
# 需要 sudo（写 /etc/systemd/system）。服务以运行脚本的那个用户身份跑，
# 用的是这个用户的 ~/.ssh/config 和 key。注意：系统服务拿不到桌面会话里的
# ssh-agent，所以 key 不能设密码（或者自己改成用 ControlMaster 之类）。
set -euo pipefail

SERVICE=hpc-dashboard
UNIT=/etc/systemd/system/${SERVICE}.service
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEFAULT_PYTHON=/home/dx/intel/oneapi/intelpython/latest/envs/pymatgen250508/bin/python

if [ "${EUID}" -ne 0 ]; then
  exec sudo PYTHON="${PYTHON:-}" bash "$0" "$@"
fi
RUN_USER="${SUDO_USER:-root}"
if [ "${RUN_USER}" = "root" ]; then
  echo "请用普通用户运行 ./install.sh（脚本会自己 sudo），服务要用你的 ssh 配置。" >&2
  exit 1
fi

if [ "${1:-}" = "--uninstall" ]; then
  systemctl disable --now "${SERVICE}" 2>/dev/null || true
  rm -f "${UNIT}"
  systemctl daemon-reload
  echo "已删除 ${SERVICE} 服务。"
  exit 0
fi

PYTHON="${PYTHON:-}"
if [ -z "${PYTHON}" ]; then
  if [ -x "${DEFAULT_PYTHON}" ]; then PYTHON="${DEFAULT_PYTHON}"; else PYTHON="$(command -v python3)"; fi
fi
if ! sudo -u "${RUN_USER}" "${PYTHON}" -c "import flask, yaml" 2>/dev/null; then
  echo "${PYTHON} 里缺 flask / pyyaml，先装一下：" >&2
  echo "  ${PYTHON} -m pip install -r ${APP_DIR}/requirements.txt" >&2
  exit 1
fi
chmod +x "${APP_DIR}/ssh_askpass_abort.sh"

cat > "${UNIT}" <<UNIT_EOF
[Unit]
Description=HPC Job Dashboard (${APP_DIR})
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_USER}
WorkingDirectory=${APP_DIR}
ExecStart=${PYTHON} ${APP_DIR}/app.py
Environment=PYTHONUNBUFFERED=1
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
UNIT_EOF

systemctl daemon-reload
systemctl enable "${SERVICE}" >/dev/null
systemctl restart "${SERVICE}"
sleep 2
systemctl --no-pager --lines=5 status "${SERVICE}" || true

PORT="$(sed -n 's/^port:[[:space:]]*\([0-9]*\).*/\1/p' "${APP_DIR}/config.yaml" | head -n1)"
echo
echo "装好了：用户 ${RUN_USER}，python ${PYTHON}"
echo "网页：http://127.0.0.1:${PORT:-8787}"
echo "看日志：journalctl -u ${SERVICE} -f"
echo "改了代码或 config.yaml 之后：sudo systemctl restart ${SERVICE}"
