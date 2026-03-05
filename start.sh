#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"
mkdir -p .pids
mkdir -p logs

SERVER_LOG_FILE="logs/server.log"
AGENTS_LOG_FILE="logs/agents.log"
SERVER_LOG_LEVEL="${SERVER_LOG_LEVEL:-info}"

now_ts() {
  date "+%Y-%m-%d %H:%M:%S"
}

log_info() {
  printf '[%s] [INFO] %s\n' "$(now_ts)" "$*"
}

log_warn() {
  printf '[%s] [WARN] %s\n' "$(now_ts)" "$*"
}

log_done() {
  printf '[%s] [DONE] %s\n' "$(now_ts)" "$*"
}

prefix_logs() {
  while IFS= read -r line || [ -n "$line" ]; do
    printf '[%s] %s\n' "$(now_ts)" "$line"
  done
}

print_quick_guide() {
  log_info "╔════════════════════════════════════════════════════════════╗"
  log_info "║ Multi-Agent Task Board 已启动                              ║"
  log_info "╚════════════════════════════════════════════════════════════╝"
  log_info "本机访问: ${LOCAL_ACCESS_URL}"
  if [ -n "${LAN_ACCESS_URL}" ]; then
    log_info "局域网访问: ${LAN_ACCESS_URL}"
  else
    log_warn "局域网地址检测失败，可手动用本机 IP + 端口访问（例如 http://192.168.x.x:${SERVER_PORT}）"
  fi
  log_info "监听配置: host=${SERVER_HOST} port=${SERVER_PORT}"
  log_info "Agent CLI 可在页面中设置: Agent 管理 -> 编辑 Agent 类型"
  log_info "快速上手:"
  log_info "  1) 首次访问先设置 admin 密码并登录"
  log_info "  2) 在项目管理里添加本地 Git 仓库路径"
  log_info "  3) 创建任务后观察状态自动流转"
  log_info "运维信息:"
  log_info "  - PID 文件: .pids/server.pid, .pids/agents.pid"
  log_info "  - 接口日志: logs/api-access.log"
  log_info "  - 服务日志: ${SERVER_LOG_FILE}"
  log_info "  - Agent 日志: ${AGENTS_LOG_FILE}"
  log_info "  - 服务日志级别: ${SERVER_LOG_LEVEL} (可通过 SERVER_LOG_LEVEL 覆盖)"
  log_info "  - 实时查看: tail -f ${SERVER_LOG_FILE} ${AGENTS_LOG_FILE}"
  log_info "  - 停止服务: 按 Ctrl+C"
}

cleanup_old_pid() {
  local name="$1"
  local file=".pids/${name}.pid"
  if [ ! -f "$file" ]; then
    return
  fi
  local old_pid
  old_pid="$(cat "$file" 2>/dev/null || true)"
  if [ -n "${old_pid}" ] && kill -0 "$old_pid" 2>/dev/null; then
    log_info "♻️  Stopping previous ${name} process (pid=${old_pid})..."
    kill "$old_pid" 2>/dev/null || true
    sleep 0.3
  fi
  rm -f "$file"
}

cleanup_old_pid "server"
cleanup_old_pid "agents"

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
  log_warn "⚠️  .env not found. Copying from .env.example..."
  cp .env.example .env
  log_warn "📝 Edit .env for server polling/timeout settings, then re-run start.sh"
fi
set -a; source .env 2>/dev/null || true; set +a

detect_lan_ip() {
  local ip=""
  if command -v ip >/dev/null 2>&1; then
    ip="$(ip route get 1 2>/dev/null | awk '{for(i=1;i<=NF;i++) if($i=="src"){print $(i+1); exit}}')"
  fi
  if [ -z "$ip" ] && command -v hostname >/dev/null 2>&1; then
    ip="$(hostname -I 2>/dev/null | awk '{print $1}')"
  fi
  if [ -z "$ip" ] && command -v ipconfig >/dev/null 2>&1; then
    for iface in en0 en1; do
      ip="$(ipconfig getifaddr "$iface" 2>/dev/null || true)"
      if [ -n "$ip" ]; then
        break
      fi
    done
  fi
  if [ -z "$ip" ] && command -v ifconfig >/dev/null 2>&1; then
    ip="$(ifconfig 2>/dev/null | awk '/inet / && $2 !~ /^127\\./ {print $2; exit}')"
  fi
  printf "%s" "$ip"
}

SERVER_HOST="${SERVER_HOST:-0.0.0.0}"
SERVER_PORT="${SERVER_PORT:-8080}"
OPEN_BROWSER="${OPEN_BROWSER:-1}"
LOCAL_ACCESS_URL="${LOCAL_ACCESS_URL:-http://localhost:${SERVER_PORT}}"
LAN_IP="${LAN_IP_OVERRIDE:-$(detect_lan_ip)}"
LAN_ACCESS_URL=""
if [ "${SERVER_HOST}" = "0.0.0.0" ] && [ -n "${LAN_IP}" ]; then
  LAN_ACCESS_URL="http://${LAN_IP}:${SERVER_PORT}"
elif [ "${SERVER_HOST}" != "127.0.0.1" ] && [ "${SERVER_HOST}" != "localhost" ]; then
  LAN_ACCESS_URL="http://${SERVER_HOST}:${SERVER_PORT}"
fi
HEALTHCHECK_HOST="${SERVER_HOST}"
if [ "${HEALTHCHECK_HOST}" = "0.0.0.0" ]; then
  HEALTHCHECK_HOST="127.0.0.1"
fi
HEALTHCHECK_URL="http://${HEALTHCHECK_HOST}:${SERVER_PORT}/"
BROWSER_URL="${BROWSER_URL:-${LOCAL_ACCESS_URL}}"

# ── Virtual environment ───────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
  log_info "🐍 Creating virtual environment..."
  python3 -m venv .venv
fi

log_info "📦 Installing dependencies..."
.venv/bin/pip install -q -r requirements.txt

# ── Start server ──────────────────────────────────────────────────────────────
log_info "🚀 Starting FastAPI server on ${SERVER_HOST}:${SERVER_PORT} ..."
.venv/bin/uvicorn server.app:app --host "${SERVER_HOST}" --port "${SERVER_PORT}" --no-access-log --log-level "$SERVER_LOG_LEVEL" \
  > >(prefix_logs >>"$SERVER_LOG_FILE") \
  2> >(prefix_logs >>"$SERVER_LOG_FILE") &
SERVER_PID=$!
echo "$SERVER_PID" > .pids/server.pid

# Wait up to 10s for server
SERVER_READY=0
for i in $(seq 1 20); do
  if curl -s "${HEALTHCHECK_URL}" > /dev/null 2>&1; then
    log_done "Server ready."
    SERVER_READY=1
    break
  fi
  sleep 0.5
done
if [ "$SERVER_READY" -ne 1 ]; then
  if ! kill -0 "$SERVER_PID" 2>/dev/null; then
    log_warn "Server process exited unexpectedly, aborting startup."
    rm -f .pids/server.pid
    exit 1
  fi
  log_warn "Server health check timed out (10s), continuing startup..."
fi

# ── Start agents ──────────────────────────────────────────────────────────────
log_info "🤖 Starting agents..."
(cd agents && PYTHONUNBUFFERED=1 ../.venv/bin/python -u run_all.py) \
  > >(prefix_logs >>"$AGENTS_LOG_FILE") \
  2> >(prefix_logs >>"$AGENTS_LOG_FILE") &
AGENTS_PID=$!
echo "$AGENTS_PID" > .pids/agents.pid

# ── Open browser ─────────────────────────────────────────────────────────────
print_quick_guide

if [ "${OPEN_BROWSER}" = "1" ] || [ "${OPEN_BROWSER}" = "true" ]; then
  if command -v open &>/dev/null; then
    open "${BROWSER_URL}" >/dev/null 2>&1 || log_warn "Auto-open browser failed; please open ${BROWSER_URL} manually."
  elif command -v xdg-open &>/dev/null; then
    xdg-open "${BROWSER_URL}" >/dev/null 2>&1 || log_warn "Auto-open browser failed; please open ${BROWSER_URL} manually."
  fi
else
  log_info "OPEN_BROWSER=${OPEN_BROWSER}，已跳过自动打开浏览器。"
fi

# ── Cleanup on exit ───────────────────────────────────────────────────────────
cleanup_on_exit() {
  log_info "Stopping..."
  if [ -n "${SERVER_PID:-}" ]; then
    kill "$SERVER_PID" 2>/dev/null || true
  fi
  if [ -n "${AGENTS_PID:-}" ]; then
    kill "$AGENTS_PID" 2>/dev/null || true
  fi
  rm -f .pids/server.pid .pids/agents.pid
  exit 0
}
trap cleanup_on_exit INT TERM

wait
