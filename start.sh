#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"
mkdir -p .pids
mkdir -p logs

SERVER_LOG_FILE="logs/server.log"
AGENTS_LOG_FILE="logs/agents.log"

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

print_quick_guide() {
  log_info "╔════════════════════════════════════════════════════════════╗"
  log_info "║ Multi-Agent Task Board 已启动                              ║"
  log_info "╚════════════════════════════════════════════════════════════╝"
  log_info "服务地址: http://localhost:8080"
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

# ── Virtual environment ───────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
  log_info "🐍 Creating virtual environment..."
  python3 -m venv .venv
fi

log_info "📦 Installing dependencies..."
.venv/bin/pip install -q -r requirements.txt

# ── Start server ──────────────────────────────────────────────────────────────
log_info "🚀 Starting FastAPI server on http://localhost:8080 ..."
.venv/bin/uvicorn server.app:app --host 0.0.0.0 --port 8080 --no-access-log --log-level warning >>"$SERVER_LOG_FILE" 2>&1 &
SERVER_PID=$!
echo "$SERVER_PID" > .pids/server.pid

# Wait up to 10s for server
SERVER_READY=0
for i in $(seq 1 20); do
  if curl -s http://localhost:8080/ > /dev/null 2>&1; then
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
(cd agents && ../.venv/bin/python run_all.py) >>"$AGENTS_LOG_FILE" 2>&1 &
AGENTS_PID=$!
echo "$AGENTS_PID" > .pids/agents.pid

# ── Open browser ─────────────────────────────────────────────────────────────
print_quick_guide

if command -v open &>/dev/null; then
  open http://localhost:8080 >/dev/null 2>&1 || log_warn "Auto-open browser failed; please open http://localhost:8080 manually."
elif command -v xdg-open &>/dev/null; then
  xdg-open http://localhost:8080 >/dev/null 2>&1 || log_warn "Auto-open browser failed; please open http://localhost:8080 manually."
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
