#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"
mkdir -p .pids

cleanup_old_pid() {
  local name="$1"
  local file=".pids/${name}.pid"
  if [ ! -f "$file" ]; then
    return
  fi
  local old_pid
  old_pid="$(cat "$file" 2>/dev/null || true)"
  if [ -n "${old_pid}" ] && kill -0 "$old_pid" 2>/dev/null; then
    echo "♻️  Stopping previous ${name} process (pid=${old_pid})..."
    kill "$old_pid" 2>/dev/null || true
    sleep 0.3
  fi
  rm -f "$file"
}

cleanup_old_pid "server"
cleanup_old_pid "agents"

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
  echo "⚠️  .env not found. Copying from .env.example..."
  cp .env.example .env
  echo "📝 Edit .env for server polling/timeout settings, then re-run start.sh"
fi
set -a; source .env 2>/dev/null || true; set +a

# ── Virtual environment ───────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
  echo "🐍 Creating virtual environment..."
  python3 -m venv .venv
fi

echo "📦 Installing dependencies..."
.venv/bin/pip install -q -r requirements.txt

# ── Start server ──────────────────────────────────────────────────────────────
echo ""
echo "🚀 Starting FastAPI server on http://localhost:8080 ..."
.venv/bin/uvicorn server.app:app --host 0.0.0.0 --port 8080 &
SERVER_PID=$!
echo "$SERVER_PID" > .pids/server.pid

# Wait up to 10s for server
for i in $(seq 1 20); do
  if curl -s http://localhost:8080/ > /dev/null 2>&1; then
    echo "   Server ready."
    break
  fi
  sleep 0.5
done

# ── Start agents ──────────────────────────────────────────────────────────────
echo "🤖 Starting agents..."
(cd agents && ../.venv/bin/python run_all.py) &
AGENTS_PID=$!
echo "$AGENTS_PID" > .pids/agents.pid

# ── Open browser ─────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════╗"
echo "║  Multi-Agent Task Board              ║"
echo "║  http://localhost:8080               ║"
echo "╚══════════════════════════════════════╝"
echo ""
echo "  Agent CLI can be configured in: Agent 管理 -> 编辑 Agent 类型"
echo ""
echo "  Press Ctrl+C to stop all processes."
echo ""

if command -v open &>/dev/null; then
  open http://localhost:8080
elif command -v xdg-open &>/dev/null; then
  xdg-open http://localhost:8080
fi

# ── Cleanup on exit ───────────────────────────────────────────────────────────
trap "echo ''; echo 'Stopping...'; kill $SERVER_PID $AGENTS_PID 2>/dev/null; rm -f .pids/server.pid .pids/agents.pid; exit 0" INT TERM

wait
