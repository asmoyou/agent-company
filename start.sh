#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
  echo "⚠️  .env not found. Copying from .env.example..."
  cp .env.example .env
  echo "📝 Edit .env to configure which CLI tools to use, then re-run start.sh"
fi
set -a; source .env 2>/dev/null || true; set +a

# ── Check required CLI tools ──────────────────────────────────────────────────
DEVELOPER_CLI="${DEVELOPER_CLI:-claude}"
REVIEWER_CLI="${REVIEWER_CLI:-claude}"

check_cli() {
  if ! command -v "$1" &>/dev/null; then
    echo "❌ '$1' not found in PATH."
    echo "   Install it or change ${2} in .env"
    exit 1
  else
    echo "✅ $1 found: $(command -v "$1")"
  fi
}

echo "🔍 Checking CLI tools..."
check_cli "$DEVELOPER_CLI" "DEVELOPER_CLI"
check_cli "$REVIEWER_CLI"  "REVIEWER_CLI"

# ── Virtual environment ───────────────────────────────────────────────────────
if [ ! -d ".venv" ]; then
  echo "🐍 Creating virtual environment..."
  python3 -m venv .venv
fi

echo "📦 Installing dependencies..."
.venv/bin/pip install -q -r requirements.txt

# ── Git setup ─────────────────────────────────────────────────────────────────
WORKTREE_DIR=".worktrees/dev"

if [ ! -d ".git" ]; then
  echo "🗃️  Initialising git repository..."
  git init
  git checkout -b main 2>/dev/null || git checkout main
  git config user.email "agent@opc-demo.local"
  git config user.name "OPC Agent"
  git commit --allow-empty -m "chore: initial commit"
else
  # Ensure git user is configured (needed for agent commits)
  git config user.email "agent@opc-demo.local" 2>/dev/null || true
  git config user.name "OPC Agent" 2>/dev/null || true
fi

if [ ! -d "$WORKTREE_DIR" ]; then
  echo "🌿 Setting up dev worktree at $WORKTREE_DIR ..."
  mkdir -p .worktrees

  if ! git show-ref --verify --quiet refs/heads/dev; then
    git branch dev
  fi

  git worktree add "$WORKTREE_DIR" dev
  echo "✅ Dev worktree ready"
fi

# ── Start server ──────────────────────────────────────────────────────────────
echo ""
echo "🚀 Starting FastAPI server on http://localhost:8000 ..."
.venv/bin/uvicorn server.app:app --host 0.0.0.0 --port 8000 &
SERVER_PID=$!

# Wait up to 10s for server
for i in $(seq 1 20); do
  if curl -s http://localhost:8000/ > /dev/null 2>&1; then
    echo "   Server ready."
    break
  fi
  sleep 0.5
done

# ── Start agents ──────────────────────────────────────────────────────────────
echo "🤖 Starting agents (developer=$DEVELOPER_CLI, reviewer=$REVIEWER_CLI)..."
(cd agents && ../.venv/bin/python run_all.py) &
AGENTS_PID=$!

# ── Open browser ─────────────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════╗"
echo "║  Multi-Agent Task Board              ║"
echo "║  http://localhost:8000               ║"
echo "╚══════════════════════════════════════╝"
echo ""
echo "  Developer CLI : $DEVELOPER_CLI"
echo "  Reviewer CLI  : $REVIEWER_CLI"
echo ""
echo "  Press Ctrl+C to stop all processes."
echo ""

if command -v open &>/dev/null; then
  open http://localhost:8000
elif command -v xdg-open &>/dev/null; then
  xdg-open http://localhost:8000
fi

# ── Cleanup on exit ───────────────────────────────────────────────────────────
trap "echo ''; echo 'Stopping...'; kill $SERVER_PID $AGENTS_PID 2>/dev/null; exit 0" INT TERM

wait
