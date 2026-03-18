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

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

detect_package_manager() {
  if command_exists brew; then
    printf "%s" "brew"
    return
  fi
  if command_exists apt-get; then
    printf "%s" "apt-get"
    return
  fi
  if command_exists dnf; then
    printf "%s" "dnf"
    return
  fi
  if command_exists yum; then
    printf "%s" "yum"
    return
  fi
  if command_exists pacman; then
    printf "%s" "pacman"
    return
  fi
  printf "%s" ""
}

PACKAGE_MANAGER="$(detect_package_manager)"
PACKAGE_CACHE_READY=0

run_as_admin() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
    return
  fi
  if command_exists sudo && sudo -n true >/dev/null 2>&1; then
    sudo "$@"
    return
  fi
  return 1
}

install_system_packages() {
  local label="$1"
  shift
  if [ "$#" -eq 0 ]; then
    return 1
  fi
  case "${PACKAGE_MANAGER}" in
    brew)
      log_info "📥 Installing ${label} via Homebrew: $*"
      brew install "$@"
      ;;
    apt-get)
      if [ "${PACKAGE_CACHE_READY}" -eq 0 ]; then
        log_info "📥 Refreshing apt package index..."
        run_as_admin apt-get update
        PACKAGE_CACHE_READY=1
      fi
      log_info "📥 Installing ${label} via apt-get: $*"
      run_as_admin apt-get install -y "$@"
      ;;
    dnf)
      log_info "📥 Installing ${label} via dnf: $*"
      run_as_admin dnf install -y "$@"
      ;;
    yum)
      log_info "📥 Installing ${label} via yum: $*"
      run_as_admin yum install -y "$@"
      ;;
    pacman)
      log_info "📥 Installing ${label} via pacman: $*"
      run_as_admin pacman -Sy --noconfirm "$@"
      ;;
    *)
      return 1
      ;;
  esac
}

ensure_command() {
  local cmd="$1"
  local label="$2"
  local brew_pkgs="$3"
  local apt_pkgs="$4"
  local rpm_pkgs="$5"
  local pacman_pkgs="$6"
  if command_exists "$cmd"; then
    log_done "${label} already available: $(command -v "$cmd")"
    return
  fi

  log_warn "Missing ${label} (${cmd}); attempting automatic install..."
  local pkg_list=""
  case "${PACKAGE_MANAGER}" in
    brew)
      pkg_list="${brew_pkgs}"
      ;;
    apt-get)
      pkg_list="${apt_pkgs}"
      ;;
    dnf|yum)
      pkg_list="${rpm_pkgs}"
      ;;
    pacman)
      pkg_list="${pacman_pkgs}"
      ;;
  esac

  if [ -n "${pkg_list}" ] && install_system_packages "${label}" ${pkg_list}; then
    hash -r
  fi

  if command_exists "$cmd"; then
    log_done "${label} installed successfully."
    return
  fi

  log_warn "Unable to auto-install ${label}. Please install ${cmd} manually and re-run start.sh."
  exit 1
}

ensure_optional_command() {
  local cmd="$1"
  local label="$2"
  local brew_pkgs="$3"
  local apt_pkgs="$4"
  local rpm_pkgs="$5"
  local pacman_pkgs="$6"
  if command_exists "$cmd"; then
    log_done "${label} already available: $(command -v "$cmd")"
    return
  fi

  log_warn "Missing optional dependency ${label} (${cmd}); attempting automatic install..."
  local pkg_list=""
  case "${PACKAGE_MANAGER}" in
    brew)
      pkg_list="${brew_pkgs}"
      ;;
    apt-get)
      pkg_list="${apt_pkgs}"
      ;;
    dnf|yum)
      pkg_list="${rpm_pkgs}"
      ;;
    pacman)
      pkg_list="${pacman_pkgs}"
      ;;
  esac

  if [ -n "${pkg_list}" ] && install_system_packages "${label}" ${pkg_list}; then
    hash -r
  fi

  if command_exists "$cmd"; then
    log_done "${label} installed successfully."
    return
  fi

  log_warn "Optional dependency ${label} is still unavailable. Continuing startup without it."
}

create_virtualenv() {
  if python3 -m venv .venv >/dev/null 2>&1; then
    return
  fi

  log_warn "python3 venv module is unavailable; attempting to install venv support..."
  case "${PACKAGE_MANAGER}" in
    apt-get)
      install_system_packages "Python venv support" python3-venv python3-pip || true
      ;;
    dnf|yum)
      install_system_packages "Python tooling" python3-pip || true
      ;;
    pacman)
      install_system_packages "Python tooling" python-pip || true
      ;;
  esac

  if python3 -m venv .venv >/dev/null 2>&1; then
    return
  fi

  log_warn "Falling back to virtualenv bootstrap..."
  python3 -m ensurepip --upgrade >/dev/null 2>&1 || true
  python3 -m pip install -q virtualenv
  python3 -m virtualenv .venv
}

activate_virtualenv() {
  if [ ! -f ".venv/bin/activate" ]; then
    log_warn "Virtual environment activation script is missing."
    exit 1
  fi
  # shellcheck disable=SC1091
  source .venv/bin/activate
  hash -r
  log_done "Virtual environment activated: ${VIRTUAL_ENV}"
}

install_node_dependencies_if_needed() {
  if [ ! -f "package.json" ]; then
    return
  fi

  log_info "📦 Detected package.json, installing Node.js dependencies..."
  if [ -f "pnpm-lock.yaml" ] && command_exists pnpm; then
    pnpm install --frozen-lockfile
    return
  fi
  if [ -f "yarn.lock" ] && command_exists yarn; then
    yarn install --frozen-lockfile
    return
  fi
  if command_exists npm; then
    npm install
    return
  fi

  log_warn "package.json exists but no supported package manager was found."
  exit 1
}

bootstrap_runtime() {
  ensure_command python3 "Python 3" "python" "python3 python3-venv python3-pip" "python3 python3-pip" "python python-pip"
  ensure_command git "Git" "git" "git" "git" "git"
  ensure_command curl "curl" "curl" "curl" "curl" "curl"
  ensure_optional_command node "Node.js" "node" "nodejs npm" "nodejs" "nodejs npm"

  if ! command_exists codex && ! command_exists claude; then
    log_warn "Neither codex nor claude CLI is installed yet. The web service can start, but agents will not execute tasks until at least one CLI is available."
  fi
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

bootstrap_runtime

# ── Load .env ─────────────────────────────────────────────────────────────────
if [ ! -f ".env" ]; then
  log_warn "⚠️  .env not found. Copying from .env.example..."
  cp .env.example .env
  log_info "📝 Using defaults from .env.example for this run. Edit .env later if you need custom settings."
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
  create_virtualenv
fi

activate_virtualenv

if ! python -m pip --version >/dev/null 2>&1; then
  log_info "📦 Bootstrapping pip in virtual environment..."
  python -m ensurepip --upgrade >/dev/null 2>&1 || true
fi

log_info "📦 Installing dependencies..."
python -m pip install -q -r requirements.txt
install_node_dependencies_if_needed

# ── Start server ──────────────────────────────────────────────────────────────
log_info "🚀 Starting FastAPI server on ${SERVER_HOST}:${SERVER_PORT} ..."
python -m uvicorn server.app:app --host "${SERVER_HOST}" --port "${SERVER_PORT}" --no-access-log --log-level "$SERVER_LOG_LEVEL" \
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
(cd agents && PYTHONUNBUFFERED=1 python -u run_all.py) \
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
