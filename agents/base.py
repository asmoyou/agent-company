import asyncio
import errno
import fcntl
import contextlib
import hashlib
import json
import os
import pty
import re
import signal
import shutil
import sys
import tempfile
import termios
import time
from pathlib import Path

import httpx

PROJECT_ROOT    = Path(__file__).parent.parent
SERVER_ROOT     = PROJECT_ROOT / "server"
if str(SERVER_ROOT) not in sys.path:
    sys.path.insert(0, str(SERVER_ROOT))

from task_intelligence import (
    UNRESOLVED_ISSUE_STATUSES,
    normalize_allowed_surface,
    normalize_issue_list,
    summarize_issue_list,
)

SERVER_URL      = os.getenv("SERVER_URL", "http://localhost:8080")
POLL_INTERVAL   = int(os.getenv("POLL_INTERVAL", "5"))
CLI_TIMEOUT     = int(os.getenv("CLI_TIMEOUT", "300"))
HEARTBEAT_SECS  = int(os.getenv("HEARTBEAT_SECS", "45"))
STATUS_HEARTBEAT_SECS = int(os.getenv("STATUS_HEARTBEAT_SECS", "20"))
AGENT_POST_TIMEOUT_SECS = float(os.getenv("AGENT_POST_TIMEOUT_SECS", "8"))
TASK_LEASE_TTL_SECS = int(os.getenv("TASK_LEASE_TTL_SECS", "180"))
TASK_LEASE_RENEW_INTERVAL_SECS = int(os.getenv("TASK_LEASE_RENEW_INTERVAL_SECS", "20"))
TASK_LEASE_RENEW_WARN_AFTER_ERRORS = int(os.getenv("TASK_LEASE_RENEW_WARN_AFTER_ERRORS", "3"))
TASK_LEASE_RENEW_FAIL_HARD_AFTER_ERRORS = int(
    os.getenv("TASK_LEASE_RENEW_FAIL_HARD_AFTER_ERRORS", "9")
)
TASK_STATUS_POLL_FAILURE_MAX = int(os.getenv("TASK_STATUS_POLL_FAILURE_MAX", "30"))
OUTPUT_POST_MAX_INFLIGHT = int(os.getenv("OUTPUT_POST_MAX_INFLIGHT", "12"))
AGENT_API_TOKEN = str(os.getenv("AGENT_API_TOKEN", "opc-agent-internal")).strip()
ANSI_ESCAPE_RE = re.compile(
    r"\x1B(?:\][^\x07\x1B]*(?:\x07|\x1B\\)|[@-Z\\-_]|\[[0-?]*[ -/]*[@-~])"
)

CLI_TEMPLATES = {
    "claude": ["claude", "--dangerously-skip-permissions", "-p", "{prompt}"],
    # Use non-interactive subcommand to avoid TTY requirement in agent subprocesses.
    "codex":  ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "{prompt}"],
}
# External stdin auto-reply is disabled by default; enable only if explicitly configured.
AUTO_REPLY_MAX = int(os.getenv("AUTO_REPLY_MAX", "0"))
# Idle fallback auto-reply is disabled by default to avoid blind ENTER loops.
AUTO_REPLY_IDLE_SECS = int(os.getenv("AUTO_REPLY_IDLE_SECS", "0"))
AUTO_REPLY_TEXT = os.getenv("AUTO_REPLY_TEXT", "\n")
HANDOFF_SYNC_STRATEGY = os.getenv("HANDOFF_SYNC_STRATEGY", "cherry-pick").strip().lower()
BRANCH_SYNC_STRATEGY = os.getenv("BRANCH_SYNC_STRATEGY", "merge").strip().lower()
TASK_DELIVERY_MODEL = os.getenv("TASK_DELIVERY_MODEL", "patchset").strip().lower() or "patchset"
MANAGER_MERGE_MODE = os.getenv("MANAGER_MERGE_MODE", "squash_patchset").strip().lower() or "squash_patchset"
CODEX_ENABLE_OUTPUT_SCHEMA = os.getenv("CODEX_ENABLE_OUTPUT_SCHEMA", "0").strip().lower() in {"1", "true", "yes", "on"}
CONTEXT_MAX_CHARS = int(os.getenv("CONTEXT_MAX_CHARS", "3600"))
CONTEXT_MAX_HANDOFFS = int(os.getenv("CONTEXT_MAX_HANDOFFS", "24"))
CONTEXT_MAX_UNRESOLVED_FEEDBACK = int(os.getenv("CONTEXT_MAX_UNRESOLVED_FEEDBACK", "12"))
CONTEXT_MAX_FEEDBACK_TIMELINE = int(os.getenv("CONTEXT_MAX_FEEDBACK_TIMELINE", "18"))
INTERACTIVE_PROMPT_RE = re.compile(
    r"(?i)(press\s+enter(?:\s+to\s+continue)?|hit\s+enter(?:\s+to\s+continue)?|"
    r"请按回车(?:继续)?|按下回车键|按回车继续|回车继续)"
)
TASK_SECTION_HEADING_RE = re.compile(r"^\s{0,3}#{1,6}\s+(.+?)\s*$")
TASK_LIST_PREFIX_RE = re.compile(r"^\s*(?:[-*+]|(?:\d+\.))\s+(?:\[[ xX]\]\s*)?")
DOCUMENT_FILE_TASK_RE = re.compile(r"(?i)(文档|合同|模板|word|docx?\b|pdf\b|xlsx?\b|pptx?\b|文件|附件|导出|转换|转为)")
INTERACTIVE_UI_TASK_RE = re.compile(r"(网页|页面|浏览器|前端|交互|按钮|界面|动画|表单|游戏)")
TASK_SECTION_ALIAS_MAP = {
    "goal": (
        "任务目标",
        "子任务目标",
        "目标",
        "需求目标",
        "objective",
    ),
    "parent_refs": (
        "关联父需求编号",
        "父需求编号",
        "需求编号",
    ),
    "scope": (
        "实施范围",
        "实现范围",
        "范围",
    ),
    "non_scope": (
        "非范围",
        "不在范围",
        "不包含",
    ),
    "constraints": (
        "关键约束",
        "约束",
        "限制",
    ),
    "todo_steps": (
        "todo步骤",
        "todo",
        "实施步骤",
        "执行步骤",
        "步骤",
    ),
    "deliverables": (
        "交付物",
        "可交付物",
    ),
    "acceptance": (
        "验收标准",
        "完成标准",
        "acceptancecriteria",
    ),
    "assumptions": (
        "假设",
        "assumptions",
        "待确认",
    ),
    "evidence_required": (
        "证据要求",
        "evidencerequired",
        "evidence",
    ),
}
TASK_PROMPT_LIST_MAX_ITEMS = 8
TASK_PROMPT_ITEM_MAX_CHARS = 180
TASK_PROMPT_TEXT_MAX_CHARS = 360
RUNTIME_GIT_EXCLUDE_PATTERNS = (
    ".opc/decisions/",
    "opc-codex-last-*",
    "opc-codex-schema-*",
)


def build_cli_cmd(cli_name: str, prompt: str) -> list[str]:
    template = CLI_TEMPLATES.get(cli_name, [cli_name, "-p", "{prompt}"])
    return [arg.replace("{prompt}", prompt) for arg in template]


def _prepare_tty_child(slave_fd: int) -> None:
    os.setsid()
    if hasattr(termios, "TIOCSCTTY"):
        try:
            fcntl.ioctl(slave_fd, termios.TIOCSCTTY, 0)
        except OSError:
            pass


def parse_status_list(raw, default: list[str]) -> list[str]:
    """Parse poll_statuses from DB row (JSON string/list) with fallback."""
    if isinstance(raw, list) and raw:
        return [str(x) for x in raw]
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
            if isinstance(decoded, list) and decoded:
                return [str(x) for x in decoded]
        except Exception:
            pass
    return list(default)


def normalize_agent_key(agent_key: str | None, default: str = "developer") -> str:
    raw = (agent_key or default).strip().lower()
    safe = re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")
    return safe or default


def safe_agent_key(agent_key: str | None) -> str:
    raw = (agent_key or "").strip().lower()
    return re.sub(r"[^a-z0-9_-]+", "-", raw).strip("-_")


def get_task_dev_agent(task: dict, fallback: str = "developer") -> str:
    return normalize_agent_key(task.get("dev_agent") or task.get("assigned_agent") or fallback)


def is_review_enabled(task: dict | None) -> bool:
    raw = (task or {}).get("review_enabled")
    if raw is None:
        return True
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return int(raw) != 0
    text = str(raw).strip().lower()
    if not text:
        return True
    return text not in {"0", "false", "off", "no"}


def _task_scope_suffix(task: dict | None) -> str:
    task_id = str((task or {}).get("id") or "").strip().lower()
    if not task_id:
        return ""
    safe = re.sub(r"[^a-z0-9_-]+", "-", task_id).strip("-_")
    return safe


def get_agent_branch(agent_key: str, task: dict | None = None) -> str:
    base = f"agent/{normalize_agent_key(agent_key)}"
    suffix = _task_scope_suffix(task)
    return f"{base}/{suffix}" if suffix else base


def get_project_dirs(task: dict, agent_key: str | None = None) -> tuple[Path, Path]:
    """Return (project_root, worktree_for_agent)."""
    project_path = task.get("project_path")
    if project_path:
        root = Path(project_path)
    else:
        root = PROJECT_ROOT / ".worktrees" / "scratch"
        root.mkdir(parents=True, exist_ok=True)
    key = normalize_agent_key(agent_key or get_task_dev_agent(task))
    scoped = _task_scope_suffix(task)
    if scoped:
        return root, root / ".worktrees" / key / scoped
    return root, root / ".worktrees" / key


class BaseAgent:
    name: str = "base"
    poll_statuses: list[str] = []
    cli_name: str = "codex"
    working_status: str = ""

    def __init__(self, shutdown_event: asyncio.Event | None = None):
        self.shutdown = shutdown_event or asyncio.Event()
        auth_headers = {"X-Agent-Token": AGENT_API_TOKEN} if AGENT_API_TOKEN else {}
        # trust_env=False: ignore system proxy (SOCKS etc.) for localhost calls
        self.http = httpx.AsyncClient(
            base_url=SERVER_URL,
            timeout=30,
            trust_env=False,
            headers=auth_headers,
        )
        # Use a separate, bounded channel for terminal stream output so
        # control-plane calls (lease renew/status/task polling) are not starved.
        self.http_output = httpx.AsyncClient(
            base_url=SERVER_URL,
            timeout=AGENT_POST_TIMEOUT_SECS,
            trust_env=False,
            headers=auth_headers,
            limits=httpx.Limits(max_keepalive_connections=8, max_connections=24),
        )
        self._output_post_sem = asyncio.Semaphore(max(1, OUTPUT_POST_MAX_INFLIGHT))
        self.project_id_scope: str | None = str(os.getenv("AGENT_PROJECT_ID", "")).strip() or None
        self.worker_id: str | None = str(os.getenv("AGENT_WORKER_ID", "")).strip() or None
        self._active_task_id: str | None = None
        self._active_run_id: str | None = None
        self._active_lease_token: str | None = None
        self._active_project_id: str | None = None
        self._active_lease_lost: bool = False
        self._active_phase: str = ""
        self._active_cli_pid: int | None = None

    def _clip_prompt_text(self, text: str, limit: int = TASK_PROMPT_TEXT_MAX_CHARS) -> str:
        compact = re.sub(r"\s+", " ", str(text or "").strip())
        if len(compact) <= limit:
            return compact
        return compact[: max(0, limit - 12)].rstrip() + " ..."

    def _resolve_git_dir(self, repo_root: Path) -> Path | None:
        dot_git = repo_root / ".git"
        if dot_git.is_dir():
            return dot_git
        if not dot_git.is_file():
            return None
        try:
            raw = dot_git.read_text(encoding="utf-8", errors="replace").strip()
        except Exception:
            return None
        match = re.match(r"gitdir:\s*(.+)$", raw, re.IGNORECASE)
        if not match:
            return None
        git_dir = Path(match.group(1).strip())
        if not git_dir.is_absolute():
            git_dir = (repo_root / git_dir).resolve()
        return git_dir

    def _ensure_runtime_git_excludes(self, worktree: Path) -> None:
        git_dir = self._resolve_git_dir(worktree)
        if git_dir is None:
            return
        info_dir = git_dir / "info"
        info_dir.mkdir(parents=True, exist_ok=True)
        exclude_path = info_dir / "exclude"
        try:
            existing = exclude_path.read_text(encoding="utf-8", errors="replace")
        except FileNotFoundError:
            existing = ""
        except Exception:
            return
        lines = existing.splitlines()
        changed = False
        for pattern in RUNTIME_GIT_EXCLUDE_PATTERNS:
            if pattern in lines:
                continue
            lines.append(pattern)
            changed = True
        if not changed:
            return
        payload = "\n".join(lines).strip()
        if payload:
            payload += "\n"
        exclude_path.write_text(payload, encoding="utf-8")

    def _normalize_task_section_name(self, raw: str) -> str:
        cleaned = re.sub(r"[\s:：()（）/_-]+", "", str(raw or "")).strip().lower()
        for key, aliases in TASK_SECTION_ALIAS_MAP.items():
            for alias in aliases:
                alias_clean = re.sub(r"[\s:：()（）/_-]+", "", alias).strip().lower()
                if cleaned == alias_clean:
                    return key
        return cleaned

    def _parse_task_description_sections(self, description: str) -> dict[str, str]:
        sections: dict[str, str] = {}
        current_key = ""
        buf: list[str] = []
        for raw_line in str(description or "").splitlines():
            line = raw_line.rstrip()
            match = TASK_SECTION_HEADING_RE.match(line)
            if match:
                if current_key:
                    body = "\n".join(buf).strip()
                    if body:
                        prev = sections.get(current_key)
                        sections[current_key] = f"{prev}\n{body}".strip() if prev else body
                current_key = self._normalize_task_section_name(match.group(1))
                buf = []
                continue
            if current_key:
                buf.append(line)
        if current_key:
            body = "\n".join(buf).strip()
            if body:
                prev = sections.get(current_key)
                sections[current_key] = f"{prev}\n{body}".strip() if prev else body
        return sections

    def _section_items(
        self,
        body: str,
        *,
        max_items: int = TASK_PROMPT_LIST_MAX_ITEMS,
        item_limit: int = TASK_PROMPT_ITEM_MAX_CHARS,
    ) -> list[str]:
        items: list[str] = []
        current = ""
        for raw_line in str(body or "").splitlines():
            stripped = raw_line.strip()
            if not stripped or stripped.startswith("```"):
                continue
            is_item = bool(TASK_LIST_PREFIX_RE.match(stripped))
            content = TASK_LIST_PREFIX_RE.sub("", stripped).strip() if is_item else stripped
            if not content:
                continue
            if is_item:
                if current:
                    items.append(self._clip_prompt_text(current, limit=item_limit))
                    if len(items) >= max_items:
                        return items
                current = content
                continue
            if current:
                current = f"{current} {content}".strip()
            else:
                current = content
        if current and len(items) < max_items:
            items.append(self._clip_prompt_text(current, limit=item_limit))
        return items

    def _extract_task_contract(self, task: dict | None) -> dict[str, object]:
        current_contract = (task or {}).get("current_contract")
        if isinstance(current_contract, dict):
            return {
                "goal": self._clip_prompt_text(str(current_contract.get("goal") or "").strip(), limit=TASK_PROMPT_TEXT_MAX_CHARS),
                "parent_refs": [str(item).strip() for item in (current_contract.get("parent_refs") or []) if str(item).strip()],
                "scope": [str(item).strip() for item in (current_contract.get("scope") or []) if str(item).strip()],
                "non_scope": [str(item).strip() for item in (current_contract.get("non_scope") or []) if str(item).strip()],
                "constraints": [str(item).strip() for item in (current_contract.get("constraints") or []) if str(item).strip()],
                "todo_steps": [str(item).strip() for item in (current_contract.get("todo_steps") or []) if str(item).strip()],
                "deliverables": [str(item).strip() for item in (current_contract.get("deliverables") or []) if str(item).strip()],
                "acceptance": [str(item).strip() for item in (current_contract.get("acceptance") or []) if str(item).strip()],
                "assumptions": [str(item).strip() for item in (current_contract.get("assumptions") or []) if str(item).strip()],
                "evidence_required": [str(item).strip() for item in (current_contract.get("evidence_required") or []) if str(item).strip()],
            }
        description = str((task or {}).get("description") or "").strip()
        if not description:
            return {}
        sections = self._parse_task_description_sections(description)
        if not sections:
            return {}
        goal_items = self._section_items(sections.get("goal", ""), max_items=3, item_limit=TASK_PROMPT_TEXT_MAX_CHARS)
        return {
            "goal": self._clip_prompt_text(" ".join(goal_items), limit=TASK_PROMPT_TEXT_MAX_CHARS) if goal_items else "",
            "parent_refs": self._section_items(sections.get("parent_refs", ""), max_items=6),
            "scope": self._section_items(sections.get("scope", ""), max_items=6),
            "non_scope": self._section_items(sections.get("non_scope", ""), max_items=6),
            "constraints": self._section_items(sections.get("constraints", ""), max_items=6),
            "todo_steps": self._section_items(sections.get("todo_steps", ""), max_items=8),
            "deliverables": self._section_items(sections.get("deliverables", ""), max_items=8),
            "acceptance": self._section_items(sections.get("acceptance", ""), max_items=8),
            "assumptions": self._section_items(sections.get("assumptions", ""), max_items=6),
            "evidence_required": self._section_items(sections.get("evidence_required", ""), max_items=6),
        }

    def _append_prompt_list_section(
        self,
        lines: list[str],
        title: str,
        items: list[str] | None,
        *,
        checkbox: bool = False,
    ) -> None:
        cleaned = [str(item or "").strip() for item in (items or []) if str(item or "").strip()]
        if not cleaned:
            return
        lines.append(title)
        prefix = "- [ ] " if checkbox else "- "
        lines.extend(f"{prefix}{item}" for item in cleaned)

    def _contract_prefers_cli_file_evidence(self, contract: dict[str, object] | None) -> bool:
        data = contract or {}
        parts: list[str] = [str(data.get("goal") or "")]
        for key in ("scope", "deliverables", "acceptance", "evidence_required", "constraints"):
            parts.extend(str(item or "") for item in (data.get(key) or []))
        text = " ".join(parts)
        if not text:
            return False
        return bool(DOCUMENT_FILE_TASK_RE.search(text)) and not bool(INTERACTIVE_UI_TASK_RE.search(text))

    def build_execution_contract_block(self, task: dict | None) -> str:
        contract = self._extract_task_contract(task)
        if not contract or not any(contract.values()):
            return ""
        lines = [
            "## 执行基线（必须遵守）",
            "- 下面的交付物和验收标准也是执行 agent 的完成定义，不是只给 reviewer 的参考清单。",
            "- 提交前必须按这些条目自检；不要擅自扩展非范围内容。",
            "- 合同中的 assumptions 是 leader 已吸收的不确定性；除非与显式要求或现场证据冲突，不要把它们当成阻塞项，也不要等待额外用户确认。",
            "- 证据优先采用当前 CLI/headless 环境可复核的本地验证；除非用户明确要求人工 GUI、桌面软件、真机或手动点击过程，不要把这些人工操作当成默认必备证据。",
        ]
        if bool(INTERACTIVE_UI_TASK_RE.search(" ".join(str(item or "") for item in (
            [contract.get("goal")] + list(contract.get("scope") or []) + list(contract.get("deliverables") or []) + list(contract.get("acceptance") or [])
        )))):
            lines.append(
                "- 对交互/行为型任务，优先补齐可脚本化的冒烟脚本、自动化测试、截图或断言结果来证明关键路径，而不是等待人工逐步点击界面。"
            )
        if self._contract_prefers_cli_file_evidence(contract):
            lines.append(
                "- 若任务属于文档、文件转换或导出类型，默认按当前 CLI/headless 环境取证；除非用户明确要求桌面软件实测，否则可用结构校验、可解析性检查、回读或转换结果替代 GUI 打开验证。"
            )
        goal = str(contract.get("goal") or "").strip()
        if goal:
            lines.append(f"- 任务目标: {goal}")
        self._append_prompt_list_section(lines, "### 关联父需求编号", contract.get("parent_refs"))
        self._append_prompt_list_section(lines, "### 实施范围", contract.get("scope"))
        self._append_prompt_list_section(lines, "### 非范围", contract.get("non_scope"))
        self._append_prompt_list_section(lines, "### TODO 步骤", contract.get("todo_steps"), checkbox=True)
        self._append_prompt_list_section(lines, "### 必须产出的交付物", contract.get("deliverables"))
        self._append_prompt_list_section(lines, "### 提交前必须满足的验收标准", contract.get("acceptance"), checkbox=True)
        self._append_prompt_list_section(lines, "### 关键约束", contract.get("constraints"))
        self._append_prompt_list_section(lines, "### 已批准的默认假设", contract.get("assumptions"))
        self._append_prompt_list_section(lines, "### 提交前必须补齐的证据", contract.get("evidence_required"))
        return "\n".join(lines)

    def build_review_contract_block(self, task: dict | None) -> str:
        contract = self._extract_task_contract(task)
        if not contract or not any(contract.values()):
            return ""
        lines = [
            "## 独立验收基线（必须据此审查）",
            "- 以下条目既是开发完成定义，也是 reviewer 的独立核查清单；不要只接受开发自述。",
            "- 只要任一验收项缺少证据、交付物缺失、或实现违反约束，就不能 approve。",
            "- 合同中的 assumptions 默认视为允许的执行基线；不要因为“存在 assumptions”本身打回。",
            "- 证据优先采用当前 CLI/headless 环境可复核的本地验证；除非用户明确要求人工 GUI、桌面软件、真机或手动点击过程，不要默认以缺少这些人工操作为由打回。",
        ]
        if bool(INTERACTIVE_UI_TASK_RE.search(" ".join(str(item or "") for item in (
            [contract.get("goal")] + list(contract.get("scope") or []) + list(contract.get("deliverables") or []) + list(contract.get("acceptance") or [])
        )))):
            lines.append(
                "- 对交互/行为型任务，可接受可脚本化的冒烟脚本、自动化测试、截图或断言结果作为关键路径证据；不要默认要求人工逐步点击界面。"
            )
        if self._contract_prefers_cli_file_evidence(contract):
            lines.append(
                "- 对文档、文件转换或导出类任务，若用户未明确要求 Word/WPS/Office 等桌面软件实测，可接受当前 CLI/headless 环境下的等价本地证据（结构校验、可解析性检查、回读/保存结果）；不要仅因缺少 GUI 打开过程而打回。"
            )
        goal = str(contract.get("goal") or "").strip()
        if goal:
            lines.append(f"- 任务目标: {goal}")
        self._append_prompt_list_section(lines, "### 应覆盖的范围", contract.get("scope"))
        self._append_prompt_list_section(lines, "### 不应被扩写的非范围", contract.get("non_scope"))
        self._append_prompt_list_section(lines, "### 应存在的交付物", contract.get("deliverables"))
        self._append_prompt_list_section(lines, "### 必须逐项核验的验收标准", contract.get("acceptance"), checkbox=True)
        self._append_prompt_list_section(lines, "### 不得违反的约束", contract.get("constraints"))
        self._append_prompt_list_section(lines, "### 允许沿用的默认假设", contract.get("assumptions"))
        self._append_prompt_list_section(lines, "### 必须核对的证据", contract.get("evidence_required"))
        lines.append("### 审查判定规则")
        lines.append("- 仅当所有验收项都有代码、测试、文档或行为证据支撑时，才能 approve。")
        lines.append("- TODO 步骤只是实现路径参考，不能替代验收标准本身。")
        lines.append("- 只有 assumptions 与显式需求/约束冲突、明显扩大 scope、或使验收无法验证时，才应 request_changes。")
        lines.append("- request_changes 时，feedback 必须指出未满足的验收项、对应文件或行为以及修复方向。")
        return "\n".join(lines)

    def build_issue_ledger_block(self, task: dict | None) -> str:
        if not task:
            return ""
        open_issues = task.get("open_issues")
        if not isinstance(open_issues, list) or not open_issues:
            return ""
        lines = [
            "## 未解决问题账本（必须逐项处理）",
            "- 本轮只围绕以下 open issue 收敛；不要无依据扩展交付面。",
            summarize_issue_list(open_issues, limit=10),
        ]
        latest_evidence = task.get("latest_evidence")
        if isinstance(latest_evidence, dict):
            summary = str(latest_evidence.get("summary") or "").strip()
            if summary:
                lines.append("### 最近一次证据包摘要")
                lines.append(f"- {summary}")
        return "\n".join(line for line in lines if str(line).strip())

    def build_retry_strategy_block(self, task: dict | None) -> str:
        if not task:
            return ""
        retry_strategy = str(task.get("retry_strategy") or "").strip()
        same_fingerprint_streak = int(task.get("same_fingerprint_streak") or 0)
        execution_phase = str(task.get("execution_phase") or "").strip()
        cooldown_until = str(task.get("cooldown_until") or "").strip()
        if not any([retry_strategy, same_fingerprint_streak, execution_phase, cooldown_until]):
            return ""
        lines = ["## 当前收敛策略"]
        if execution_phase:
            lines.append(f"- execution_phase: {execution_phase}")
        if retry_strategy:
            lines.append(f"- retry_strategy: {retry_strategy}")
        if same_fingerprint_streak:
            lines.append(f"- same_failure_streak: {same_fingerprint_streak}")
        if cooldown_until:
            lines.append(f"- cooldown_until: {cooldown_until}")
        allowed_surface = normalize_allowed_surface(task.get("allowed_surface") or task.get("allowed_surface_json"))
        if any(allowed_surface.values()):
            roots = ", ".join((allowed_surface.get("roots") or [])[:8])
            if roots:
                lines.append(f"- allowed_surface_roots: {roots}")
        return "\n".join(lines)

    # ── HTTP helpers ─────────────────────────────────────────────────────────

    async def fetch_tasks(self, status: str) -> list[dict]:
        r = await self.http.get(f"/tasks/status/{status}")
        r.raise_for_status()
        return r.json()

    async def claim_task(
        self, status: str, working_status: str, respect_assignment: bool, project_id: str | None = None
    ) -> dict | None:
        scope_project_id = project_id
        if scope_project_id is None:
            scope_project_id = self.project_id_scope
        r = await self.http.post(
            "/tasks/claim",
            json={
                "status": status,
                "working_status": working_status,
                "agent": self.name,
                "agent_key": normalize_agent_key(self.name),
                "respect_assignment": respect_assignment,
                "lease_ttl_secs": TASK_LEASE_TTL_SECS,
                "project_id": scope_project_id,
            },
        )
        r.raise_for_status()
        return r.json().get("task")

    def _lease_guard_fields(self, task_id: str | None = None) -> dict:
        if not self._active_run_id or not self._active_lease_token:
            return {}
        if task_id and self._active_task_id and task_id != self._active_task_id:
            return {}
        return {
            "run_id": self._active_run_id,
            "lease_token": self._active_lease_token,
        }

    def _transition_guard_fields(self, task_id: str | None = None) -> dict:
        if not self._active_run_id or not self._active_lease_token:
            return {}
        if task_id and self._active_task_id and task_id != self._active_task_id:
            return {}
        return {
            "expected_run_id": self._active_run_id,
            "expected_lease_token": self._active_lease_token,
        }

    async def renew_task_lease(
        self,
        task_id: str,
        run_id: str,
        lease_token: str,
        lease_ttl_secs: int = TASK_LEASE_TTL_SECS,
    ) -> bool | None:
        try:
            r = await self.http.post(
                f"/tasks/{task_id}/lease/renew",
                json={
                    "run_id": run_id,
                    "lease_token": lease_token,
                    "lease_ttl_secs": lease_ttl_secs,
                },
                timeout=AGENT_POST_TIMEOUT_SECS,
            )
            if r.status_code in (404, 409):
                return False
            r.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code in (404, 409):
                return False
            return None
        except Exception:
            return None

    async def update_task(self, task_id: str, **fields) -> dict:
        r = await self.http.patch(f"/tasks/{task_id}", json=fields)
        r.raise_for_status()
        return r.json()

    async def update_patchset(
        self,
        task_id: str,
        *,
        patchset: dict,
        update_task_refs: bool = False,
    ) -> dict:
        payload = {
            "patchset": dict(patchset or {}),
            "update_task_refs": bool(update_task_refs),
            **self._transition_guard_fields(task_id),
        }
        r = await self.http.post(f"/tasks/{task_id}/patchsets", json=payload)
        if r.status_code >= 400:
            detail = ""
            with contextlib.suppress(Exception):
                detail = str(r.text or "").strip()
            if detail:
                raise RuntimeError(
                    f"Patchset update failed ({r.status_code}) for task {task_id}: {detail[:500]}"
                )
        r.raise_for_status()
        return r.json()

    async def transition_task(
        self,
        task_id: str,
        *,
        fields: dict | None = None,
        handoff: dict | None = None,
        log_message: str | None = None,
        log_agent: str | None = None,
    ) -> dict | None:
        payload: dict = {}
        if fields:
            payload["fields"] = fields
        if handoff:
            handoff_payload = dict(handoff)
            handoff_payload.setdefault("from_agent", self.name)
            payload["handoff"] = handoff_payload
        if log_message is not None:
            payload["log"] = {
                "agent": log_agent or self.name,
                "message": log_message,
            }
        payload.update(self._transition_guard_fields(task_id))
        r = await self.http.post(f"/tasks/{task_id}/transition", json=payload)
        if r.status_code == 404:
            # Task may be deleted/cancelled while agent is still finishing up.
            self._post_output_bg(f"ℹ 任务不存在，停止同步: {task_id}")
            return None
        if r.status_code >= 400:
            detail = ""
            with contextlib.suppress(Exception):
                detail = str(r.text or "").strip()
            if detail:
                raise RuntimeError(
                    f"Transition failed ({r.status_code}) for task {task_id}: {detail[:500]}"
                )
        r.raise_for_status()
        return r.json()

    async def get_task(self, task_id: str) -> dict:
        r = await self.http.get(f"/tasks/{task_id}")
        r.raise_for_status()
        return r.json()

    async def is_task_cancelled(self, task_id: str) -> bool:
        try:
            task = await self.get_task(task_id)
        except httpx.HTTPStatusError as e:
            # 404 means task has been removed/invalidated; treat as cancelled.
            if e.response is not None and e.response.status_code == 404:
                return True
            return False
        except Exception:
            return False
        if not task:
            return True
        status = str(task.get("status") or "").strip().lower()
        archived = int(task.get("archived") or 0)
        return status == "cancelled" or archived == 1

    async def get_task_if_exists(self, task_id: str) -> dict | None:
        try:
            task = await self.get_task(task_id)
            return task if task else None
        except httpx.HTTPStatusError as e:
            if e.response is not None and e.response.status_code == 404:
                return None
            raise

    async def stop_if_task_cancelled(self, task_id: str, stage: str = "") -> bool:
        if not await self.is_task_cancelled(task_id):
            return False
        msg = "任务已取消，停止继续执行"
        if stage:
            msg += f"（{stage}）"
        await self.add_log(task_id, msg)
        self._post_output_bg(f"🛑 {msg}")
        return True

    async def add_log(self, task_id: str, message: str):
        try:
            await self.http.post(
                f"/tasks/{task_id}/logs",
                json={"agent": self.name, "message": message, **self._lease_guard_fields(task_id)},
            )
        except Exception:
            pass

    async def add_alert(
        self,
        summary: str,
        task_id: str | None = None,
        message: str = "",
        kind: str = "error",
        code: str = "",
        stage: str = "",
        metadata: dict | None = None,
    ):
        try:
            await self.http.post(
                "/alerts",
                json={
                    "agent": self.name,
                    "task_id": task_id,
                    "kind": kind,
                    "summary": summary,
                    "message": message,
                    "code": code,
                    "stage": stage,
                    "metadata": metadata or {},
                },
            )
        except Exception:
            pass

    async def get_handoffs(self, task_id: str) -> list[dict]:
        try:
            r = await self.http.get(f"/tasks/{task_id}/handoffs")
            r.raise_for_status()
            data = r.json()
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def _parse_feedback_history(self, raw) -> list[dict]:
        data = raw
        if isinstance(raw, str):
            txt = raw.strip()
            if not txt:
                return []
            try:
                data = json.loads(txt)
            except Exception:
                return []
        if not isinstance(data, list):
            return []
        out = []
        for item in data:
            if not isinstance(item, dict):
                continue
            feedback = str(item.get("feedback") or "").strip()
            if not feedback:
                continue
            resolved_at = str(item.get("resolved_at") or "").strip()
            out.append(
                {
                    "id": str(item.get("id") or "").strip(),
                    "created_at": str(item.get("created_at") or "").strip(),
                    "source": str(item.get("source") or "").strip() or "system",
                    "status_at": str(item.get("status_at") or "").strip(),
                    "stage": str(item.get("stage") or "").strip(),
                    "actor": str(item.get("actor") or "").strip(),
                    "feedback": feedback[:1200],
                    "resolved": bool(item.get("resolved")) or bool(resolved_at),
                    "resolved_at": resolved_at,
                    "resolved_reason": str(item.get("resolved_reason") or "").strip(),
                }
            )
        return out

    def _fit_recent_lines(self, newest_lines: list[str], char_budget: int) -> list[str]:
        if char_budget <= 0:
            return []
        kept: list[str] = []
        used = 0
        for line in newest_lines:
            add = len(line) + 1
            if kept and used + add > char_budget:
                break
            if not kept and add > char_budget:
                kept.append(line[: max(10, char_budget - 1)])
                break
            kept.append(line)
            used += add
        return kept

    def _fmt_context_time(self, raw) -> str:
        return str(raw or "").strip().replace("T", " ")[:19]

    def _feedback_meta_line(self, item: dict) -> str:
        fid = str(item.get("id") or "").strip() or "FB????"
        stage = str(item.get("stage") or "").strip() or "-"
        actor = str(item.get("actor") or "").strip() or str(item.get("source") or "").strip() or "-"
        status_at = str(item.get("status_at") or "").strip()
        created = self._fmt_context_time(item.get("created_at"))
        parts = [fid, stage, actor]
        if status_at:
            parts.append(f"status={status_at}")
        if created:
            parts.append(created)
        return " | ".join(parts)

    def _build_unresolved_feedback_lines(self, task: dict | None) -> list[str]:
        if not task:
            return []
        history = self._parse_feedback_history(task.get("review_feedback_history"))
        unresolved = [h for h in history if not bool(h.get("resolved"))]
        unresolved = unresolved[-max(1, CONTEXT_MAX_UNRESOLVED_FEEDBACK) :]

        # Backward compatibility for historical rows without history payload.
        if not unresolved:
            status = str(task.get("status") or "").strip().lower()
            fallback_feedback = str(task.get("review_feedback") or "").strip()
            if fallback_feedback and status in {"needs_changes", "blocked"}:
                unresolved = [
                    {
                        "id": "FB0000",
                        "source": "legacy",
                        "stage": "legacy_feedback",
                        "actor": "legacy",
                        "status_at": status,
                        "created_at": str(task.get("updated_at") or "").strip(),
                        "feedback": fallback_feedback[:1200],
                    }
                ]
        if not unresolved:
            return []

        latest = unresolved[-1]
        older = list(reversed(unresolved[:-1]))
        lines = [
            "## 本次打回原因（仅处理未解决意见）",
            "- 只处理下面标记“未解决”的意见；已解决/已替代意见不要重复修改。",
        ]
        latest_feedback = str(latest.get("feedback") or "").strip()
        lines.append(
            f"- [未解决 | 本次打回 | {self._feedback_meta_line(latest)}] {latest_feedback[:360]}"
        )
        for item in older:
            feedback = str(item.get("feedback") or "").strip()
            if not feedback:
                continue
            lines.append(f"- [未解决 | 历史遗留 | {self._feedback_meta_line(item)}] {feedback[:280]}")
        return lines

    def _build_feedback_timeline_lines(self, task: dict | None) -> list[str]:
        if not task:
            return []
        history = self._parse_feedback_history(task.get("review_feedback_history"))
        if not history:
            status = str(task.get("status") or "").strip().lower()
            fallback_feedback = str(task.get("review_feedback") or "").strip()
            if fallback_feedback and status in {"needs_changes", "blocked"}:
                history = [
                    {
                        "id": "FB0000",
                        "source": "legacy",
                        "stage": "legacy_feedback",
                        "actor": "legacy",
                        "status_at": status,
                        "created_at": str(task.get("updated_at") or "").strip(),
                        "feedback": fallback_feedback[:1200],
                        "resolved": False,
                        "resolved_at": "",
                        "resolved_reason": "",
                    }
                ]
        if not history:
            return []

        recent = history[-max(1, CONTEXT_MAX_FEEDBACK_TIMELINE) :]
        lines = ["## 反馈时间线（最近记录）", "- 用于说明历次评审意见与解决情况。"]
        for item in reversed(recent):
            feedback = str(item.get("feedback") or "").strip()
            if not feedback:
                continue
            state = "未解决"
            if bool(item.get("resolved")):
                reason = str(item.get("resolved_reason") or "").strip()
                state = f"已解决:{reason or 'done'}"
            meta = self._feedback_meta_line(item)
            resolved_at = self._fmt_context_time(item.get("resolved_at"))
            if resolved_at:
                meta += f" | resolved={resolved_at}"
            lines.append(f"- [{state} | {meta}] {feedback[:220]}")
        return lines if len(lines) > 2 else []

    async def build_handoff_context(self, task_id: str, limit: int = 6) -> str:
        budget = max(1200, CONTEXT_MAX_CHARS)
        max_handoffs = max(limit, CONTEXT_MAX_HANDOFFS)
        sections: list[str] = []

        try:
            task = await self.get_task(task_id)
        except Exception:
            task = None

        unresolved_lines = self._build_unresolved_feedback_lines(task)
        if unresolved_lines:
            used = sum(len(s) + 2 for s in sections)
            unresolved_budget = min(
                max(420, int(budget * 0.35)),
                max(220, budget - used - 40),
            )
            unresolved_head = unresolved_lines[:2]
            unresolved_body = unresolved_lines[2:]
            body_budget = max(
                120,
                unresolved_budget - sum(len(x) + 1 for x in unresolved_head) - 10,
            )
            kept = self._fit_recent_lines(unresolved_body, body_budget)
            if kept:
                sections.append("\n".join([*unresolved_head, *kept]))
            elif unresolved_head:
                sections.append("\n".join(unresolved_head))

        timeline_lines = self._build_feedback_timeline_lines(task)
        if timeline_lines:
            used = sum(len(s) + 2 for s in sections)
            timeline_budget = min(
                max(360, int(budget * 0.30)),
                max(220, budget - used - 40),
            )
            timeline_head = timeline_lines[:2]
            timeline_body = timeline_lines[2:]
            body_budget = max(
                120,
                timeline_budget - sum(len(x) + 1 for x in timeline_head) - 10,
            )
            kept = self._fit_recent_lines(timeline_body, body_budget)
            if kept:
                sections.append("\n".join([*timeline_head, *kept]))
            elif timeline_head:
                sections.append("\n".join(timeline_head))

        handoffs = await self.get_handoffs(task_id)
        if handoffs:
            newest = []
            for h in reversed(handoffs[-max_handoffs:]):
                created = self._fmt_context_time(h.get("created_at"))
                stage = str(h.get("stage") or "").strip() or "-"
                from_agent = str(h.get("from_agent") or "").strip() or "-"
                to_agent = str(h.get("to_agent") or "").strip() or "-"
                summary = str(h.get("summary") or "").strip()
                commit_hash = str(h.get("commit_hash") or "").strip()
                patchset = self._extract_handoff_patchset(h) or {}
                patchset_id = str(patchset.get("id") or "").strip()
                conclusion = str(h.get("conclusion") or "").strip()
                info = conclusion or summary
                line = f"- {(created or '-')} | {from_agent} -> {to_agent} [{stage}]"
                if commit_hash:
                    line += f" commit={commit_hash}"
                if patchset_id:
                    line += f" patchset={patchset_id}"
                if info:
                    line += f" | {info[:180]}"
                newest.append(line)

            used = sum(len(s) + 2 for s in sections)
            handoff_budget = max(300, budget - used - 40)
            kept = self._fit_recent_lines(newest, handoff_budget)
            kept.reverse()
            if kept:
                omitted = max(0, len(newest) - len(kept))
                lines = ["## 历史交接记录（仅供时间线参考，非当前待办）", *kept]
                if omitted:
                    lines.append(f"- ... 已省略更早 {omitted} 条交接")
                sections.append("\n".join(lines))

        text = "\n\n".join(s for s in sections if s).strip()
        if len(text) > budget:
            return text[: budget - 20].rstrip() + "\n... (context truncated)"
        return text

    def _extract_handoff_commit(self, handoff: dict) -> str:
        commit_hash = str(handoff.get("commit_hash") or "").strip()
        if commit_hash:
            return commit_hash
        payload = handoff.get("payload")
        if isinstance(payload, dict):
            return str(payload.get("commit_hash") or "").strip()
        return ""

    def _extract_handoff_source_branch(self, handoff: dict) -> str:
        payload = handoff.get("payload")
        if isinstance(payload, dict):
            return str(payload.get("source_branch") or "").strip()
        return ""

    def _extract_handoff_related_commits(self, handoff: dict) -> list[dict]:
        payload = handoff.get("payload")
        if not isinstance(payload, dict):
            return []
        raw_items = payload.get("related_history_commits")
        if not isinstance(raw_items, list):
            return []
        out: list[dict] = []
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            commit_hash = str(item.get("hash") or item.get("commit_hash") or "").strip()
            if not commit_hash:
                continue
            out.append(
                {
                    "hash": commit_hash,
                    "short": str(item.get("short") or "").strip(),
                    "subject": str(item.get("subject") or "").strip(),
                    "created_at": str(item.get("created_at") or "").strip(),
                    "score": int(item.get("score") or 0),
                }
            )
        return out

    def _extract_handoff_commit_candidates(self, handoff: dict) -> list[str]:
        candidates: list[str] = []
        primary = self._extract_handoff_commit(handoff)
        if primary:
            candidates.append(primary)
        for item in self._extract_handoff_related_commits(handoff):
            h = str(item.get("hash") or "").strip()
            if h:
                candidates.append(h)
        # preserve order, dedupe
        uniq: list[str] = []
        seen: set[str] = set()
        for c in candidates:
            if c in seen:
                continue
            seen.add(c)
            uniq.append(c)
        return uniq

    async def resolve_handoff_commit_candidate(
        self,
        task_id: str,
        repo_root: Path,
    ) -> tuple[str, list[dict]]:
        handoff = await self.get_latest_handoff_for_agent(task_id)
        if not handoff:
            return "", []
        related = self._extract_handoff_related_commits(handoff)
        for commit_hash in self._extract_handoff_commit_candidates(handoff):
            try:
                await self.git("cat-file", "-e", f"{commit_hash}^{{commit}}", cwd=repo_root)
                return commit_hash, related
            except Exception:
                continue
        return "", related

    def _task_commit_keywords(self, task: dict) -> list[str]:
        text = " ".join(
            [
                str(task.get("id") or ""),
                str(task.get("title") or ""),
                str(task.get("description") or ""),
            ]
        ).lower()
        latin = [tok for tok in re.split(r"[^a-z0-9]+", text) if len(tok) >= 3]
        zh = re.findall(r"[\u4e00-\u9fff]{2,}", text)
        words = latin + zh
        # preserve order + dedupe
        out: list[str] = []
        seen: set[str] = set()
        for w in words:
            if w in seen:
                continue
            seen.add(w)
            out.append(w)
        return out[:24]

    async def collect_task_related_commits(
        self,
        task: dict,
        repo_root: Path,
        *,
        max_count: int = 6,
        scan_count: int = 120,
    ) -> list[dict]:
        try:
            raw = await self.git(
                "log",
                f"-n{max(20, int(scan_count))}",
                "--pretty=format:%H%x1f%h%x1f%s%x1f%ci",
                cwd=repo_root,
            )
        except Exception:
            return []
        keywords = self._task_commit_keywords(task)
        task_id = str(task.get("id") or "").strip().lower()
        rows: list[dict] = []
        for ln in str(raw or "").splitlines():
            parts = ln.split("\x1f")
            if len(parts) < 4:
                continue
            full_hash, short_hash, subject, created_at = parts[0], parts[1], parts[2], parts[3]
            hay = subject.lower()
            score = 0
            if task_id and task_id in hay:
                score += 3
            for kw in keywords:
                if kw and kw in hay:
                    score += 1
            rows.append(
                {
                    "hash": full_hash.strip(),
                    "short": short_hash.strip(),
                    "subject": subject.strip(),
                    "created_at": created_at.strip(),
                    "score": score,
                }
            )
        if not rows:
            return []
        ranked = sorted(rows, key=lambda x: (int(x.get("score") or 0), str(x.get("created_at") or "")), reverse=True)
        selected = [r for r in ranked if int(r.get("score") or 0) > 0]
        if not selected:
            selected = rows[:3]
        # dedupe by commit hash and trim
        out: list[dict] = []
        seen: set[str] = set()
        for item in selected:
            h = str(item.get("hash") or "").strip()
            if not h or h in seen:
                continue
            seen.add(h)
            out.append(item)
            if len(out) >= max(1, int(max_count)):
                break
        return out

    def _extract_handoff_patchset(self, handoff: dict) -> dict | None:
        payload = handoff.get("payload")
        if not isinstance(payload, dict):
            return None
        raw = payload.get("patchset")
        if not isinstance(raw, dict):
            return None
        head_sha = str(raw.get("head_sha") or raw.get("commit_hash") or "").strip()
        patchset_id = str(raw.get("id") or payload.get("patchset_id") or "").strip()
        if not head_sha and not patchset_id:
            return None
        commit_list = raw.get("commit_list")
        if isinstance(commit_list, str):
            try:
                commit_list = json.loads(commit_list)
            except Exception:
                commit_list = []
        if not isinstance(commit_list, list):
            commit_list = []
        normalized_list: list[dict] = []
        for item in commit_list:
            if not isinstance(item, dict):
                continue
            commit_hash = str(item.get("hash") or item.get("commit_hash") or "").strip()
            if not commit_hash:
                continue
            normalized_list.append(
                {
                    "hash": commit_hash[:120],
                    "short": str(item.get("short") or commit_hash[:12]).strip()[:24],
                    "subject": str(item.get("subject") or "").strip()[:240],
                }
            )
        worktree_clean = raw.get("worktree_clean")
        if isinstance(worktree_clean, str):
            worktree_clean = worktree_clean.strip().lower() not in {"0", "false", "off", "no"}
        elif worktree_clean is None:
            worktree_clean = True
        changed_files = raw.get("changed_files")
        if isinstance(changed_files, str):
            try:
                changed_files = json.loads(changed_files)
            except Exception:
                changed_files = []
        if not isinstance(changed_files, list):
            changed_files = []
        normalized_changed_files: list[dict] = []
        for item in changed_files:
            if not isinstance(item, dict):
                continue
            path = str(item.get("path") or item.get("new_path") or "").strip()
            if not path:
                continue
            normalized_item = {
                "status": str(item.get("status") or "M").strip()[:16] or "M",
                "path": path[:500],
            }
            old_path = str(item.get("old_path") or "").strip()
            if old_path:
                normalized_item["old_path"] = old_path[:500]
            normalized_changed_files.append(normalized_item)
            if len(normalized_changed_files) >= 256:
                break
        artifact_manifest = raw.get("artifact_manifest")
        if isinstance(artifact_manifest, str):
            try:
                artifact_manifest = json.loads(artifact_manifest)
            except Exception:
                artifact_manifest = {}
        if not isinstance(artifact_manifest, dict):
            artifact_manifest = {}
        return {
            "id": patchset_id[:80],
            "source_branch": str(raw.get("source_branch") or payload.get("source_branch") or "").strip(),
            "base_sha": str(raw.get("base_sha") or payload.get("base_sha") or "").strip(),
            "head_sha": head_sha[:120],
            "commit_count": int(raw.get("commit_count") or len(normalized_list) or (1 if head_sha else 0)),
            "commit_list": normalized_list,
            "diff_stat": str(raw.get("diff_stat") or payload.get("diff_stat") or "").strip(),
            "status": str(raw.get("status") or payload.get("patchset_status") or "").strip(),
            "worktree_clean": bool(worktree_clean),
            "merge_strategy": str(raw.get("merge_strategy") or payload.get("merge_strategy") or "").strip(),
            "summary": str(raw.get("summary") or payload.get("conclusion") or "").strip(),
            "artifact_path": str(raw.get("artifact_path") or handoff.get("artifact_path") or "").strip(),
            "queue_status": str(raw.get("queue_status") or payload.get("queue_status") or "").strip(),
            "queue_reason": str(raw.get("queue_reason") or payload.get("queue_reason") or "").strip(),
            "queued_at": str(raw.get("queued_at") or payload.get("queued_at") or "").strip(),
            "queue_started_at": str(raw.get("queue_started_at") or payload.get("queue_started_at") or "").strip(),
            "queue_finished_at": str(raw.get("queue_finished_at") or payload.get("queue_finished_at") or "").strip(),
            "approved_at": str(raw.get("approved_at") or payload.get("approved_at") or "").strip(),
            "merged_at": str(raw.get("merged_at") or payload.get("merged_at") or "").strip(),
            "reviewed_main_sha": str(raw.get("reviewed_main_sha") or payload.get("reviewed_main_sha") or "").strip(),
            "queue_main_sha": str(raw.get("queue_main_sha") or payload.get("queue_main_sha") or "").strip(),
            "changed_files": normalized_changed_files,
            "artifact_manifest": artifact_manifest,
        }

    def _task_patchset_from_task(self, task: dict | None) -> dict | None:
        item = task or {}
        patchset_id = str(item.get("current_patchset_id") or "").strip()
        status = str(item.get("current_patchset_status") or "").strip()
        if not patchset_id and not status:
            return None
        head_sha = str(item.get("commit_hash") or "").strip()
        return {
            "id": patchset_id[:80],
            "base_sha": "",
            "head_sha": head_sha[:120],
            "source_branch": "",
            "commit_count": 1 if head_sha else 0,
            "commit_list": [],
            "diff_stat": "",
            "status": status,
            "worktree_clean": True,
            "merge_strategy": "",
            "summary": "",
            "artifact_path": "",
            "queue_status": "",
            "queue_reason": "",
            "queued_at": "",
            "queue_started_at": "",
            "queue_finished_at": "",
            "approved_at": "",
            "merged_at": "",
            "reviewed_main_sha": "",
            "queue_main_sha": "",
            "changed_files": [],
            "artifact_manifest": {},
        }

    def _patchset_snapshots_match(self, primary: dict | None, secondary: dict | None) -> bool:
        first = primary or {}
        second = secondary or {}
        first_id = str(first.get("id") or "").strip()
        second_id = str(second.get("id") or "").strip()
        if first_id and second_id:
            return first_id == second_id
        first_head = str(first.get("head_sha") or first.get("commit_hash") or "").strip()
        second_head = str(second.get("head_sha") or second.get("commit_hash") or "").strip()
        if first_head and second_head:
            return first_head == second_head
        return False

    async def resolve_task_patchset(self, task: dict) -> dict | None:
        patchset = self._task_patchset_from_task(task)
        task_id = str(task.get("id") or "").strip()
        if not task_id:
            return patchset
        handoff = await self.get_latest_handoff_for_agent(task_id)
        if handoff:
            handoff_patchset = self._extract_handoff_patchset(handoff)
            if handoff_patchset and patchset:
                if not self._patchset_snapshots_match(patchset, handoff_patchset):
                    return patchset
                merged = dict(patchset)
                for key, value in handoff_patchset.items():
                    if value not in ("", [], None):
                        merged[key] = value
                return merged
            if handoff_patchset:
                return handoff_patchset
        return patchset

    async def build_patchset_snapshot(
        self,
        repo_root: Path,
        *,
        source_branch: str,
        head_sha: str | None = None,
    ) -> dict:
        if not head_sha:
            head_sha = (await self.git("rev-parse", "HEAD", cwd=repo_root)).strip()
        base_sha = ""
        try:
            base_sha = (await self.git("merge-base", "main", head_sha, cwd=repo_root)).strip()
        except Exception:
            base_sha = ""

        if base_sha:
            rev_range = f"{base_sha}..{head_sha}"
        else:
            rev_range = head_sha

        commit_list: list[dict] = []
        try:
            raw_commits = await self.git(
                "log",
                "--reverse",
                "--pretty=format:%H%x1f%h%x1f%s",
                rev_range,
                cwd=repo_root,
            )
            for line in str(raw_commits or "").splitlines():
                parts = line.split("\x1f")
                if len(parts) < 3:
                    continue
                commit_list.append(
                    {
                        "hash": parts[0].strip()[:120],
                        "short": parts[1].strip()[:24],
                        "subject": parts[2].strip()[:240],
                    }
                )
        except Exception:
            commit_list = []

        diff_stat = ""
        try:
            if base_sha:
                diff_stat = await self.git("diff", "--stat", f"{base_sha}..{head_sha}", cwd=repo_root)
            else:
                diff_stat = await self.git("show", "--stat", "--format=", head_sha, cwd=repo_root)
        except Exception:
            diff_stat = ""

        worktree_clean = True
        try:
            worktree_clean = not bool((await self.git("status", "--porcelain", cwd=repo_root)).strip())
        except Exception:
            worktree_clean = True
        changed_files = await self._collect_patchset_changed_files(
            repo_root,
            base_sha=base_sha,
            head_sha=str(head_sha or "").strip(),
        )
        artifact_manifest = self._read_patchset_artifact_manifest(repo_root)

        payload = {
            "id": "",
            "source_branch": str(source_branch or "").strip(),
            "base_sha": base_sha,
            "head_sha": str(head_sha or "").strip()[:120],
            "commit_count": len(commit_list) if commit_list else (1 if head_sha else 0),
            "commit_list": commit_list,
            "diff_stat": str(diff_stat or "").strip()[:4000],
            "status": "",
            "worktree_clean": worktree_clean,
            "merge_strategy": "squash",
            "summary": "",
            "artifact_path": str(repo_root),
            "created_by_agent": self.name,
            "queue_status": "",
            "queue_reason": "",
            "queued_at": "",
            "queue_started_at": "",
            "queue_finished_at": "",
            "approved_at": "",
            "merged_at": "",
            "reviewed_main_sha": "",
            "queue_main_sha": "",
            "changed_files": changed_files,
            "artifact_manifest": artifact_manifest,
        }
        raw_id = f"{payload['source_branch']}|{payload['base_sha']}|{payload['head_sha']}"
        payload["id"] = f"ps_{hashlib.sha1(raw_id.encode('utf-8')).hexdigest()[:24]}"
        return payload

    async def enrich_patchset_snapshot(
        self,
        repo_root: Path,
        patchset: dict | None,
        *,
        source_branch: str = "",
    ) -> dict:
        data = dict(patchset or {})
        head_sha = str(data.get("head_sha") or data.get("commit_hash") or "").strip()
        if not head_sha:
            return data
        base_sha = str(data.get("base_sha") or "").strip()
        if not isinstance(data.get("changed_files"), list) or not data.get("changed_files"):
            data["changed_files"] = await self._collect_patchset_changed_files(
                repo_root,
                base_sha=base_sha,
                head_sha=head_sha,
            )
        artifact_manifest = data.get("artifact_manifest")
        if not isinstance(artifact_manifest, dict) or not artifact_manifest:
            data["artifact_manifest"] = self._read_patchset_artifact_manifest(repo_root)
        if not str(data.get("artifact_path") or "").strip():
            data["artifact_path"] = str(repo_root)
        if source_branch and not str(data.get("source_branch") or "").strip():
            data["source_branch"] = str(source_branch).strip()
        return data

    async def _collect_patchset_changed_files(
        self,
        repo_root: Path,
        *,
        base_sha: str,
        head_sha: str,
    ) -> list[dict]:
        if not head_sha:
            return []
        try:
            if base_sha:
                raw = await self.git(
                    "diff",
                    "--name-status",
                    "--find-renames",
                    "--find-copies",
                    f"{base_sha}..{head_sha}",
                    cwd=repo_root,
                )
            else:
                raw = await self.git("show", "--name-status", "--format=", head_sha, cwd=repo_root)
        except Exception:
            return []
        changed_files: list[dict] = []
        for raw_line in str(raw or "").splitlines():
            line = raw_line.strip()
            if not line:
                continue
            parts = [part.strip() for part in line.split("\t") if part.strip()]
            if len(parts) < 2:
                continue
            status = parts[0][:16] or "M"
            if len(parts) >= 3 and status.startswith(("R", "C")):
                old_path = parts[1][:500]
                path = parts[2][:500]
                changed_files.append({"status": status, "path": path, "old_path": old_path})
            else:
                changed_files.append({"status": status, "path": parts[1][:500]})
            if len(changed_files) >= 256:
                break
        return changed_files

    def _read_patchset_artifact_manifest(self, repo_root: Path) -> dict:
        candidates = (
            ".opc/delivery.json",
            ".opc/handoff.json",
            ".opc/manifest.json",
        )
        for rel_path in candidates:
            path = repo_root / rel_path
            if not path.exists() or not path.is_file():
                continue
            try:
                raw_text = path.read_text(encoding="utf-8", errors="replace")
            except Exception as exc:
                return {"path": rel_path, "error": f"read_failed:{str(exc)[:120]}"}
            try:
                parsed = json.loads(raw_text)
            except Exception:
                return {"path": rel_path, "error": "invalid_json"}
            manifest: dict[str, object] = {
                "path": rel_path,
                "data": self._sanitize_manifest_value(parsed),
            }
            if isinstance(parsed, dict):
                manifest["keys"] = [str(key)[:120] for key in list(parsed.keys())[:24]]
            return manifest
        return {}

    def _sanitize_manifest_value(self, value, *, depth: int = 0):
        if depth >= 4:
            return "..."
        if value is None or isinstance(value, (bool, int, float)):
            return value
        if isinstance(value, str):
            return value[:500]
        if isinstance(value, list):
            return [self._sanitize_manifest_value(item, depth=depth + 1) for item in value[:32]]
        if isinstance(value, dict):
            out: dict[str, object] = {}
            for key, item in list(value.items())[:40]:
                out[str(key)[:120]] = self._sanitize_manifest_value(item, depth=depth + 1)
            return out
        return str(value)[:500]

    async def get_latest_handoff_for_agent(self, task_id: str) -> dict | None:
        handoffs = await self.get_handoffs(task_id)
        if not handoffs:
            return None
        me = safe_agent_key(self.name)
        for h in reversed(handoffs):
            to_agent = safe_agent_key(h.get("to_agent"))
            if to_agent and to_agent == me:
                return h
        for h in reversed(handoffs):
            if self._extract_handoff_commit(h):
                return h
        return handoffs[-1]

    async def _is_commit_in_head(self, worktree: Path, commit_hash: str) -> bool:
        try:
            await self.git("merge-base", "--is-ancestor", commit_hash, "HEAD", cwd=worktree)
            return True
        except Exception:
            return False

    async def sync_from_latest_handoff(
        self,
        task: dict,
        worktree: Path,
        current_branch: str | None = None,
    ) -> dict:
        strategy = HANDOFF_SYNC_STRATEGY
        if strategy in ("", "none", "off", "disabled"):
            return {"status": "disabled"}

        task_id = task["id"]
        handoff = await self.get_latest_handoff_for_agent(task_id)
        if not handoff:
            return {"status": "no_handoff"}

        commit_hash = self._extract_handoff_commit(handoff) or str(task.get("commit_hash") or "").strip()
        if not commit_hash:
            return {"status": "no_commit"}

        source_branch = self._extract_handoff_source_branch(handoff)
        if current_branch is None:
            try:
                current_branch = await self.git("branch", "--show-current", cwd=worktree)
            except Exception:
                current_branch = ""

        if source_branch and current_branch and source_branch == current_branch:
            return {"status": "same_branch", "commit_hash": commit_hash, "source_branch": source_branch}

        try:
            await self.git("cat-file", "-e", f"{commit_hash}^{{commit}}", cwd=worktree)
        except Exception as e:
            msg = f"[系统错误] 交接引用的 commit 不存在：{commit_hash} ({e})"
            await self.add_alert(
                summary="交接 commit 不存在",
                task_id=task_id,
                message=msg,
                kind="error",
                code=f"{self.name}_sync_missing_commit",
                stage=f"{self.name}_sync_failed",
                metadata={"commit_hash": commit_hash, "source_branch": source_branch},
            )
            await self.transition_task(
                task_id,
                fields={
                    "status": "blocked",
                    "assignee": None,
                    "assigned_agent": self.name,
                    "review_feedback": msg[:500],
                    "feedback_source": self.name,
                    "feedback_stage": f"{self.name}_sync_failed",
                    "feedback_actor": self.name,
                },
                handoff={
                    "stage": f"{self.name}_sync_failed",
                    "to_agent": self.name,
                    "status_from": task.get("status"),
                    "status_to": "blocked",
                    "title": "交接同步失败",
                    "summary": msg[:300],
                    "commit_hash": commit_hash,
                    "conclusion": "交接 commit 丢失，任务阻塞",
                    "payload": {"reason": "missing_commit_object", "source_branch": source_branch},
                },
                log_message=msg[:500],
            )
            return {"status": "failed", "reason": "missing_commit_object"}

        if await self._is_commit_in_head(worktree, commit_hash):
            return {"status": "already_contains", "commit_hash": commit_hash, "source_branch": source_branch}

        try:
            if strategy == "merge":
                await self.git("merge", "--no-edit", "--no-ff", commit_hash, cwd=worktree)
            else:
                await self.git("cherry-pick", "-x", commit_hash, cwd=worktree)
        except Exception as e:
            err = str(e)
            try:
                if strategy == "merge":
                    await self.git("merge", "--abort", cwd=worktree)
                else:
                    await self.git("cherry-pick", "--abort", cwd=worktree)
            except Exception:
                pass
            msg = f"[系统错误] 交接 commit 同步失败：{commit_hash}（{strategy}）{err[:260]}"
            await self.add_alert(
                summary="交接同步失败",
                task_id=task_id,
                message=msg,
                kind="error",
                code=f"{self.name}_sync_failed",
                stage=f"{self.name}_sync_failed",
                metadata={
                    "commit_hash": commit_hash,
                    "source_branch": source_branch,
                    "strategy": strategy,
                },
            )
            await self.transition_task(
                task_id,
                fields={
                    "status": "blocked",
                    "assignee": None,
                    "assigned_agent": self.name,
                    "review_feedback": msg[:500],
                    "feedback_source": self.name,
                    "feedback_stage": f"{self.name}_sync_failed",
                    "feedback_actor": self.name,
                },
                handoff={
                    "stage": f"{self.name}_sync_failed",
                    "to_agent": self.name,
                    "status_from": task.get("status"),
                    "status_to": "blocked",
                    "title": "交接同步失败",
                    "summary": msg[:300],
                    "commit_hash": commit_hash,
                    "conclusion": "交接同步冲突/失败，任务阻塞",
                    "payload": {
                        "reason": "sync_failed",
                        "strategy": strategy,
                        "source_branch": source_branch,
                    },
                    "artifact_path": str(worktree),
                },
                log_message=msg[:500],
            )
            return {"status": "failed", "reason": "sync_failed"}

        sync_msg = f"已同步交接 commit {commit_hash} 到当前分支（{strategy}）"
        await self.add_log(task_id, sync_msg)
        await self.add_handoff(
            task_id,
            stage=f"{self.name}_sync_in",
            to_agent=self.name,
            status_from=task.get("status"),
            status_to=task.get("status"),
            title="接收交接同步",
            summary=sync_msg,
            commit_hash=commit_hash,
            conclusion="已完成跨 Agent 代码同步",
            payload={
                "strategy": strategy,
                "source_branch": source_branch,
                "source_stage": handoff.get("stage"),
            },
            artifact_path=str(worktree),
        )
        return {"status": "synced", "commit_hash": commit_hash, "source_branch": source_branch}

    async def add_handoff(
        self,
        task_id: str,
        stage: str,
        to_agent: str | None = None,
        status_from: str | None = None,
        status_to: str | None = None,
        title: str = "",
        summary: str = "",
        commit_hash: str | None = None,
        conclusion: str | None = None,
        payload: dict | None = None,
        artifact_path: str | None = None,
    ):
        payload_obj = dict(payload or {})
        if commit_hash and not payload_obj.get("commit_hash"):
            payload_obj["commit_hash"] = commit_hash
        if conclusion and not payload_obj.get("conclusion"):
            payload_obj["conclusion"] = conclusion
        try:
            await self.http.post(
                f"/tasks/{task_id}/handoffs",
                json={
                    "stage": stage,
                    "from_agent": self.name,
                    "to_agent": to_agent,
                    "status_from": status_from,
                    "status_to": status_to,
                    "title": title,
                    "summary": summary,
                    "commit_hash": commit_hash,
                    "conclusion": conclusion,
                    "payload": payload_obj,
                    "artifact_path": artifact_path,
                    **self._transition_guard_fields(task_id),
                },
            )
        except Exception:
            pass

    # ── Agent terminal output ─────────────────────────────────────────────────

    def _post_output_bg(
        self,
        line: str,
        *,
        kind: str = "meta",
        event: str = "line",
        exit_code: int | None = None,
    ):
        task_id = self._active_task_id
        run_id = self._active_run_id
        project_id = str(self._active_project_id or self.project_id_scope or "").strip() or None
        important = kind in {"meta", "event"} or event in {"started", "finished"}
        if not important and self._output_post_sem.locked():
            # Drop low-priority stream lines when backlog is full.
            return

        async def _send():
            await self._output_post_sem.acquire()
            try:
                await self.http_output.post(
                    f"/agents/{self.name}/output",
                    json={
                        "line": line,
                        "kind": kind,
                        "event": event,
                        "exit_code": exit_code,
                        "agent_key": normalize_agent_key(self.name),
                        "worker_id": self.worker_id,
                        "project_id": project_id,
                        "task_id": task_id,
                        "run_id": run_id,
                    },
                    timeout=AGENT_POST_TIMEOUT_SECS,
                )
            except Exception:
                pass
            finally:
                self._output_post_sem.release()
        asyncio.create_task(_send())

    async def set_agent_status(
        self,
        status: str,
        task_title: str = "",
        *,
        project_id: str | None = None,
        task_id: str | None = None,
        run_id: str | None = None,
        lease_token: str | None = None,
        phase: str | None = None,
        pid: int | None = None,
    ):
        busy = str(status or "").strip().lower() == "busy"
        if busy:
            project_id = project_id if project_id is not None else (self._active_project_id or self.project_id_scope)
            task_id = task_id if task_id is not None else self._active_task_id
            run_id = run_id if run_id is not None else self._active_run_id
            lease_token = lease_token if lease_token is not None else self._active_lease_token
            phase = phase if phase is not None else self._active_phase
            pid = pid if pid is not None else self._active_cli_pid
        else:
            project_id = project_id if project_id is not None else self.project_id_scope
            task_id = ""
            run_id = ""
            lease_token = ""
            phase = ""
            pid = None
        try:
            await self.http.post(
                f"/agents/{self.name}/status",
                json={
                    "status": status,
                    "task": task_title,
                    "agent_key": normalize_agent_key(self.name),
                    "worker_id": self.worker_id,
                    "project_id": project_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "lease_token": lease_token,
                    "phase": phase,
                    "pid": pid,
                },
                timeout=AGENT_POST_TIMEOUT_SECS,
            )
        except Exception:
            pass

    async def _terminate_proc_tree(self, proc: asyncio.subprocess.Process) -> None:
        """
        Best-effort termination for subprocess and any children.
        We start subprocesses in a new session and kill the whole process group.
        """
        if proc.returncode is not None:
            return
        try:
            if hasattr(os, "killpg"):
                os.killpg(proc.pid, signal.SIGKILL)
                return
        except ProcessLookupError:
            return
        except Exception:
            pass
        with contextlib.suppress(ProcessLookupError, Exception):
            proc.kill()

    def _cli_uses_pty(self) -> bool:
        return self.cli_name == "claude"

    # ── Git ───────────────────────────────────────────────────────────────────

    async def git(self, *args: str, cwd: Path, task_id: str | None = None) -> str:
        proc = await asyncio.create_subprocess_exec(
            "git", *args, cwd=str(cwd),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        watch_task_id = task_id or self._active_task_id

        async def cancel_watcher():
            if not watch_task_id:
                return
            while proc.returncode is None:
                await asyncio.sleep(1)
                if proc.returncode is not None:
                    return
                if await self.is_task_cancelled(watch_task_id):
                    await self._terminate_proc_tree(proc)
                    return

        cwatch = asyncio.create_task(cancel_watcher())
        try:
            stdout, stderr = await proc.communicate()
        finally:
            cwatch.cancel()
        if proc.returncode != 0:
            raise RuntimeError(f"git {' '.join(args)} failed: {stderr.decode().strip()}")
        return stdout.decode().strip()

    # ── CLI runner ────────────────────────────────────────────────────────────

    async def run_cli(
        self,
        prompt: str,
        cwd: Path,
        task_id: str | None = None,
        output_schema: dict | None = None,
        expected_status: str | None = None,
        expected_assignee: str | None = None,
        reasoning_effort: str | None = None,
    ) -> tuple[int, str]:
        schema_path: Path | None = None
        last_message_path: Path | None = None
        if self.cli_name == "codex":
            cmd = ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox"]
            if str(reasoning_effort or "").strip():
                cmd += ["-c", f"model_reasoning_effort={json.dumps(str(reasoning_effort).strip())}"]
            try:
                fd_last, raw_last = tempfile.mkstemp(
                    prefix="opc-codex-last-", suffix=".txt", dir=str(cwd)
                )
                os.close(fd_last)
                last_message_path = Path(raw_last)
                cmd += ["--output-last-message", str(last_message_path)]
            except Exception:
                last_message_path = None

            if output_schema is not None and CODEX_ENABLE_OUTPUT_SCHEMA:
                try:
                    fd_schema, raw_schema = tempfile.mkstemp(
                        prefix="opc-codex-schema-", suffix=".json", dir=str(cwd)
                    )
                    with os.fdopen(fd_schema, "w", encoding="utf-8") as f:
                        json.dump(output_schema, f, ensure_ascii=False)
                    schema_path = Path(raw_schema)
                    cmd += ["--output-schema", str(schema_path)]
                except Exception:
                    schema_path = None
            elif output_schema is not None:
                self._post_output_bg("ℹ codex output_schema 已禁用（使用文件产物做结构化校验）", kind="meta")

            cmd.append(prompt)
        else:
            cmd = build_cli_cmd(self.cli_name, prompt)
        print(f"[{self.name}] Spawning {cmd[0]} (cwd={cwd.name})")
        self._post_output_bg(f"$ {cmd[0]}  cwd={cwd}", kind="meta")
        env = os.environ.copy()
        # Hint CLIs to avoid interactive flows.
        env.setdefault("CI", "1")
        env.setdefault("NONINTERACTIVE", "1")
        use_pty = self._cli_uses_pty()
        if use_pty:
            env.setdefault("TERM", "xterm-256color")

        master_fd: int | None = None
        slave_fd: int | None = None
        self._active_phase = "cli_spawning"
        try:
            if use_pty:
                master_fd, slave_fd = pty.openpty()
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=str(cwd),
                    stdin=slave_fd,
                    stdout=slave_fd,
                    stderr=slave_fd,
                    env=env,
                    preexec_fn=lambda: _prepare_tty_child(slave_fd),
                    close_fds=True,
                )
            else:
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=str(cwd),
                    stdin=asyncio.subprocess.PIPE,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                    start_new_session=True,
                )
        except Exception:
            for fd in (master_fd, slave_fd):
                if fd is None:
                    continue
                with contextlib.suppress(OSError):
                    os.close(fd)
            raise
        finally:
            if slave_fd is not None:
                with contextlib.suppress(OSError):
                    os.close(slave_fd)
        self._active_cli_pid = int(proc.pid) if proc.pid is not None else None
        self._active_phase = "cli_running"
        self._post_output_bg(
            f"▶ CLI 已启动: {cmd[0]} (pid={self._active_cli_pid or '-'})",
            kind="event",
            event="started",
        )

        lines: list[str] = []
        start_time = time.monotonic()
        last_output_at = time.monotonic()
        auto_reply_count = 0
        # At most one idle-triggered auto-reply for the same output phase.
        idle_reply_output_marker: float | None = None
        auto_reply_lock = asyncio.Lock()

        async def emit_output_line(line: str, stream_kind: str):
            nonlocal last_output_at
            clean = ANSI_ESCAPE_RE.sub("", line).rstrip("\r\n")
            if not clean:
                return
            last_output_at = time.monotonic()
            lines.append(clean)
            self._post_output_bg(clean, kind=stream_kind)
            if INTERACTIVE_PROMPT_RE.search(clean):
                await auto_reply(f"检测到交互提示: {clean}")

        async def write_cli_input(text: str) -> bool:
            if proc.returncode is not None:
                return False
            payload = text.encode("utf-8", errors="ignore")
            if use_pty:
                if master_fd is None:
                    return False
                try:
                    await asyncio.to_thread(os.write, master_fd, payload)
                    return True
                except Exception:
                    return False
            if proc.stdin is None:
                return False
            try:
                proc.stdin.write(payload)
                await proc.stdin.drain()
                return True
            except Exception:
                return False

        async def auto_reply(reason: str):
            nonlocal auto_reply_count, idle_reply_output_marker
            if AUTO_REPLY_MAX <= 0:
                return
            if auto_reply_count >= AUTO_REPLY_MAX:
                return
            async with auto_reply_lock:
                if auto_reply_count >= AUTO_REPLY_MAX:
                    return
                if not await write_cli_input(AUTO_REPLY_TEXT):
                    return
                auto_reply_count += 1
                idle_reply_output_marker = last_output_at
                msg = f"↩ 自动应答({auto_reply_count}/{AUTO_REPLY_MAX}) ENTER [{reason[:80]}]"
                lines.append(msg)
                self._post_output_bg(msg, kind="meta")
                if task_id:
                    await self.add_log(task_id, msg)

        async def drain(stream, stream_kind: str):
            async for raw in stream:
                line = raw.decode(errors="replace").rstrip("\n")
                await emit_output_line(line, stream_kind)

        async def drain_pty(fd: int, stream_kind: str):
            loop = asyncio.get_running_loop()
            queue: asyncio.Queue[bytes | BaseException | None] = asyncio.Queue()
            pending = ""

            def on_readable():
                try:
                    chunk = os.read(fd, 4096)
                except OSError as exc:
                    item: bytes | BaseException | None
                    item = None if exc.errno == errno.EIO else exc
                    with contextlib.suppress(Exception):
                        loop.remove_reader(fd)
                    queue.put_nowait(item)
                    return
                if not chunk:
                    with contextlib.suppress(Exception):
                        loop.remove_reader(fd)
                    queue.put_nowait(None)
                    return
                queue.put_nowait(chunk)

            async def flush_pending(*, final: bool = False):
                nonlocal pending
                normalized = pending.replace("\r\n", "\n").replace("\r", "\n")
                parts = normalized.split("\n")
                pending = "" if final else parts.pop()
                for part in parts:
                    await emit_output_line(part, stream_kind)
                if final and pending:
                    await emit_output_line(pending, stream_kind)
                    pending = ""

            loop.add_reader(fd, on_readable)
            try:
                while True:
                    item = await queue.get()
                    if item is None:
                        await flush_pending(final=True)
                        return
                    if isinstance(item, BaseException):
                        raise item
                    pending += item.decode(errors="replace")
                    await flush_pending()
            finally:
                with contextlib.suppress(Exception):
                    loop.remove_reader(fd)

        async def heartbeat():
            while True:
                await asyncio.sleep(HEARTBEAT_SECS)
                if proc.returncode is not None:
                    return
                elapsed = int(time.monotonic() - start_time)
                msg = f"⏳ 仍在工作中... 已运行 {elapsed}s"
                self._post_output_bg(msg, kind="meta")
                if task_id:
                    await self.add_log(task_id, msg)

        async def idle_auto_reply():
            if AUTO_REPLY_IDLE_SECS <= 0:
                return
            while True:
                await asyncio.sleep(AUTO_REPLY_IDLE_SECS)
                if proc.returncode is not None:
                    return
                idle = time.monotonic() - last_output_at
                if idle < AUTO_REPLY_IDLE_SECS:
                    continue
                # Prevent blind periodic ENTER every interval with no new output.
                if idle_reply_output_marker == last_output_at:
                    continue
                await auto_reply(f"idle {int(idle)}s")

        async def cancel_watcher():
            if not task_id:
                return
            expected_status_lc = str(expected_status or "").strip().lower()
            expected_assignee_lc = str(expected_assignee or "").strip().lower()
            consecutive_api_errors = 0
            while proc.returncode is None:
                await asyncio.sleep(1)
                if proc.returncode is not None:
                    return
                if self._active_task_id == task_id and self._active_lease_lost:
                    msg = "🛑 检测到任务租约失效，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg, kind="meta")
                    await self._terminate_proc_tree(proc)
                    return
                try:
                    task = await self.get_task_if_exists(task_id)
                    consecutive_api_errors = 0
                except Exception:
                    # Short transient API errors are tolerated. Prolonged control-plane
                    # failures should stop execution to avoid wedging the task.
                    consecutive_api_errors += 1
                    if consecutive_api_errors >= max(3, TASK_STATUS_POLL_FAILURE_MAX):
                        msg = (
                            f"🛑 连续 {consecutive_api_errors}s 无法获取任务状态，"
                            "终止当前 CLI 执行以避免任务卡住"
                        )
                        lines.append(msg)
                        self._post_output_bg(msg, kind="meta")
                        await self._terminate_proc_tree(proc)
                        return
                    continue
                if not task:
                    msg = "🛑 检测到任务不存在，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg, kind="meta")
                    await self._terminate_proc_tree(proc)
                    return
                status = str(task.get("status") or "").strip().lower()
                archived = int(task.get("archived") or 0)
                if status == "cancelled" or archived == 1:
                    msg = "🛑 检测到任务已取消/归档，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg, kind="meta")
                    await self._terminate_proc_tree(proc)
                    return
                if expected_status_lc and status != expected_status_lc:
                    msg = f"🛑 检测到任务状态变更为 {status}，终止当前 CLI 执行"
                    lines.append(msg)
                    self._post_output_bg(msg, kind="meta")
                    await self._terminate_proc_tree(proc)
                    return
                if expected_assignee_lc:
                    assignee = str(task.get("assignee") or "").strip().lower()
                    if assignee != expected_assignee_lc:
                        msg = (
                            f"🛑 检测到任务 assignee 变更为 {assignee or '(空)'}，"
                            "终止当前 CLI 执行"
                        )
                        lines.append(msg)
                        self._post_output_bg(msg, kind="meta")
                        await self._terminate_proc_tree(proc)
                        return

        hb = asyncio.create_task(heartbeat())
        iar = asyncio.create_task(idle_auto_reply())
        cwatch = asyncio.create_task(cancel_watcher())
        try:
            drain_tasks = (
                [drain_pty(master_fd, "stdout")]
                if use_pty and master_fd is not None
                else [drain(proc.stdout, "stdout"), drain(proc.stderr, "stderr")]
            )
            await asyncio.wait_for(
                asyncio.gather(*drain_tasks),
                timeout=CLI_TIMEOUT,
            )
        except asyncio.TimeoutError:
            self._active_phase = "cli_timeout"
            await self._terminate_proc_tree(proc)
            lines.append(f"[TIMEOUT after {CLI_TIMEOUT}s]")
            self._post_output_bg(f"⚠ TIMEOUT after {CLI_TIMEOUT}s", kind="meta")
        finally:
            for t in (hb, iar, cwatch):
                t.cancel()
            for t in (hb, iar, cwatch):
                with contextlib.suppress(asyncio.CancelledError):
                    await t

        await proc.wait()
        self._post_output_bg(
            f"■ CLI 结束: exit={proc.returncode}",
            kind="event",
            event="finished",
            exit_code=proc.returncode,
        )
        self._active_cli_pid = None
        if self._active_phase in {"cli_spawning", "cli_running", "cli_timeout"}:
            self._active_phase = "running"

        if last_message_path and last_message_path.exists():
            try:
                last_msg = last_message_path.read_text(encoding="utf-8", errors="replace").strip()
                if last_msg:
                    lines.append(last_msg)
            except Exception:
                pass

        for p in (last_message_path, schema_path):
            if not p:
                continue
            try:
                p.unlink(missing_ok=True)
            except Exception:
                pass
        if master_fd is not None:
            with contextlib.suppress(OSError):
                os.close(master_fd)

        return proc.returncode, "\n".join(lines)

    async def _sync_branch_with_main(self, root: Path, worktree: Path, branch: str) -> str:
        """
        Try to fast-sync branch with `main`.
        Strategy is controlled by BRANCH_SYNC_STRATEGY:
        - merge (default): merge --no-edit main
        - rebase: rebase main
        - none/off/disabled: skip sync
        Returns a short status string; never raises.
        """
        strategy = BRANCH_SYNC_STRATEGY
        if strategy in {"none", "off", "disabled"}:
            return "sync_disabled"
        if strategy not in {"merge", "rebase"}:
            strategy = "merge"

        if branch == "main":
            return "main"

        has_main = bool((await self.git("branch", "--list", "main", cwd=root)).strip())
        if not has_main:
            return "no_main"

        try:
            dirty = await self.git("status", "--porcelain", cwd=worktree)
        except Exception as e:
            return f"status_error:{e}"
        if dirty.strip():
            return "dirty"

        try:
            before = await self.git("rev-parse", "HEAD", cwd=worktree)
        except Exception as e:
            return f"head_error:{e}"

        try:
            if strategy == "rebase":
                await self.git("rebase", "main", cwd=worktree)
            else:
                await self.git("merge", "--no-edit", "main", cwd=worktree)
        except Exception as e:
            err = str(e)
            try:
                if strategy == "rebase":
                    await self.git("rebase", "--abort", cwd=worktree)
                else:
                    await self.git("merge", "--abort", cwd=worktree)
            except Exception:
                pass
            return f"conflict:{err}"

        try:
            after = await self.git("rev-parse", "HEAD", cwd=worktree)
            if before == after:
                return "up_to_date"
            return "rebased" if strategy == "rebase" else "merged"
        except Exception:
            return "rebased" if strategy == "rebase" else "synced"

    async def ensure_agent_workspace(
        self, task: dict, agent_key: str | None = None, sync_with_main: bool = True
    ) -> tuple[Path, Path, str]:
        """
        Ensure git branch+worktree for a given agent key.
        Branch: agent/{key}/{task_id}
        Worktree: .worktrees/{key}/{task_id}
        """
        key = normalize_agent_key(agent_key or get_task_dev_agent(task))
        branch = get_agent_branch(key, task=task)
        root, worktree = get_project_dirs(task, agent_key=key)
        root.mkdir(parents=True, exist_ok=True)

        # Bootstrap repo for scratch or non-initialized project paths.
        if not (root / ".git").exists():
            await self.git("init", cwd=root)
            await self.git("config", "user.email", "agent@opc-demo.local", cwd=root)
            await self.git("config", "user.name", "OPC Agent", cwd=root)
            try:
                await self.git("checkout", "-b", "main", cwd=root)
            except Exception:
                pass
            try:
                await self.git("commit", "--allow-empty", "-m", "chore: init project", cwd=root)
            except Exception:
                pass

        has_main = bool((await self.git("branch", "--list", "main", cwd=root)).strip())
        has_agent_branch = bool((await self.git("branch", "--list", branch, cwd=root)).strip())
        if not has_agent_branch:
            base_ref = "main" if has_main else "HEAD"
            await self.git("branch", branch, base_ref, cwd=root)

        if not worktree.exists():
            worktree.parent.mkdir(parents=True, exist_ok=True)
            await self.git("worktree", "add", str(worktree), branch, cwd=root)
        else:
            try:
                await self.git("checkout", branch, cwd=worktree)
            except Exception:
                pass

        await self.git("config", "user.email", "agent@opc-demo.local", cwd=worktree)
        await self.git("config", "user.name", "OPC Agent", cwd=worktree)
        self._ensure_runtime_git_excludes(root)
        self._ensure_runtime_git_excludes(worktree)

        if sync_with_main:
            sync_result = await self._sync_branch_with_main(root, worktree, branch)
            if sync_result == "merged":
                print(f"[{self.name}] Synced {branch} with main")
            elif sync_result == "rebased":
                print(f"[{self.name}] Rebased {branch} onto main")
            elif sync_result in ("dirty",) or sync_result.startswith("conflict:"):
                print(f"[{self.name}] WARN: main sync skipped for {branch}: {sync_result}")

        return root, worktree, branch

    # ── JSON decision parser ─────────────────────────────────────────────────

    def _normalize_decision_payload(self, payload: dict) -> dict | None:
        decision = str(payload.get("decision") or "").strip().lower()
        if decision not in {"approve", "request_changes"}:
            return None

        placeholder_values = {
            "简要说明通过原因",
            "条列说明需要修改的具体内容",
            "请修复问题",
            "...",
        }

        issues = normalize_issue_list(payload.get("issues"), default_status="open")

        if decision == "approve":
            comment = str(payload.get("comment") or "").strip()
            if not comment or comment in placeholder_values:
                return None
            if any(str(item.get("status") or "").strip().lower() in UNRESOLVED_ISSUE_STATUSES for item in issues):
                return None
            return {"decision": "approve", "comment": comment, "issues": issues}

        feedback = str(payload.get("feedback") or "").strip()
        if not feedback or feedback in placeholder_values:
            return None
        if not issues:
            issues = normalize_issue_list(
                [{"summary": feedback, "status": "open", "category": "other", "severity": "medium"}],
                default_status="open",
            )
        return {"decision": "request_changes", "feedback": feedback, "issues": issues}

    def parse_json_decision(self, text: str) -> dict | None:
        # Only parse near the tail to avoid picking JSON examples echoed from the prompt.
        tail = text[-6000:] if len(text) > 6000 else text

        for m in reversed(re.findall(r"```(?:json)?\s*(\{[\s\S]+?\})\s*```", tail)):
            try:
                d = json.loads(m)
                parsed = self._normalize_decision_payload(d)
                if parsed:
                    return parsed
            except json.JSONDecodeError:
                pass
        for m in reversed(re.findall(r"\{[^{}]*\}", tail)):
            try:
                d = json.loads(m)
                parsed = self._normalize_decision_payload(d)
                if parsed:
                    return parsed
            except json.JSONDecodeError:
                pass
        return None

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def process_task(self, task: dict):
        raise NotImplementedError

    def working_status_for(self, status: str) -> str:
        return self.working_status or status

    def respect_assignment_for(self, status: str) -> bool:
        return True

    async def run(self):
        if not shutil.which(self.cli_name):
            print(f"[{self.name}] WARNING: '{self.cli_name}' not found in PATH")
        print(f"[{self.name}] Starting (CLI={self.cli_name}, polls={self.poll_statuses})")
        await self.set_agent_status("idle")

        while not self.shutdown.is_set():
            for status in self.poll_statuses:
                if self.shutdown.is_set():
                    break
                try:
                    while not self.shutdown.is_set():
                        task = await self.claim_task(
                            status=status,
                            working_status=self.working_status_for(status),
                            respect_assignment=self.respect_assignment_for(status),
                        )
                        if not task:
                            break
                        task["_claimed_from_status"] = status
                        if self.shutdown.is_set():
                            break
                        print(f"[{self.name}] → '{task['title'][:40]}' ({status})")
                        self._active_task_id = task["id"]
                        self._active_run_id = str(task.get("claim_run_id") or "").strip() or None
                        self._active_lease_token = str(task.get("lease_token") or "").strip() or None
                        self._active_project_id = str(task.get("project_id") or "").strip() or self.project_id_scope
                        self._active_lease_lost = False
                        self._active_phase = "claimed"
                        self._active_cli_pid = None
                        await self.set_agent_status("busy", task["title"])
                        self._post_output_bg(f"▶ 任务: {task['title']}")
                        async def busy_status_heartbeat():
                            while True:
                                await asyncio.sleep(STATUS_HEARTBEAT_SECS)
                                await self.set_agent_status("busy", task["title"])

                        async def lease_heartbeat():
                            consecutive_errors = 0
                            while True:
                                await asyncio.sleep(TASK_LEASE_RENEW_INTERVAL_SECS)
                                if self._active_task_id != task["id"]:
                                    return
                                if self._active_lease_lost:
                                    return
                                run_id = str(self._active_run_id or "").strip()
                                lease_token = str(self._active_lease_token or "").strip()
                                if not run_id or not lease_token:
                                    return
                                renewed = await self.renew_task_lease(
                                    task_id=task["id"],
                                    run_id=run_id,
                                    lease_token=lease_token,
                                    lease_ttl_secs=TASK_LEASE_TTL_SECS,
                                )
                                if renewed is True:
                                    consecutive_errors = 0
                                    if self._active_phase in {"lease_retry", "claimed"}:
                                        self._active_phase = "running"
                                    continue
                                if renewed is False:
                                    self._active_lease_lost = True
                                    self._active_phase = "lease_lost"
                                    self._post_output_bg(
                                        "🛑 租约续期失败，任务可能已被回收/接管，停止当前执行"
                                    )
                                    return
                                consecutive_errors += 1
                                if consecutive_errors >= TASK_LEASE_RENEW_WARN_AFTER_ERRORS:
                                    self._active_phase = "lease_retry"
                                    self._post_output_bg(
                                        f"⚠ 租约续期连续失败 {consecutive_errors} 次（将继续重试）"
                                    )
                                if consecutive_errors >= TASK_LEASE_RENEW_FAIL_HARD_AFTER_ERRORS:
                                    self._active_lease_lost = True
                                    self._active_phase = "lease_unreachable"
                                    msg = (
                                        f"🛑 租约续期连续失败 {consecutive_errors} 次，"
                                        "主动终止当前执行并重新进入认领循环"
                                    )
                                    self._post_output_bg(msg)
                                    await self.add_log(task["id"], msg)
                                    return

                        busy_hb = asyncio.create_task(busy_status_heartbeat())
                        lease_hb = asyncio.create_task(lease_heartbeat())
                        try:
                            self._active_phase = "running"
                            await self.process_task(task)
                        except Exception as e:
                            self._active_phase = "failed"
                            await self.add_log(task["id"], f"错误: {e}")
                            await self.add_alert(
                                summary=f"{self.name} 运行异常",
                                task_id=task["id"],
                                message=str(e),
                                kind="error",
                                code=f"{self.name}_runtime_error",
                                stage="runtime_exception",
                            )
                            self._post_output_bg(f"✗ 错误: {e}")
                            print(f"[{self.name}] Error: {e}")
                        finally:
                            for t in (busy_hb, lease_hb):
                                t.cancel()
                            for t in (busy_hb, lease_hb):
                                with contextlib.suppress(asyncio.CancelledError):
                                    await t
                            self._active_task_id = None
                            self._active_run_id = None
                            self._active_lease_token = None
                            self._active_project_id = None
                            self._active_lease_lost = False
                            self._active_phase = ""
                            self._active_cli_pid = None
                        await self.set_agent_status("idle")
                        self._post_output_bg("─── 等待下一个任务 ───")
                except Exception as e:
                    msg = f"Poll error ({status}): {e}"
                    print(f"[{self.name}] {msg}")
                    self._post_output_bg(f"⚠ {msg[:300]}")

            # Sleep for POLL_INTERVAL, but wake up early if shutdown is set
            try:
                await asyncio.wait_for(self.shutdown.wait(), timeout=POLL_INTERVAL)
            except asyncio.TimeoutError:
                pass  # Normal timeout, continue polling

        self._post_output_bg(f"[{self.name}] 已收到关闭信号，停止接受新任务")
        print(f"[{self.name}] Stopped.")
        await self.http.aclose()
        await self.http_output.aclose()
