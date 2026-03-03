import json
import hashlib
import hmac
import re
import secrets
import sqlite3
import uuid
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "tasks.db"
CANCELLED_STATUS = "cancelled"
ACTIONABLE_FEEDBACK_STATUSES = {"needs_changes", "blocked"}
FEEDBACK_RESOLVE_STATUSES = {"approved", "pending_acceptance", "completed", CANCELLED_STATUS}
ROLE_ADMIN = "admin"
ROLE_USER = "user"
SESSION_TTL_DAYS = 30


class LeaseConflictError(RuntimeError):
    """Raised when task lease fence validation fails."""

DEVELOPER_PROMPT_DEFAULT = (
    "你是一名专业软件工程师，负责实现以下任务。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "{rework_section}\n\n"
    "## 工作要求\n\n"
    "1. **所有成果必须写入文件**，不要只在终端打印输出\n"
    "   - 代码任务 → 创建对应语言的源文件（.py / .ts / .go 等）\n"
    "   - 文档/方案任务 → 创建 `.md` 文件，把完整内容写入\n"
    "   - 至少创建一个文件，否则任务无法通过审查\n\n"
    "2. **质量标准**\n"
    "   - 代码需有适当注释，边界情况需处理\n"
    "   - 文档需完整、结构清晰\n\n"
    "3. **分支与交接约束**\n"
    "   - 在当前工作分支完成实现并提交，不要自行合并 main\n"
    "   - 提交后由 reviewer/manager 继续流程，不要跳过审查与合并环节\n"
    "   - 不要伪造“已合并/已发布”结论\n\n"
    "4. 直接开始实现，不需要解释计划"
)

REVIEWER_PROMPT_DEFAULT = (
    "你是资深代码/文档审查工程师，负责审查以下变更。\n\n"
    "## 任务信息\n\n"
    "**标题**：{task_title}\n\n"
    "**需求描述**：\n"
    "{task_description}\n\n"
    "## 变更内容\n\n"
    "```\n"
    "{diff}\n"
    "```\n\n"
    "## 审查要点\n\n"
    "- 是否完整实现了需求描述中的所有要求\n"
    "- 代码/内容是否正确，有无明显错误或遗漏\n"
    "- 代码质量、可读性、边界情况处理\n"
    "- 文件结构是否合理\n\n"
    "## 输出格式\n\n"
    "审查完毕后，在回复最后一行只输出一个 JSON 对象（不要代码块、不要额外文字）：\n"
    '- decision 只能是 "approve" 或 "request_changes"\n'
    '- decision="approve" 时必须提供 comment 字段\n'
    '- decision="request_changes" 时必须提供 feedback 字段'
)

MANAGER_PROMPT_DEFAULT = (
    "你是发布合并管理者，负责把经审查通过的目标 commit 从开发分支精确合并到 main。\n\n"
    "任务标题：{task_title}\n"
    "请确保只合并已审查的 commit_hash（不要直接合并分支 HEAD），并在冲突时停止自动流程。"
)

LEADER_PROMPT_DEFAULT = (
    "你是一个专业的项目评估与分解专家。请评估以下任务是否需要分解：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "## 评估标准\n"
    "- **简单任务**：可以由单个 agent 独立完成，工作量在 1-2 小时内\n"
    "- **复杂任务**：涉及多个独立功能模块，或需要不同专业技能协作\n\n"
    "## 子任务质量门槛（必须满足）\n"
    "1. 子任务必须可独立验收，禁止空泛措辞。\n"
    "2. 每个子任务必须包含：title/objective/todo_steps/deliverables/acceptance_criteria/agent。\n"
    "3. deliverables 要写清文件、接口、页面或脚本等可交付物。\n"
    "4. acceptance_criteria 至少 2 条，必须可验证。\n\n"
    "## 输出格式（严格 JSON，不要任何其他文字）\n\n"
    "如果是简单任务：\n"
    '{"action": "simple", "reason": "一句话说明为何不需要分解"}\n\n'
    "如果是复杂任务：\n"
    '{"action": "decompose", "subtasks": [\n'
    '  {"title":"子任务标题","objective":"子任务目标","todo_steps":["步骤1","步骤2"],"deliverables":["交付物1"],"acceptance_criteria":["验收1","验收2"],"agent":"developer"}\n'
    "]}"
)

BUILTIN_PROMPTS = {
    "developer": DEVELOPER_PROMPT_DEFAULT,
    "reviewer": REVIEWER_PROMPT_DEFAULT,
    "manager": MANAGER_PROMPT_DEFAULT,
    "leader": LEADER_PROMPT_DEFAULT,
}


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS projects (
            id         TEXT PRIMARY KEY,
            name       TEXT NOT NULL,
            path       TEXT NOT NULL UNIQUE,
            created_by_user_id TEXT,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS users (
            id            TEXT PRIMARY KEY,
            username      TEXT NOT NULL UNIQUE,
            password_hash TEXT,
            role          TEXT NOT NULL DEFAULT 'user',
            created_by    TEXT,
            created_at    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS sessions (
            id         TEXT PRIMARY KEY,
            user_id    TEXT NOT NULL,
            token_hash TEXT NOT NULL UNIQUE,
            created_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id              TEXT PRIMARY KEY,
            project_id      TEXT REFERENCES projects(id),
            title           TEXT NOT NULL,
            description     TEXT NOT NULL DEFAULT '',
            status          TEXT NOT NULL DEFAULT 'todo',
            assignee        TEXT,
            claim_run_id    TEXT,
            lease_token     TEXT,
            lease_expires_at TEXT,
            review_feedback TEXT,
            review_feedback_history TEXT NOT NULL DEFAULT '[]',
            commit_hash     TEXT,
            archived        INTEGER NOT NULL DEFAULT 0,
            created_at      TEXT NOT NULL,
            updated_at      TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS logs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id    TEXT NOT NULL,
            agent      TEXT NOT NULL,
            message    TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        );

        CREATE TABLE IF NOT EXISTS agent_outputs (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            agent      TEXT NOT NULL,
            task_id    TEXT,
            run_id     TEXT,
            line       TEXT NOT NULL,
            kind       TEXT NOT NULL DEFAULT 'line',
            event      TEXT NOT NULL DEFAULT 'line',
            exit_code  INTEGER,
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS task_handoffs (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id      TEXT NOT NULL,
            stage        TEXT NOT NULL,
            from_agent   TEXT NOT NULL,
            to_agent     TEXT,
            status_from  TEXT,
            status_to    TEXT,
            title        TEXT NOT NULL DEFAULT '',
            summary      TEXT NOT NULL DEFAULT '',
            commit_hash  TEXT,
            conclusion   TEXT,
            payload      TEXT NOT NULL DEFAULT '{}',
            artifact_path TEXT,
            created_at   TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        );

        CREATE TABLE IF NOT EXISTS agent_types (
            id             TEXT PRIMARY KEY,
            key            TEXT NOT NULL UNIQUE,
            name           TEXT NOT NULL,
            description    TEXT NOT NULL DEFAULT '',
            prompt         TEXT NOT NULL DEFAULT '',
            poll_statuses  TEXT NOT NULL DEFAULT '["todo"]',
            next_status    TEXT NOT NULL DEFAULT 'in_review',
            working_status TEXT NOT NULL DEFAULT 'in_progress',
            cli            TEXT NOT NULL DEFAULT 'claude',
            is_builtin     INTEGER NOT NULL DEFAULT 0,
            created_at     TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_agent_outputs_agent_id ON agent_outputs(agent, id);
        CREATE INDEX IF NOT EXISTS idx_agent_outputs_created_at ON agent_outputs(created_at);
    """)

    # Migrations for existing DBs: ensure all runtime-required columns exist.
    _ensure_columns(conn, "projects", [
        ("name", "TEXT NOT NULL DEFAULT ''"),
        ("path", "TEXT NOT NULL DEFAULT ''"),
        ("created_by_user_id", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "users", [
        ("username", "TEXT NOT NULL DEFAULT ''"),
        ("password_hash", "TEXT"),
        ("role", "TEXT NOT NULL DEFAULT 'user'"),
        ("created_by", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "sessions", [
        ("user_id", "TEXT NOT NULL DEFAULT ''"),
        ("token_hash", "TEXT NOT NULL DEFAULT ''"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ("expires_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "tasks", [
        ("project_id", "TEXT"),
        ("title", "TEXT NOT NULL DEFAULT ''"),
        ("description", "TEXT NOT NULL DEFAULT ''"),
        ("status", "TEXT NOT NULL DEFAULT 'todo'"),
        ("assignee", "TEXT"),
        ("claim_run_id", "TEXT"),
        ("lease_token", "TEXT"),
        ("lease_expires_at", "TEXT"),
        ("review_feedback", "TEXT"),
        ("review_feedback_history", "TEXT NOT NULL DEFAULT '[]'"),
        ("commit_hash", "TEXT"),
        ("archived", "INTEGER NOT NULL DEFAULT 0"),
        ("parent_task_id", "TEXT"),
        ("subtask_order", "INTEGER NOT NULL DEFAULT 0"),
        ("assigned_agent", "TEXT"),
        ("dev_agent", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
        ("updated_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "logs", [
        ("task_id", "TEXT"),
        ("agent", "TEXT"),
        ("message", "TEXT"),
        ("created_at", "TEXT"),
    ])
    _ensure_columns(conn, "agent_outputs", [
        ("agent", "TEXT"),
        ("task_id", "TEXT"),
        ("run_id", "TEXT"),
        ("line", "TEXT"),
        ("kind", "TEXT NOT NULL DEFAULT 'line'"),
        ("event", "TEXT NOT NULL DEFAULT 'line'"),
        ("exit_code", "INTEGER"),
        ("created_at", "TEXT"),
    ])
    _ensure_columns(conn, "task_handoffs", [
        ("task_id", "TEXT"),
        ("stage", "TEXT NOT NULL DEFAULT ''"),
        ("from_agent", "TEXT NOT NULL DEFAULT ''"),
        ("to_agent", "TEXT"),
        ("status_from", "TEXT"),
        ("status_to", "TEXT"),
        ("title", "TEXT NOT NULL DEFAULT ''"),
        ("summary", "TEXT NOT NULL DEFAULT ''"),
        ("commit_hash", "TEXT"),
        ("conclusion", "TEXT"),
        ("payload", "TEXT NOT NULL DEFAULT '{}'"),
        ("artifact_path", "TEXT"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])
    _ensure_columns(conn, "agent_types", [
        ("key", "TEXT"),
        ("name", "TEXT"),
        ("description", "TEXT NOT NULL DEFAULT ''"),
        ("prompt", "TEXT NOT NULL DEFAULT ''"),
        ("poll_statuses", "TEXT NOT NULL DEFAULT '[\"todo\"]'"),
        ("next_status", "TEXT NOT NULL DEFAULT 'in_review'"),
        ("working_status", "TEXT NOT NULL DEFAULT 'in_progress'"),
        ("cli", "TEXT NOT NULL DEFAULT 'claude'"),
        ("is_builtin", "INTEGER NOT NULL DEFAULT 0"),
        ("created_at", "TEXT NOT NULL DEFAULT ''"),
    ])

    _seed_admin_user(conn)
    _cleanup_expired_sessions(conn)
    _seed_builtin_agents(conn)
    _recover_reviewer_stuck_tasks(conn)
    _recover_invalid_todo_assignments(conn)
    _backfill_subtask_order(conn)
    conn.commit()
    conn.close()


def _ensure_columns(conn, table: str, columns: list[tuple[str, str]]):
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, defn in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {defn}")


def _seed_admin_user(conn):
    now = _now()
    row = conn.execute("SELECT id FROM users WHERE username='admin'").fetchone()
    if not row:
        conn.execute(
            """
            INSERT INTO users (id, username, password_hash, role, created_by, created_at)
            VALUES (?, 'admin', NULL, ?, NULL, ?)
            """,
            (str(uuid.uuid4()), ROLE_ADMIN, now),
        )
        return
    conn.execute(
        """
        UPDATE users
           SET role=?
         WHERE username='admin'
           AND COALESCE(TRIM(role), '') != ?
        """,
        (ROLE_ADMIN, ROLE_ADMIN),
    )


def _cleanup_expired_sessions(conn):
    conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (_now(),))


def _seed_builtin_agents(conn):
    """Insert built-in agent records if they don't exist yet."""
    builtins = [
        {
            "key": "developer",
            "name": "开发者",
            "description": "实现任务需求，在 agent/<agent> 工作分支提交并交接审查",
            "prompt": BUILTIN_PROMPTS["developer"],
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "reviewer",
            "name": "审查者",
            "description": "审查代码变更，决定通过或要求修改",
            "prompt": BUILTIN_PROMPTS["reviewer"],
            "poll_statuses": '["in_review"]',
            "next_status": "approved",
            "working_status": "reviewing",
        },
        {
            "key": "manager",
            "name": "合并管理者",
            "description": "将审查通过的目标 commit 精确合并到 main",
            "prompt": BUILTIN_PROMPTS["manager"],
            "poll_statuses": '["approved"]',
            "next_status": "pending_acceptance",
            "working_status": "merging",
        },
        {
            "key": "leader",
            "name": "分解专家",
            "description": "评估新任务复杂度：简单任务直接推进开发，复杂任务自动分解为子任务",
            "poll_statuses": '["triage","decompose"]',
            "next_status": "decomposed",
            "working_status": "triaging",
            "prompt": BUILTIN_PROMPTS["leader"],
        },
    ]
    now = _now()
    for b in builtins:
        exists = conn.execute("SELECT id FROM agent_types WHERE key=?", (b["key"],)).fetchone()
        if not exists:
            conn.execute(
                """INSERT INTO agent_types
                   (id,key,name,description,prompt,poll_statuses,next_status,working_status,cli,is_builtin,created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,1,?)""",
                (str(uuid.uuid4()), b["key"], b["name"], b["description"],
                 b.get("prompt", ""),
                 b["poll_statuses"], b["next_status"], b["working_status"], "claude", now),
            )
    # Migrate existing leader record to new triage-aware config
    conn.execute(
        """UPDATE agent_types
           SET poll_statuses='["triage","decompose"]', working_status='triaging'
           WHERE key='leader' AND (poll_statuses='["decompose"]' OR working_status='decomposing')"""
    )
    # Migrate legacy rows where built-in prompt was empty.
    for key, prompt in BUILTIN_PROMPTS.items():
        conn.execute(
            """UPDATE agent_types
               SET prompt=?
               WHERE key=? AND is_builtin=1 AND TRIM(COALESCE(prompt, ''))=''""",
            (prompt, key),
        )
    # Migrate legacy reviewer prompt that embedded example JSON bodies; those
    # examples can be echoed by CLI and misparsed as real decisions.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='reviewer' AND is_builtin=1
             AND INSTR(prompt, '条列说明需要修改的具体内容') > 0""",
        (BUILTIN_PROMPTS["reviewer"],),
    )
    # Migrate legacy leader prompt with weak/free-form subtask spec.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='leader' AND is_builtin=1
             AND INSTR(prompt, '输出格式') > 0
             AND INSTR(prompt, '子任务质量门槛') = 0
             AND (INSTR(prompt, 'acceptance_criteria') = 0 OR INSTR(prompt, 'todo_steps') = 0)""",
        (BUILTIN_PROMPTS["leader"],),
    )
    # Migrate outdated built-in developer prompt that lacked branch/handoff constraints.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='developer' AND is_builtin=1
             AND INSTR(prompt, '所有成果必须写入文件') > 0
             AND INSTR(prompt, '分支与交接约束') = 0""",
        (BUILTIN_PROMPTS["developer"],),
    )
    # Migrate outdated built-in manager prompt that did not require exact commit_hash merge.
    conn.execute(
        """UPDATE agent_types
           SET prompt=?
           WHERE key='manager' AND is_builtin=1
             AND INSTR(prompt, '合并到主分支') > 0
             AND INSTR(prompt, 'commit_hash') = 0""",
        (BUILTIN_PROMPTS["manager"],),
    )
    # Migrate outdated built-in descriptions.
    conn.execute(
        """UPDATE agent_types
           SET description=?
           WHERE key='developer' AND is_builtin=1
             AND (INSTR(description, 'dev 分支') > 0 OR INSTR(description, '提交到 dev') > 0)""",
        ("实现任务需求，在 agent/<agent> 工作分支提交并交接审查",),
    )
    conn.execute(
        """UPDATE agent_types
           SET description=?
           WHERE key='manager' AND is_builtin=1
             AND INSTR(description, '合并到主分支') > 0""",
        ("将审查通过的目标 commit 精确合并到 main",),
    )


def _recover_reviewer_stuck_tasks(conn):
    """Repair historical reviewer-system-error tasks stuck in needs_changes."""
    conn.execute(
        """
        UPDATE tasks
           SET status='blocked',
               assigned_agent='reviewer',
               assignee=NULL,
               updated_at=?
         WHERE status='needs_changes'
           AND assigned_agent='reviewer'
           AND (
                review_feedback LIKE '[系统错误]%'
                OR review_feedback LIKE '[系统错误][review_retry=%'
           )
        """,
        (_now(),),
    )


def _parse_poll_statuses(raw) -> list[str]:
    try:
        data = json.loads(raw or "[]")
    except Exception:
        return []
    if not isinstance(data, list):
        return []
    return [str(x) for x in data]


def _working_statuses(conn) -> set[str]:
    rows = conn.execute(
        "SELECT DISTINCT working_status FROM agent_types WHERE TRIM(COALESCE(working_status, '')) != ''"
    ).fetchall()
    out: set[str] = set()
    for row in rows:
        ws = str(row["working_status"] or "").strip()
        if ws:
            out.add(ws)
    return out


def _lease_deadline_iso(ttl_seconds: int) -> str:
    ttl = max(30, int(ttl_seconds))
    return (datetime.utcnow() + timedelta(seconds=ttl)).isoformat()


def _assert_task_fence_in_conn(
    conn,
    task_id: str,
    expected_run_id: str | None = None,
    expected_lease_token: str | None = None,
    strict_if_active: bool = False,
) -> dict | None:
    row = conn.execute(
        "SELECT id, assignee, claim_run_id, lease_token FROM tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    if not row:
        return None

    task = dict(row)
    assignee = str(task.get("assignee") or "").strip()
    active_token = str(task.get("lease_token") or "").strip()
    if not assignee or not active_token:
        return task

    run_id = str(expected_run_id or "").strip()
    token = str(expected_lease_token or "").strip()
    if not run_id or not token:
        if strict_if_active:
            raise LeaseConflictError("任务存在活动租约，缺少 run_id/lease_token")
        return task

    current_run_id = str(task.get("claim_run_id") or "").strip()
    if run_id != current_run_id or token != active_token:
        raise LeaseConflictError("租约已失效或被其他运行接管")
    return task


def validate_task_lease(
    task_id: str,
    expected_run_id: str | None = None,
    expected_lease_token: str | None = None,
    strict_if_active: bool = False,
) -> tuple[bool, str]:
    conn = get_conn()
    try:
        row = _assert_task_fence_in_conn(
            conn,
            task_id=task_id,
            expected_run_id=expected_run_id,
            expected_lease_token=expected_lease_token,
            strict_if_active=strict_if_active,
        )
        if not row:
            return False, "task_not_found"
        return True, "ok"
    except LeaseConflictError as e:
        return False, str(e)
    finally:
        conn.close()


def _todo_pollers(conn) -> set[str]:
    rows = conn.execute("SELECT key, poll_statuses FROM agent_types").fetchall()
    out: set[str] = set()
    for row in rows:
        key = str(row["key"] or "").strip()
        if not key:
            continue
        if "todo" in _parse_poll_statuses(row["poll_statuses"]):
            out.add(key)
    return out


def _recover_invalid_todo_assignments(conn):
    """
    Fix historical rows where todo tasks were assigned to agents
    that do not poll todo (e.g. leader/reviewer/manager), which blocks claiming.
    """
    todo_pollers = _todo_pollers(conn)
    if not todo_pollers:
        return
    rows = conn.execute(
        """
        SELECT id, assigned_agent, dev_agent
          FROM tasks
         WHERE status='todo'
           AND archived=0
           AND assigned_agent IS NOT NULL
           AND TRIM(assigned_agent) != ''
        """
    ).fetchall()
    if not rows:
        return
    now = _now()
    for row in rows:
        assigned = str(row["assigned_agent"] or "").strip()
        if assigned in todo_pollers:
            continue
        dev_agent = str(row["dev_agent"] or "").strip()
        fallback = dev_agent if dev_agent in todo_pollers else None
        conn.execute(
            "UPDATE tasks SET assigned_agent=?, updated_at=? WHERE id=?",
            (fallback, now, row["id"]),
        )


def _backfill_subtask_order(conn):
    """
    Ensure all subtasks under the same parent have deterministic 1..N order.
    Existing explicit order is preferred; missing/legacy order falls back to created_at.
    """
    parents = conn.execute(
        """
        SELECT DISTINCT parent_task_id
          FROM tasks
         WHERE parent_task_id IS NOT NULL
           AND TRIM(parent_task_id) != ''
        """
    ).fetchall()
    for p in parents:
        parent_id = str(p["parent_task_id"] or "").strip()
        if not parent_id:
            continue
        rows = conn.execute(
            """
            SELECT id, subtask_order, created_at
              FROM tasks
             WHERE parent_task_id=?
             ORDER BY
               CASE WHEN COALESCE(subtask_order, 0) > 0 THEN 0 ELSE 1 END ASC,
               COALESCE(subtask_order, 0) ASC,
               created_at ASC,
               id ASC
            """,
            (parent_id,),
        ).fetchall()
        for idx, row in enumerate(rows, 1):
            current = int(row["subtask_order"] or 0)
            if current == idx:
                continue
            conn.execute("UPDATE tasks SET subtask_order=? WHERE id=?", (idx, row["id"]))


def _now():
    return datetime.utcnow().isoformat()


def _normalize_username(username: str) -> str:
    return str(username or "").strip().lower()


def _password_hash(password: str, iterations: int = 260000) -> str:
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return f"pbkdf2_sha256${iterations}${salt.hex()}${digest.hex()}"


def _verify_password(password: str, encoded: str | None) -> bool:
    payload = str(encoded or "").strip()
    if not payload:
        return False
    try:
        algo, iter_s, salt_hex, digest_hex = payload.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        iterations = int(iter_s)
        salt = bytes.fromhex(salt_hex)
        expected = bytes.fromhex(digest_hex)
    except Exception:
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def _hash_session_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _public_user(row) -> dict:
    data = dict(row)
    return {
        "id": data["id"],
        "username": data["username"],
        "role": data["role"],
        "created_by": data.get("created_by"),
        "created_at": data.get("created_at"),
        "password_set": bool(str(data.get("password_hash") or "").strip()),
    }


def admin_password_is_set() -> bool:
    conn = get_conn()
    row = conn.execute(
        "SELECT password_hash FROM users WHERE username='admin' LIMIT 1"
    ).fetchone()
    conn.close()
    return bool(row and str(row["password_hash"] or "").strip())


def set_admin_initial_password(password: str) -> dict | None:
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT * FROM users WHERE username='admin' LIMIT 1"
        ).fetchone()
        if not row:
            return None
        if str(row["password_hash"] or "").strip():
            return None
        hashed = _password_hash(password)
        conn.execute(
            "UPDATE users SET password_hash=? WHERE id=?",
            (hashed, row["id"]),
        )
        conn.commit()
        updated = conn.execute("SELECT * FROM users WHERE id=?", (row["id"],)).fetchone()
        return _public_user(updated) if updated else None
    finally:
        conn.close()


def get_user(user_id: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    return _public_user(row) if row else None


def get_user_by_username(username: str) -> dict | None:
    uname = _normalize_username(username)
    if not uname:
        return None
    conn = get_conn()
    row = conn.execute("SELECT * FROM users WHERE username=?", (uname,)).fetchone()
    conn.close()
    return _public_user(row) if row else None


def authenticate_user(username: str, password: str) -> dict | None:
    uname = _normalize_username(username)
    if not uname:
        return None
    conn = get_conn()
    try:
        row = conn.execute("SELECT * FROM users WHERE username=?", (uname,)).fetchone()
        if not row:
            return None
        if not _verify_password(password, row["password_hash"]):
            return None
        return _public_user(row)
    finally:
        conn.close()


def create_user(username: str, password: str, role: str = ROLE_USER, created_by: str | None = None) -> dict:
    uname = _normalize_username(username)
    if not uname:
        raise ValueError("username 不能为空")
    if role not in {ROLE_ADMIN, ROLE_USER}:
        raise ValueError("role 不合法")
    now = _now()
    uid = str(uuid.uuid4())
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO users (id, username, password_hash, role, created_by, created_at)
            VALUES (?,?,?,?,?,?)
            """,
            (uid, uname, _password_hash(password), role, created_by, now),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
        if not row:
            raise RuntimeError("create_user failed")
        return _public_user(row)
    finally:
        conn.close()


def list_users() -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM users ORDER BY CASE role WHEN 'admin' THEN 0 ELSE 1 END, created_at ASC"
    ).fetchall()
    conn.close()
    return [_public_user(r) for r in rows]


def create_session(user_id: str, ttl_days: int = SESSION_TTL_DAYS) -> dict:
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    expires = now + timedelta(days=max(1, int(ttl_days)))
    row = {
        "id": str(uuid.uuid4()),
        "token": token,
        "token_hash": _hash_session_token(token),
        "user_id": user_id,
        "created_at": now.isoformat(),
        "expires_at": expires.isoformat(),
    }
    conn = get_conn()
    try:
        conn.execute(
            """
            INSERT INTO sessions (id, user_id, token_hash, created_at, expires_at)
            VALUES (?,?,?,?,?)
            """,
            (row["id"], row["user_id"], row["token_hash"], row["created_at"], row["expires_at"]),
        )
        conn.commit()
        return {"token": token, "expires_at": row["expires_at"]}
    finally:
        conn.close()


def revoke_session(token: str) -> bool:
    hashed = _hash_session_token(token)
    conn = get_conn()
    cur = conn.execute("DELETE FROM sessions WHERE token_hash=?", (hashed,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def get_session_user(token: str) -> dict | None:
    hashed = _hash_session_token(token)
    conn = get_conn()
    try:
        conn.execute("DELETE FROM sessions WHERE expires_at <= ?", (_now(),))
        row = conn.execute(
            """
            SELECT u.*
              FROM sessions s
              JOIN users u ON u.id = s.user_id
             WHERE s.token_hash=?
               AND s.expires_at > ?
             LIMIT 1
            """,
            (hashed, _now()),
        ).fetchone()
        conn.commit()
        return _public_user(row) if row else None
    finally:
        conn.close()


def _parse_feedback_history(raw) -> list[dict]:
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

    out: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        feedback = str(item.get("feedback") or "").strip()
        if not feedback:
            continue
        resolved_at = str(item.get("resolved_at") or "").strip()
        resolved = bool(item.get("resolved")) or bool(resolved_at)
        out.append(
            {
                "id": str(item.get("id") or "").strip(),
                "created_at": str(item.get("created_at") or "").strip(),
                "source": str(item.get("source") or "system").strip() or "system",
                "status_at": str(item.get("status_at") or "").strip(),
                "stage": str(item.get("stage") or "").strip(),
                "actor": str(item.get("actor") or "").strip(),
                "feedback": feedback[:4000],
                "resolved": resolved,
                "resolved_at": resolved_at,
                "resolved_reason": str(item.get("resolved_reason") or "").strip(),
            }
        )
    return out


def _dump_feedback_history(history: list[dict]) -> str:
    return json.dumps(history, ensure_ascii=False)


def _next_feedback_id(history: list[dict]) -> str:
    max_n = 0
    for item in history:
        fid = str(item.get("id") or "").strip().upper()
        m = re.match(r"^FB(\d+)$", fid)
        if not m:
            continue
        try:
            max_n = max(max_n, int(m.group(1)))
        except Exception:
            continue
    return f"FB{max_n + 1:04d}"


def _resolve_open_feedback(history: list[dict], resolved_at: str, reason: str) -> bool:
    changed = False
    for item in history:
        if bool(item.get("resolved")):
            continue
        item["resolved"] = True
        item["resolved_at"] = resolved_at
        item["resolved_reason"] = reason[:80]
        changed = True
    return changed


def _append_feedback_entry(
    history: list[dict],
    feedback: str,
    source: str,
    status_at: str,
    stage: str,
    actor: str,
    created_at: str,
) -> bool:
    text = str(feedback or "").strip()
    if not text:
        return False

    _resolve_open_feedback(history, created_at, "superseded")
    history.append(
        {
            "id": _next_feedback_id(history),
            "created_at": created_at,
            "source": source[:40] or "system",
            "status_at": status_at[:40],
            "stage": stage[:80],
            "actor": actor[:80],
            "feedback": text[:4000],
            "resolved": False,
            "resolved_at": "",
            "resolved_reason": "",
        }
    )
    # Keep payload bounded to avoid unbounded task row growth.
    if len(history) > 120:
        del history[: len(history) - 120]
    return True


def reset_stuck_tasks():
    """
    On server startup, reset tasks left in transient agent states
    back to the last stable state (in case of a crash/restart).
    Uses the agent_types table so custom agents are also handled.
    """
    conn = get_conn()
    rows = conn.execute(
        "SELECT poll_statuses, working_status FROM agent_types WHERE working_status != ''"
    ).fetchall()
    for row in rows:
        working = row["working_status"]
        poll = json.loads(row["poll_statuses"] or "[]")
        reset_to = poll[0] if poll else "todo"
        conn.execute(
            """
            UPDATE tasks
               SET status=?,
                   assignee=NULL,
                   claim_run_id=NULL,
                   lease_token=NULL,
                   lease_expires_at=NULL
             WHERE status=? AND archived=0
            """,
            (reset_to, working),
        )
    conn.commit()
    conn.close()


# ── Projects ──────────────────────────────────────────────────────────────────

def create_project(name: str, path: str, created_by_user_id: str | None = None) -> dict:
    conn = get_conn()
    pid = str(uuid.uuid4())
    now = _now()
    conn.execute(
        "INSERT INTO projects (id, name, path, created_by_user_id, created_at) VALUES (?,?,?,?,?)",
        (pid, name, path, created_by_user_id, now),
    )
    conn.commit()
    row = conn.execute(
        """
        SELECT p.*, u.username AS created_by_username
          FROM projects p
          LEFT JOIN users u ON u.id = p.created_by_user_id
         WHERE p.id=?
        """,
        (pid,),
    ).fetchone()
    conn.close()
    return dict(row)


def get_project(project_id: str, user_id: str | None = None, is_admin: bool = True) -> dict | None:
    conn = get_conn()
    if user_id and not is_admin:
        row = conn.execute(
            """
            SELECT p.*, u.username AS created_by_username
              FROM projects p
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE p.id=?
               AND p.created_by_user_id=?
            """,
            (project_id, user_id),
        ).fetchone()
    else:
        row = conn.execute(
            """
            SELECT p.*, u.username AS created_by_username
              FROM projects p
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE p.id=?
            """,
            (project_id,),
        ).fetchone()
    conn.close()
    return dict(row) if row else None


def list_projects(user_id: str | None = None, is_admin: bool = True) -> list[dict]:
    conn = get_conn()
    if user_id and not is_admin:
        rows = conn.execute(
            """
            SELECT p.*, u.username AS created_by_username
              FROM projects p
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE p.created_by_user_id=?
             ORDER BY p.created_at DESC
            """,
            (user_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT p.*, u.username AS created_by_username
              FROM projects p
              LEFT JOIN users u ON u.id = p.created_by_user_id
             ORDER BY p.created_at DESC
            """
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def user_can_access_project(project_id: str, user_id: str | None, is_admin: bool) -> bool:
    if is_admin:
        return bool(get_project(project_id))
    if not user_id:
        return False
    conn = get_conn()
    row = conn.execute(
        "SELECT id FROM projects WHERE id=? AND created_by_user_id=? LIMIT 1",
        (project_id, user_id),
    ).fetchone()
    conn.close()
    return bool(row)


def delete_project(project_id: str) -> bool:
    """Delete a project and all its tasks/logs. Returns True if deleted."""
    conn = get_conn()
    # Delete handoffs for all tasks in this project
    conn.execute(
        "DELETE FROM task_handoffs WHERE task_id IN (SELECT id FROM tasks WHERE project_id=?)",
        (project_id,),
    )
    # Delete logs for all tasks in this project
    conn.execute(
        "DELETE FROM logs WHERE task_id IN (SELECT id FROM tasks WHERE project_id=?)",
        (project_id,),
    )
    # Delete all tasks in this project
    conn.execute("DELETE FROM tasks WHERE project_id=?", (project_id,))
    # Delete the project itself
    cur = conn.execute("DELETE FROM projects WHERE id=?", (project_id,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def project_has_claimed_tasks(project_id: str) -> bool:
    """
    Return True if project has non-archived tasks currently claimed by an agent.
    This is used to avoid deleting projects while agents are still processing tasks.
    """
    conn = get_conn()
    row = conn.execute(
        """
        SELECT 1
          FROM tasks
         WHERE project_id=?
           AND archived=0
           AND assignee IS NOT NULL
         LIMIT 1
        """,
        (project_id,),
    ).fetchone()
    conn.close()
    return bool(row)


# ── Tasks ─────────────────────────────────────────────────────────────────────

def _join_project(base_sql: str) -> str:
    """Wrap a task query to also return project.path as project_path."""
    return f"""
        SELECT
            t.*,
            p.path as project_path,
            p.name as project_name,
            p.created_by_user_id as project_owner_user_id,
            u.username as project_owner_username
        FROM ({base_sql}) t
        LEFT JOIN projects p ON t.project_id = p.id
        LEFT JOIN users u ON u.id = p.created_by_user_id
    """


def create_task(title: str, description: str, project_id: str | None = None,
                parent_task_id: str | None = None,
                assigned_agent: str | None = None,
                dev_agent: str | None = None,
                status: str = "triage",
                subtask_order: int | None = None) -> dict:
    conn = get_conn()
    tid = str(uuid.uuid4())
    now = _now()
    normalized_subtask_order = int(subtask_order or 0)
    if normalized_subtask_order < 0:
        normalized_subtask_order = 0
    conn.execute(
        """INSERT INTO tasks
           (id, project_id, title, description, status,
            parent_task_id, subtask_order, assigned_agent, dev_agent, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
        (tid, project_id, title, description, status,
         parent_task_id, normalized_subtask_order, assigned_agent, dev_agent, now, now),
    )
    conn.commit()
    row = conn.execute(
        _join_project("SELECT * FROM tasks WHERE id=?"), (tid,)
    ).fetchone()
    conn.close()
    return dict(row)


def list_subtasks(parent_task_id: str, user_id: str | None = None, is_admin: bool = True) -> list[dict]:
    conn = get_conn()
    if user_id and not is_admin:
        rows = conn.execute(
            """
            SELECT
                t.*,
                p.path as project_path,
                p.name as project_name,
                p.created_by_user_id as project_owner_user_id,
                u.username as project_owner_username
              FROM tasks t
              LEFT JOIN projects p ON t.project_id = p.id
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE t.parent_task_id=?
               AND p.created_by_user_id=?
             ORDER BY
               CASE WHEN COALESCE(t.subtask_order, 0) > 0 THEN 0 ELSE 1 END ASC,
               COALESCE(t.subtask_order, 0) ASC,
               t.created_at ASC,
               t.id ASC
            """,
            (parent_task_id, user_id),
        ).fetchall()
    else:
        rows = conn.execute(
            """
            SELECT
                t.*,
                p.path as project_path,
                p.name as project_name,
                p.created_by_user_id as project_owner_user_id,
                u.username as project_owner_username
              FROM tasks t
              LEFT JOIN projects p ON t.project_id = p.id
              LEFT JOIN users u ON u.id = p.created_by_user_id
             WHERE t.parent_task_id=?
             ORDER BY
               CASE WHEN COALESCE(t.subtask_order, 0) > 0 THEN 0 ELSE 1 END ASC,
               COALESCE(t.subtask_order, 0) ASC,
               t.created_at ASC,
               t.id ASC
            """,
            (parent_task_id,),
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def check_parent_completion(parent_task_id: str) -> bool:
    """
    If all subtasks of parent_task_id are 'completed', auto-complete the parent.
    Returns True if parent was just completed.
    """
    conn = get_conn()
    subtasks = conn.execute(
        "SELECT status FROM tasks WHERE parent_task_id=?", (parent_task_id,)
    ).fetchall()
    if not subtasks:
        conn.close()
        return False
    all_done = all(s["status"] == "completed" for s in subtasks)
    if all_done:
        conn.execute(
            "UPDATE tasks SET status='completed', updated_at=? WHERE id=? AND status='decomposed'",
            (_now(), parent_task_id),
        )
        conn.commit()
    conn.close()
    return all_done


def get_task(task_id: str, user_id: str | None = None, is_admin: bool = True) -> dict | None:
    conn = get_conn()
    if user_id and not is_admin:
        row = conn.execute(
            _join_project(
                """
                SELECT t.*
                  FROM tasks t
                  JOIN projects p ON p.id = t.project_id
                 WHERE t.id=?
                   AND p.created_by_user_id=?
                """
            ),
            (task_id, user_id),
        ).fetchone()
    else:
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
    conn.close()
    return dict(row) if row else None


def list_tasks(project_id: str | None = None, user_id: str | None = None, is_admin: bool = True) -> list[dict]:
    conn = get_conn()
    if user_id and not is_admin:
        if project_id:
            rows = conn.execute(
                _join_project(
                    """
                    SELECT t.*
                      FROM tasks t
                      JOIN projects p ON p.id = t.project_id
                     WHERE t.project_id=?
                       AND p.created_by_user_id=?
                     ORDER BY t.created_at DESC
                    """
                ),
                (project_id, user_id),
            ).fetchall()
        else:
            rows = conn.execute(
                _join_project(
                    """
                    SELECT t.*
                      FROM tasks t
                      JOIN projects p ON p.id = t.project_id
                     WHERE p.created_by_user_id=?
                     ORDER BY t.created_at DESC
                    """
                ),
                (user_id,),
            ).fetchall()
    else:
        if project_id:
            rows = conn.execute(
                _join_project("SELECT * FROM tasks WHERE project_id=? ORDER BY created_at DESC"),
                (project_id,),
            ).fetchall()
        else:
            rows = conn.execute(
                _join_project("SELECT * FROM tasks ORDER BY created_at DESC")
            ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def _update_task_in_conn(conn, task_id: str, **fields) -> dict | None:
    fields = dict(fields or {})
    # Control/meta fields used by API/agents but not persisted directly.
    feedback_source = str(fields.pop("feedback_source", "") or "").strip() or "system"
    feedback_stage = str(fields.pop("feedback_stage", "") or "").strip()
    feedback_actor = str(fields.pop("feedback_actor", "") or "").strip()
    fields.pop("create_handoff", None)

    current = conn.execute(
        """
        SELECT
            status,
            assignee,
            assigned_agent,
            dev_agent,
            claim_run_id,
            lease_token,
            lease_expires_at,
            review_feedback,
            review_feedback_history
        FROM tasks
        WHERE id=?
        """,
        (task_id,),
    ).fetchone()
    if not current:
        return None
    # Canceled tasks are immutable by normal updates so late agent writes
    # cannot resurrect them.
    if current["status"] == CANCELLED_STATUS:
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
        return dict(row) if row else None
    if not fields:
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
        return dict(row) if row else None

    target_status = str(fields.get("status") or current["status"] or "").strip()
    now = _now()

    # ── Feedback history maintenance ────────────────────────────────────────
    history = _parse_feedback_history(current["review_feedback_history"])
    history_changed = False

    if "review_feedback" in fields:
        new_feedback = str(fields.get("review_feedback") or "").strip()
        old_feedback = str(current["review_feedback"] or "").strip()
        if (
            new_feedback
            and new_feedback != old_feedback
            and target_status in ACTIONABLE_FEEDBACK_STATUSES
        ):
            history_changed = _append_feedback_entry(
                history,
                feedback=new_feedback,
                source=feedback_source,
                status_at=target_status,
                stage=feedback_stage,
                actor=feedback_actor,
                created_at=now,
            ) or history_changed

    if target_status in FEEDBACK_RESOLVE_STATUSES:
        history_changed = _resolve_open_feedback(
            history,
            resolved_at=now,
            reason=f"status:{target_status}",
        ) or history_changed

    if history_changed:
        fields["review_feedback_history"] = _dump_feedback_history(history)

    if target_status == "todo":
        todo_pollers = _todo_pollers(conn)
        # Validate current/effective assignment for todo claimability.
        if "assigned_agent" in fields:
            effective_assigned = str(fields.get("assigned_agent") or "").strip()
        else:
            effective_assigned = str(current["assigned_agent"] or "").strip()
        if effective_assigned and effective_assigned not in todo_pollers:
            fallback_dev = str(fields.get("dev_agent") or current["dev_agent"] or "").strip()
            fields["assigned_agent"] = fallback_dev if fallback_dev in todo_pollers else None

    working_statuses = _working_statuses(conn)
    if "status" in fields and target_status not in working_statuses:
        fields["claim_run_id"] = None
        fields["lease_token"] = None
        fields["lease_expires_at"] = None
    if "assignee" in fields:
        next_assignee = str(fields.get("assignee") or "").strip()
        current_assignee = str(current["assignee"] or "").strip()
        if not next_assignee or next_assignee != current_assignee:
            fields["claim_run_id"] = None
            fields["lease_token"] = None
            fields["lease_expires_at"] = None

    fields["updated_at"] = now
    set_clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [task_id]
    conn.execute(f"UPDATE tasks SET {set_clause} WHERE id=?", values)
    row = conn.execute(
        _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
    ).fetchone()
    return dict(row) if row else None


def update_task(task_id: str, **fields) -> dict | None:
    conn = get_conn()
    try:
        row = _update_task_in_conn(conn, task_id, **fields)
        conn.commit()
        return row
    finally:
        conn.close()


def get_tasks_by_status(
    status: str,
    project_id: str | None = None,
    user_id: str | None = None,
    is_admin: bool = True,
) -> list[dict]:
    conn = get_conn()
    if user_id and not is_admin:
        if project_id:
            rows = conn.execute(
                _join_project(
                    """
                    SELECT t.*
                      FROM tasks t
                      JOIN projects p ON p.id = t.project_id
                     WHERE t.status=?
                       AND t.project_id=?
                       AND p.created_by_user_id=?
                     ORDER BY t.updated_at ASC
                    """
                ),
                (status, project_id, user_id),
            ).fetchall()
        else:
            rows = conn.execute(
                _join_project(
                    """
                    SELECT t.*
                      FROM tasks t
                      JOIN projects p ON p.id = t.project_id
                     WHERE t.status=?
                       AND p.created_by_user_id=?
                     ORDER BY t.updated_at ASC
                    """
                ),
                (status, user_id),
            ).fetchall()
    else:
        if project_id:
            rows = conn.execute(
                _join_project("SELECT * FROM tasks WHERE status=? AND project_id=? ORDER BY updated_at ASC"),
                (status, project_id),
            ).fetchall()
        else:
            rows = conn.execute(
                _join_project("SELECT * FROM tasks WHERE status=? ORDER BY updated_at ASC"),
                (status,),
            ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def cancel_task(task_id: str, include_subtasks: bool = True) -> list[dict] | None:
    """
    Cancel a task (and optionally all descendants), archive it, and make it non-runnable.
    Returns updated rows with joined project fields in deterministic order, or None if task missing.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        root = conn.execute("SELECT id FROM tasks WHERE id=?", (task_id,)).fetchone()
        if not root:
            conn.rollback()
            return None

        if include_subtasks:
            rows = conn.execute(
                """
                WITH RECURSIVE task_tree(id) AS (
                    SELECT id FROM tasks WHERE id=?
                    UNION ALL
                    SELECT t.id
                    FROM tasks t
                    JOIN task_tree tt ON t.parent_task_id = tt.id
                )
                SELECT id FROM task_tree
                """,
                (task_id,),
            ).fetchall()
            task_ids = [r["id"] for r in rows]
        else:
            task_ids = [task_id]

        now = _now()
        placeholders = ",".join("?" for _ in task_ids)
        conn.execute(
            f"""
            UPDATE tasks
               SET status=?,
                   archived=1,
                   assignee=NULL,
                   claim_run_id=NULL,
                   lease_token=NULL,
                   lease_expires_at=NULL,
                   updated_at=?
             WHERE id IN ({placeholders})
            """,
            [CANCELLED_STATUS, now, *task_ids],
        )

        conn.commit()

        # Root first, then descendants (recursive query order).
        ordered = []
        root_row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
        if root_row:
            ordered.append(dict(root_row))
        for tid in task_ids:
            if tid == task_id:
                continue
            row = conn.execute(
                _join_project("SELECT * FROM tasks WHERE id=?"), (tid,)
            ).fetchone()
            if row:
                ordered.append(dict(row))
        return ordered
    finally:
        conn.close()


def claim_task(status: str, working_status: str, agent: str, agent_key: str,
               respect_assignment: bool = True,
               lease_ttl_secs: int = 180,
               project_id: str | None = None) -> dict | None:
    """
    Atomically claim the next task in `status` and move it to `working_status`.
    Returns the claimed task row (with joined project fields) or None.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        where = ["t.status=?", "t.archived=0"]
        params: list[str] = [status]

        if project_id:
            where.append("t.project_id=?")
            params.append(project_id)

        if respect_assignment:
            where.append("(t.assigned_agent IS NULL OR t.assigned_agent=?)")
            params.append(agent_key)

        if status == "todo":
            where.append(
                f"""
                (
                    t.parent_task_id IS NULL
                    OR NOT EXISTS (
                        SELECT 1
                          FROM tasks prev
                         WHERE prev.parent_task_id = t.parent_task_id
                           AND prev.id != t.id
                           AND COALESCE(prev.archived, 0) = 0
                           AND prev.status NOT IN ('completed', '{CANCELLED_STATUS}')
                           AND (
                                 (
                                   COALESCE(t.subtask_order, 0) > 0
                                   AND (
                                       (COALESCE(prev.subtask_order, 0) > 0 AND prev.subtask_order < t.subtask_order)
                                       OR COALESCE(prev.subtask_order, 0) <= 0
                                   )
                                 )
                                 OR
                                 (
                                   COALESCE(t.subtask_order, 0) <= 0
                                   AND (
                                       prev.created_at < t.created_at
                                       OR (prev.created_at = t.created_at AND prev.id < t.id)
                                   )
                                 )
                           )
                    )
                )
                """
            )

        row = conn.execute(
            f"SELECT t.id FROM tasks t WHERE {' AND '.join(where)} ORDER BY t.updated_at ASC LIMIT 1",
            tuple(params),
        ).fetchone()
        if not row:
            conn.rollback()
            return None

        now = _now()
        run_id = str(uuid.uuid4())
        lease_token = str(uuid.uuid4())
        lease_expires_at = _lease_deadline_iso(lease_ttl_secs)
        cur = conn.execute(
            """
            UPDATE tasks
               SET status=?,
                   assignee=?,
                   claim_run_id=?,
                   lease_token=?,
                   lease_expires_at=?,
                   updated_at=?
             WHERE id=? AND status=? AND archived=0
            """,
            (
                working_status,
                agent,
                run_id,
                lease_token,
                lease_expires_at,
                now,
                row["id"],
                status,
            ),
        )
        if cur.rowcount != 1:
            conn.rollback()
            return None

        conn.commit()
        claimed = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (row["id"],)
        ).fetchone()
        return dict(claimed) if claimed else None
    finally:
        conn.close()


def recover_stale_tasks_for_agent(agent_key: str) -> list[dict]:
    """
    Recover tasks left in an agent's working state when the agent appears stale.
    Returns changed rows as:
      {"task": <task_row>, "from_status": "...", "to_status": "..."}.
    """
    key = str(agent_key or "").strip().lower()
    if not key:
        return []
    conn = get_conn()
    changed: list[dict] = []
    try:
        rows = conn.execute(
            "SELECT poll_statuses, working_status FROM agent_types WHERE key=? AND working_status != ''",
            (key,),
        ).fetchall()
        if not rows:
            return []
        now = _now()
        for row in rows:
            working = str(row["working_status"] or "").strip()
            if not working:
                continue
            poll = _parse_poll_statuses(row["poll_statuses"])
            reset_to = poll[0] if poll else "todo"
            task_rows = conn.execute(
                "SELECT id FROM tasks WHERE status=? AND assignee=? AND archived=0",
                (working, key),
            ).fetchall()
            for t in task_rows:
                conn.execute(
                    """
                    UPDATE tasks
                       SET status=?,
                           assignee=NULL,
                           claim_run_id=NULL,
                           lease_token=NULL,
                           lease_expires_at=NULL,
                           updated_at=?
                     WHERE id=?
                    """,
                    (reset_to, now, t["id"]),
                )
                updated = conn.execute(
                    _join_project("SELECT * FROM tasks WHERE id=?"), (t["id"],)
                ).fetchone()
                if updated:
                    changed.append(
                        {
                            "task": dict(updated),
                            "from_status": working,
                            "to_status": reset_to,
                        }
                    )
        if changed:
            conn.commit()
        else:
            conn.rollback()
    finally:
        conn.close()
    return changed


def renew_task_lease(
    task_id: str,
    run_id: str,
    lease_token: str,
    lease_ttl_secs: int = 180,
) -> dict | None:
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        _assert_task_fence_in_conn(
            conn,
            task_id=task_id,
            expected_run_id=run_id,
            expected_lease_token=lease_token,
            strict_if_active=True,
        )
        now = _now()
        lease_expires_at = _lease_deadline_iso(lease_ttl_secs)
        cur = conn.execute(
            """
            UPDATE tasks
               SET lease_expires_at=?,
                   updated_at=?
             WHERE id=?
               AND claim_run_id=?
               AND lease_token=?
               AND assignee IS NOT NULL
               AND archived=0
            """,
            (lease_expires_at, now, task_id, run_id, lease_token),
        )
        if cur.rowcount != 1:
            conn.rollback()
            return None
        conn.commit()
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"),
            (task_id,),
        ).fetchone()
        return dict(row) if row else None
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def recover_expired_task_leases(
    grace_secs: int = 0,
    exclude_task_ids: set[str] | None = None,
) -> list[dict]:
    """
    Recover tasks whose lease has expired.
    Returns changed rows as:
      {"task": <task_row>, "from_status": "...", "to_status": "...",
       "agent_key": "...", "expired_secs": <int>}
    """
    conn = get_conn()
    changed: list[dict] = []
    excluded = {str(x or "").strip() for x in (exclude_task_ids or set()) if str(x or "").strip()}
    try:
        conn.execute("BEGIN IMMEDIATE")
        now_dt = datetime.utcnow()
        now = now_dt.isoformat()
        cutoff = (now_dt - timedelta(seconds=max(0, int(grace_secs)))).isoformat()
        rows = conn.execute(
            """
            SELECT id, status, assignee, lease_expires_at
              FROM tasks
             WHERE archived=0
               AND assignee IS NOT NULL
               AND TRIM(COALESCE(lease_token, '')) != ''
               AND TRIM(COALESCE(lease_expires_at, '')) != ''
               AND lease_expires_at <= ?
            """,
            (cutoff,),
        ).fetchall()
        for row in rows:
            task_id = str(row["id"] or "").strip()
            if not task_id:
                continue
            if task_id in excluded:
                continue
            from_status = str(row["status"] or "").strip()
            agent_key = str(row["assignee"] or "").strip().lower()
            cfg = conn.execute(
                "SELECT poll_statuses, working_status FROM agent_types WHERE key=?",
                (agent_key,),
            ).fetchone()
            if cfg:
                working = str(cfg["working_status"] or "").strip()
                poll = _parse_poll_statuses(cfg["poll_statuses"])
                reset_to = poll[0] if poll else "todo"
            else:
                working = ""
                reset_to = "todo"
            to_status = reset_to if (working and from_status == working) else from_status
            conn.execute(
                """
                UPDATE tasks
                   SET status=?,
                       assignee=NULL,
                       claim_run_id=NULL,
                       lease_token=NULL,
                       lease_expires_at=NULL,
                       updated_at=?
                 WHERE id=?
                   AND archived=0
                """,
                (to_status, now, task_id),
            )
            updated = conn.execute(
                _join_project("SELECT * FROM tasks WHERE id=?"),
                (task_id,),
            ).fetchone()
            if not updated:
                continue
            expired_secs = 0
            lease_expires_at = str(row["lease_expires_at"] or "").strip()
            if lease_expires_at:
                try:
                    exp_dt = datetime.fromisoformat(lease_expires_at.rstrip("Z"))
                    expired_secs = max(0, int((now_dt - exp_dt).total_seconds()))
                except Exception:
                    expired_secs = 0
            changed.append(
                {
                    "task": dict(updated),
                    "from_status": from_status,
                    "to_status": to_status,
                    "agent_key": agent_key,
                    "expired_secs": expired_secs,
                }
            )
        if changed:
            conn.commit()
        else:
            conn.rollback()
    finally:
        conn.close()
    return changed


def add_agent_output(
    agent: str,
    line: str,
    task_id: str | None = None,
    run_id: str | None = None,
    kind: str | None = None,
    event: str | None = None,
    exit_code: int | None = None,
    keep_last: int = 1000,
) -> dict:
    conn = get_conn()
    now = _now()
    output_kind = str(kind or "line").strip().lower() or "line"
    output_event = str(event or "line").strip().lower() or "line"
    output_exit_code = None if exit_code is None else int(exit_code)
    cur = conn.execute(
        """
        INSERT INTO agent_outputs (agent, task_id, run_id, line, kind, event, exit_code, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(agent or "").strip().lower(),
            str(task_id or "").strip() or None,
            str(run_id or "").strip() or None,
            str(line or ""),
            output_kind,
            output_event,
            output_exit_code,
            now,
        ),
    )
    row_id = int(cur.lastrowid or 0)
    keep = max(1, int(keep_last or 1000))
    conn.execute(
        """
        DELETE FROM agent_outputs
         WHERE agent=?
           AND id NOT IN (
               SELECT id
                 FROM agent_outputs
                WHERE agent=?
                ORDER BY id DESC
                LIMIT ?
           )
        """,
        (str(agent or "").strip().lower(), str(agent or "").strip().lower(), keep),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM agent_outputs WHERE id=?", (row_id,)).fetchone()
    conn.close()
    return dict(row) if row else {
        "id": row_id,
        "agent": str(agent or "").strip().lower(),
        "task_id": str(task_id or "").strip() or None,
        "run_id": str(run_id or "").strip() or None,
        "line": str(line or ""),
        "kind": output_kind,
        "event": output_event,
        "exit_code": output_exit_code,
        "created_at": now,
    }


def list_agent_output_agents() -> list[str]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT DISTINCT agent FROM agent_outputs WHERE TRIM(COALESCE(agent, '')) != ''"
    ).fetchall()
    conn.close()
    return [str(r["agent"] or "").strip().lower() for r in rows if str(r["agent"] or "").strip()]


def get_agent_output_lines(agent: str, limit: int = 1000) -> list[str]:
    return [str(e.get("line") or "") for e in get_agent_output_entries(agent, limit=limit)]


def get_agent_output_entries(agent: str, limit: int = 1000) -> list[dict]:
    key = str(agent or "").strip().lower()
    if not key:
        return []
    size = max(1, int(limit or 1000))
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT line, task_id, run_id, kind, event, exit_code, created_at
          FROM agent_outputs
         WHERE agent=?
         ORDER BY id DESC
         LIMIT ?
        """,
        (key, size),
    ).fetchall()
    conn.close()
    # DB query is DESC for performance; reverse to natural chronology.
    entries: list[dict] = []
    for r in reversed(rows):
        entries.append(
            {
                "line": str(r["line"] or ""),
                "task_id": str(r["task_id"] or "").strip() or None,
                "run_id": str(r["run_id"] or "").strip() or None,
                "kind": str(r["kind"] or "line").strip().lower() or "line",
                "event": str(r["event"] or "line").strip().lower() or "line",
                "exit_code": int(r["exit_code"]) if r["exit_code"] is not None else None,
                "created_at": str(r["created_at"] or ""),
            }
        )
    return entries


def _add_log_in_conn(conn, task_id: str, agent: str, message: str) -> dict:
    now = _now()
    cur = conn.execute(
        "INSERT INTO logs (task_id, agent, message, created_at) VALUES (?,?,?,?)",
        (task_id, agent, message, now),
    )
    log = {"id": cur.lastrowid, "task_id": task_id, "agent": agent,
           "message": message, "created_at": now}
    return log


def add_log(task_id: str, agent: str, message: str) -> dict:
    conn = get_conn()
    try:
        log = _add_log_in_conn(conn, task_id, agent, message)
        conn.commit()
        return log
    finally:
        conn.close()


def _add_handoff_in_conn(
    conn,
    task_id: str,
    stage: str,
    from_agent: str,
    to_agent: str | None = None,
    status_from: str | None = None,
    status_to: str | None = None,
    title: str = "",
    summary: str = "",
    commit_hash: str | None = None,
    conclusion: str | None = None,
    payload: dict | None = None,
    artifact_path: str | None = None,
) -> dict:
    now = _now()
    payload_text = json.dumps(payload or {}, ensure_ascii=False)
    cur = conn.execute(
        """
        INSERT INTO task_handoffs
        (task_id, stage, from_agent, to_agent, status_from, status_to, title, summary, commit_hash, conclusion, payload, artifact_path, created_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            task_id,
            stage,
            from_agent,
            to_agent,
            status_from,
            status_to,
            title,
            summary,
            commit_hash,
            conclusion,
            payload_text,
            artifact_path,
            now,
        ),
    )
    row = conn.execute(
        "SELECT * FROM task_handoffs WHERE id=?",
        (cur.lastrowid,),
    ).fetchone()
    return dict(row) if row else {
        "id": cur.lastrowid,
        "task_id": task_id,
        "stage": stage,
        "from_agent": from_agent,
        "to_agent": to_agent,
        "status_from": status_from,
        "status_to": status_to,
        "title": title,
        "summary": summary,
        "commit_hash": commit_hash,
        "conclusion": conclusion,
        "payload": payload_text,
        "artifact_path": artifact_path,
        "created_at": now,
    }


def add_handoff(
    task_id: str,
    stage: str,
    from_agent: str,
    to_agent: str | None = None,
    status_from: str | None = None,
    status_to: str | None = None,
    title: str = "",
    summary: str = "",
    commit_hash: str | None = None,
    conclusion: str | None = None,
    payload: dict | None = None,
    artifact_path: str | None = None,
) -> dict:
    conn = get_conn()
    try:
        row = _add_handoff_in_conn(
            conn,
            task_id=task_id,
            stage=stage,
            from_agent=from_agent,
            to_agent=to_agent,
            status_from=status_from,
            status_to=status_to,
            title=title,
            summary=summary,
            commit_hash=commit_hash,
            conclusion=conclusion,
            payload=payload,
            artifact_path=artifact_path,
        )
        conn.commit()
        return row
    finally:
        conn.close()


def transition_task(
    task_id: str,
    fields: dict | None = None,
    handoff: dict | None = None,
    log: dict | None = None,
    expected_run_id: str | None = None,
    expected_lease_token: str | None = None,
) -> dict | None:
    """
    Atomically apply task update + optional handoff + optional log in one tx.
    Returns {"task": ..., "handoff": ..., "log": ...} or None when task missing.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")
        guard_row = _assert_task_fence_in_conn(
            conn,
            task_id=task_id,
            expected_run_id=expected_run_id,
            expected_lease_token=expected_lease_token,
            strict_if_active=True,
        )
        if not guard_row:
            conn.rollback()
            return None
        update_fields = dict(fields or {})
        task = _update_task_in_conn(conn, task_id, **update_fields)
        if not task:
            conn.rollback()
            return None

        is_cancelled = (
            str(task.get("status") or "").strip().lower() == CANCELLED_STATUS
            or int(task.get("archived") or 0) == 1
        )
        created_handoff = None
        created_log = None
        if not is_cancelled and handoff:
            created_handoff = _add_handoff_in_conn(
                conn,
                task_id=task_id,
                stage=str(handoff.get("stage") or "").strip(),
                from_agent=str(handoff.get("from_agent") or "").strip(),
                to_agent=handoff.get("to_agent"),
                status_from=handoff.get("status_from"),
                status_to=handoff.get("status_to"),
                title=str(handoff.get("title") or ""),
                summary=str(handoff.get("summary") or ""),
                commit_hash=handoff.get("commit_hash"),
                conclusion=handoff.get("conclusion"),
                payload=handoff.get("payload") if isinstance(handoff.get("payload"), dict) else {},
                artifact_path=handoff.get("artifact_path"),
            )
        if not is_cancelled and log:
            created_log = _add_log_in_conn(
                conn,
                task_id=task_id,
                agent=str(log.get("agent") or "system").strip() or "system",
                message=str(log.get("message") or ""),
            )
        conn.commit()
        return {"task": task, "handoff": created_handoff, "log": created_log}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_logs(task_id: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM logs WHERE task_id=? ORDER BY created_at ASC", (task_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_handoffs(task_id: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM task_handoffs WHERE task_id=? ORDER BY created_at ASC, id ASC",
        (task_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── Agent Types ────────────────────────────────────────────────────────────────

def list_agent_types() -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM agent_types ORDER BY is_builtin DESC, created_at ASC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_agent_type(key: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM agent_types WHERE key=?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def create_agent_type(key: str, name: str, description: str, prompt: str,
                      poll_statuses: list, next_status: str,
                      working_status: str, cli: str) -> dict:
    conn = get_conn()
    aid = str(uuid.uuid4())
    now = _now()
    conn.execute(
        """INSERT INTO agent_types
           (id,key,name,description,prompt,poll_statuses,next_status,working_status,cli,is_builtin,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,0,?)""",
        (aid, key, name, description, prompt,
         json.dumps(poll_statuses), next_status, working_status, cli, now),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM agent_types WHERE id=?", (aid,)).fetchone()
    conn.close()
    return dict(row)


def update_agent_type(key: str, **fields) -> dict | None:
    if "poll_statuses" in fields and isinstance(fields["poll_statuses"], list):
        fields["poll_statuses"] = json.dumps(fields["poll_statuses"])
    conn = get_conn()
    set_clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [key]
    conn.execute(f"UPDATE agent_types SET {set_clause} WHERE key=?", values)
    conn.commit()
    row = conn.execute("SELECT * FROM agent_types WHERE key=?", (key,)).fetchone()
    conn.close()
    return dict(row) if row else None


def delete_agent_type(key: str) -> bool:
    conn = get_conn()
    cur = conn.execute("DELETE FROM agent_types WHERE key=? AND is_builtin=0", (key,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0
