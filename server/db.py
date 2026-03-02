import json
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "tasks.db"
CANCELLED_STATUS = "cancelled"

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
    "3. 直接开始实现，不需要解释计划"
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
    "你是发布合并管理者，负责把经审查通过的 commit 合并到主分支。\n\n"
    "任务标题：{task_title}\n"
    "请确保只合并经过审查的目标 commit，并在冲突时停止自动流程。"
)

LEADER_PROMPT_DEFAULT = (
    "你是一个专业的项目评估与分解专家。请评估以下任务是否需要分解：\n\n"
    "## 任务标题\n{task_title}\n\n"
    "## 任务描述\n{task_description}\n\n"
    "## 可用 Agent 类型\n{agent_list}\n\n"
    "## 评估标准\n"
    "- **简单任务**：可以由单个 agent 独立完成，工作量在 1-2 小时内\n"
    "- **复杂任务**：涉及多个独立功能模块，或需要不同专业技能协作\n\n"
    "## 输出格式（严格 JSON，不要任何其他文字）\n\n"
    "如果是简单任务：\n"
    '{"action": "simple", "reason": "一句话说明为何不需要分解"}\n\n'
    "如果是复杂任务：\n"
    '{"action": "decompose", "subtasks": [\n'
    '  {"title": "子任务标题", "description": "详细描述和验收标准", "agent": "developer"}\n'
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
            created_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS tasks (
            id              TEXT PRIMARY KEY,
            project_id      TEXT REFERENCES projects(id),
            title           TEXT NOT NULL,
            description     TEXT NOT NULL DEFAULT '',
            status          TEXT NOT NULL DEFAULT 'todo',
            assignee        TEXT,
            review_feedback TEXT,
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
    """)

    # Migrations for existing DBs
    existing = {r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()}
    for col, defn in [
        ("project_id",     "TEXT"),
        ("archived",       "INTEGER NOT NULL DEFAULT 0"),
        ("parent_task_id", "TEXT"),
        ("assigned_agent", "TEXT"),
        ("dev_agent",      "TEXT"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {defn}")

    handoff_existing = {r[1] for r in conn.execute("PRAGMA table_info(task_handoffs)").fetchall()}
    for col, defn in [
        ("commit_hash", "TEXT"),
        ("conclusion", "TEXT"),
    ]:
        if col not in handoff_existing:
            conn.execute(f"ALTER TABLE task_handoffs ADD COLUMN {col} {defn}")

    _seed_builtin_agents(conn)
    _recover_reviewer_stuck_tasks(conn)
    conn.commit()
    conn.close()


def _seed_builtin_agents(conn):
    """Insert built-in agent records if they don't exist yet."""
    builtins = [
        {
            "key": "developer",
            "name": "开发者",
            "description": "实现任务需求，编写代码并提交到 dev 分支",
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
            "description": "将审查通过的代码合并到主分支",
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


def _now():
    return datetime.utcnow().isoformat()


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
            "UPDATE tasks SET status=?, assignee=NULL WHERE status=? AND archived=0",
            (reset_to, working),
        )
    conn.commit()
    conn.close()


# ── Projects ──────────────────────────────────────────────────────────────────

def create_project(name: str, path: str) -> dict:
    conn = get_conn()
    pid = str(uuid.uuid4())
    now = _now()
    conn.execute(
        "INSERT INTO projects (id, name, path, created_at) VALUES (?,?,?,?)",
        (pid, name, path, now),
    )
    conn.commit()
    row = conn.execute("SELECT * FROM projects WHERE id=?", (pid,)).fetchone()
    conn.close()
    return dict(row)


def get_project(project_id: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM projects WHERE id=?", (project_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def list_projects() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM projects ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


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


# ── Tasks ─────────────────────────────────────────────────────────────────────

def _join_project(base_sql: str) -> str:
    """Wrap a task query to also return project.path as project_path."""
    return f"""
        SELECT t.*, p.path as project_path, p.name as project_name
        FROM ({base_sql}) t
        LEFT JOIN projects p ON t.project_id = p.id
    """


def create_task(title: str, description: str, project_id: str | None = None,
                parent_task_id: str | None = None,
                assigned_agent: str | None = None,
                dev_agent: str | None = None,
                status: str = "triage") -> dict:
    conn = get_conn()
    tid = str(uuid.uuid4())
    now = _now()
    conn.execute(
        """INSERT INTO tasks
           (id, project_id, title, description, status,
            parent_task_id, assigned_agent, dev_agent, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (tid, project_id, title, description, status,
         parent_task_id, assigned_agent, dev_agent, now, now),
    )
    conn.commit()
    row = conn.execute(
        _join_project("SELECT * FROM tasks WHERE id=?"), (tid,)
    ).fetchone()
    conn.close()
    return dict(row)


def list_subtasks(parent_task_id: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        _join_project("SELECT * FROM tasks WHERE parent_task_id=? ORDER BY created_at ASC"),
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


def get_task(task_id: str) -> dict | None:
    conn = get_conn()
    row = conn.execute(
        _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def list_tasks(project_id: str | None = None) -> list[dict]:
    conn = get_conn()
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


def update_task(task_id: str, **fields) -> dict | None:
    conn = get_conn()
    current = conn.execute(
        "SELECT status FROM tasks WHERE id=?",
        (task_id,),
    ).fetchone()
    if not current:
        conn.close()
        return None
    # Canceled tasks are immutable by normal updates so late agent writes
    # cannot resurrect them.
    if current["status"] == CANCELLED_STATUS:
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    if not fields:
        row = conn.execute(
            _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    fields["updated_at"] = _now()
    set_clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [task_id]
    conn.execute(f"UPDATE tasks SET {set_clause} WHERE id=?", values)
    conn.commit()
    row = conn.execute(
        _join_project("SELECT * FROM tasks WHERE id=?"), (task_id,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def get_tasks_by_status(status: str, project_id: str | None = None) -> list[dict]:
    conn = get_conn()
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
               project_id: str | None = None) -> dict | None:
    """
    Atomically claim the next task in `status` and move it to `working_status`.
    Returns the claimed task row (with joined project fields) or None.
    """
    conn = get_conn()
    try:
        conn.execute("BEGIN IMMEDIATE")

        where = ["status=?", "archived=0"]
        params: list[str] = [status]

        if project_id:
            where.append("project_id=?")
            params.append(project_id)

        if respect_assignment:
            where.append("(assigned_agent IS NULL OR assigned_agent=?)")
            params.append(agent_key)

        row = conn.execute(
            f"SELECT id FROM tasks WHERE {' AND '.join(where)} ORDER BY updated_at ASC LIMIT 1",
            tuple(params),
        ).fetchone()
        if not row:
            conn.rollback()
            return None

        now = _now()
        cur = conn.execute(
            "UPDATE tasks SET status=?, assignee=?, updated_at=? WHERE id=? AND status=? AND archived=0",
            (working_status, agent, now, row["id"], status),
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


def add_log(task_id: str, agent: str, message: str) -> dict:
    conn = get_conn()
    now = _now()
    cur = conn.execute(
        "INSERT INTO logs (task_id, agent, message, created_at) VALUES (?,?,?,?)",
        (task_id, agent, message, now),
    )
    log = {"id": cur.lastrowid, "task_id": task_id, "agent": agent,
           "message": message, "created_at": now}
    conn.commit()
    conn.close()
    return log


def get_logs(task_id: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM logs WHERE task_id=? ORDER BY created_at ASC", (task_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


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
    conn.commit()
    row = conn.execute(
        "SELECT * FROM task_handoffs WHERE id=?",
        (cur.lastrowid,),
    ).fetchone()
    conn.close()
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
