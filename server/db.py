import json
import sqlite3
import uuid
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "tasks.db"


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

    _seed_builtin_agents(conn)
    conn.commit()
    conn.close()


def _seed_builtin_agents(conn):
    """Insert built-in agent records if they don't exist yet."""
    builtins = [
        {
            "key": "developer",
            "name": "开发者",
            "description": "实现任务需求，编写代码并提交到 dev 分支",
            "poll_statuses": '["todo","needs_changes"]',
            "next_status": "in_review",
            "working_status": "in_progress",
        },
        {
            "key": "reviewer",
            "name": "审查者",
            "description": "审查代码变更，决定通过或要求修改",
            "poll_statuses": '["in_review"]',
            "next_status": "approved",
            "working_status": "reviewing",
        },
        {
            "key": "manager",
            "name": "合并管理者",
            "description": "将审查通过的代码合并到主分支",
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
            "prompt": (
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
            ),
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
            "UPDATE tasks SET status=?, assignee=NULL WHERE status=?",
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

        where = ["status=?"]
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
            "UPDATE tasks SET status=?, assignee=?, updated_at=? WHERE id=? AND status=?",
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
