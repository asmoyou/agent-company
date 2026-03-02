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
    """)

    # Migrations for existing DBs
    existing = {r[1] for r in conn.execute("PRAGMA table_info(tasks)").fetchall()}
    for col, defn in [
        ("project_id", "TEXT"),
        ("archived",   "INTEGER NOT NULL DEFAULT 0"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE tasks ADD COLUMN {col} {defn}")

    conn.commit()
    conn.close()


def _now():
    return datetime.utcnow().isoformat()


def reset_stuck_tasks():
    """
    On server startup, reset tasks left in transient agent states
    back to the last stable state (in case of a crash/restart).
    """
    conn = get_conn()
    # in_progress → todo  (developer was working, didn't finish)
    conn.execute("UPDATE tasks SET status='todo',    assignee=NULL WHERE status='in_progress'")
    # reviewing   → in_review  (reviewer was working)
    conn.execute("UPDATE tasks SET status='in_review', assignee=NULL WHERE status='reviewing'")
    # merging     → approved   (manager was merging)
    conn.execute("UPDATE tasks SET status='approved',  assignee=NULL WHERE status='merging'")
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


def create_task(title: str, description: str, project_id: str | None = None) -> dict:
    conn = get_conn()
    tid = str(uuid.uuid4())
    now = _now()
    conn.execute(
        """INSERT INTO tasks
           (id, project_id, title, description, status, created_at, updated_at)
           VALUES (?,?,?,?,'todo',?,?)""",
        (tid, project_id, title, description, now, now),
    )
    conn.commit()
    row = conn.execute(
        _join_project("SELECT * FROM tasks WHERE id=?"), (tid,)
    ).fetchone()
    conn.close()
    return dict(row)


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
