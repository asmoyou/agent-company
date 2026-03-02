import sqlite3
import json
import uuid
from datetime import datetime
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "tasks.db"


def get_conn():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS tasks (
            id          TEXT PRIMARY KEY,
            title       TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            status      TEXT NOT NULL DEFAULT 'todo',
            assignee    TEXT,
            review_feedback TEXT,
            commit_hash TEXT,
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL
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
    conn.commit()
    conn.close()


def _now():
    return datetime.utcnow().isoformat()


def create_task(title: str, description: str) -> dict:
    conn = get_conn()
    task_id = str(uuid.uuid4())
    now = _now()
    conn.execute(
        "INSERT INTO tasks (id, title, description, status, created_at, updated_at) VALUES (?, ?, ?, 'todo', ?, ?)",
        (task_id, title, description, now, now),
    )
    conn.commit()
    task = dict(conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone())
    conn.close()
    return task


def get_task(task_id: str) -> dict | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def list_tasks() -> list[dict]:
    conn = get_conn()
    rows = conn.execute("SELECT * FROM tasks ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_task(task_id: str, **fields) -> dict | None:
    conn = get_conn()
    fields["updated_at"] = _now()
    set_clause = ", ".join(f"{k}=?" for k in fields)
    values = list(fields.values()) + [task_id]
    conn.execute(f"UPDATE tasks SET {set_clause} WHERE id=?", values)
    conn.commit()
    row = conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def add_log(task_id: str, agent: str, message: str) -> dict:
    conn = get_conn()
    now = _now()
    cur = conn.execute(
        "INSERT INTO logs (task_id, agent, message, created_at) VALUES (?, ?, ?, ?)",
        (task_id, agent, message, now),
    )
    log = {"id": cur.lastrowid, "task_id": task_id, "agent": agent, "message": message, "created_at": now}
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


def get_tasks_by_status(status: str) -> list[dict]:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM tasks WHERE status=? ORDER BY updated_at ASC", (status,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
