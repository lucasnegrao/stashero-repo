import os
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set


class DBService:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None
        self._writes_since_commit = 0
        self._commit_every = 50
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = self._connect()
        return self._conn

    def _init_db(self) -> None:
        conn = self._get_conn()
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS operation_batches (
                id TEXT PRIMARY KEY,
                mode TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                success INTEGER,
                error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_operations (
                id TEXT PRIMARY KEY,
                operation_type TEXT NOT NULL,
                batch_id TEXT,
                related_operation_id TEXT,
                created_at TEXT NOT NULL,
                scene_id TEXT NOT NULL,
                old_path TEXT NOT NULL,
                new_path TEXT NOT NULL,
                old_name TEXT NOT NULL,
                new_name TEXT NOT NULL,
                success INTEGER NOT NULL DEFAULT 1,
                error TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS rename_templates (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL,
                filename_template TEXT NOT NULL,
                path_template TEXT,
                filter_json TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hook_configs (
                hook_type TEXT PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 0,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS hook_template_bindings (
                id TEXT PRIMARY KEY,
                hook_type TEXT NOT NULL,
                template_id TEXT NOT NULL,
                sort_order INTEGER NOT NULL DEFAULT 0,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS watchdog_configs (
                id TEXT PRIMARY KEY,
                path TEXT NOT NULL,
                operation TEXT NOT NULL,
                options TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                sort_order INTEGER NOT NULL DEFAULT 0
            )
            """
        )
        try:
            conn.execute("ALTER TABLE file_operations ADD COLUMN batch_id TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE rename_templates ADD COLUMN filter_json TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            conn.execute("ALTER TABLE watchdog_configs ADD COLUMN sort_order INTEGER NOT NULL DEFAULT 0")
        except sqlite3.OperationalError:
            pass
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_operations_related ON file_operations(related_operation_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_file_operations_batch ON file_operations(batch_id)"
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_operations_scene ON file_operations(scene_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_file_operations_type_created ON file_operations(operation_type, created_at)")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_operation_batches_started ON operation_batches(started_at)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_rename_templates_created ON rename_templates(created_at)"
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_hook_template_bindings_unique ON hook_template_bindings(hook_type, template_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_hook_template_bindings_order ON hook_template_bindings(hook_type, sort_order)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_watchdog_configs_enabled ON watchdog_configs(enabled)"
        )
        conn.commit()

    def start_batch(
        self,
        mode: str,
        reuse_latest_for_mode: bool = False,
        fixed_batch_id: Optional[str] = None,
    ) -> str:
        normalized_mode = str(mode or "rename")
        ts = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()

        if fixed_batch_id:
            batch_id = str(fixed_batch_id).strip()
            if not batch_id:
                raise ValueError("fixed_batch_id cannot be empty")
            existing = conn.execute(
                """
                SELECT id
                FROM operation_batches
                WHERE id = ?
                LIMIT 1
                """,
                (batch_id,),
            ).fetchone()
            if existing:
                conn.execute(
                    """
                    UPDATE operation_batches
                    SET mode = ?, started_at = ?, completed_at = NULL, success = NULL, error = NULL
                    WHERE id = ?
                    """,
                    (normalized_mode, ts, batch_id),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO operation_batches (id, mode, started_at, completed_at, success, error)
                    VALUES (?, ?, ?, NULL, NULL, NULL)
                    """,
                    (batch_id, normalized_mode, ts),
                )
            self._writes_since_commit += 1
            if self._writes_since_commit >= self._commit_every:
                conn.commit()
                self._writes_since_commit = 0
            return batch_id

        if reuse_latest_for_mode:
            existing = conn.execute(
                """
                SELECT id
                FROM operation_batches
                WHERE mode = ?
                ORDER BY started_at DESC, id DESC
                LIMIT 1
                """,
                (normalized_mode,),
            ).fetchone()
            if existing:
                batch_id = str(existing["id"])
                conn.execute(
                    """
                    UPDATE operation_batches
                    SET started_at = ?, completed_at = NULL, success = NULL, error = NULL
                    WHERE id = ?
                    """,
                    (ts, batch_id),
                )
                self._writes_since_commit += 1
                if self._writes_since_commit >= self._commit_every:
                    conn.commit()
                    self._writes_since_commit = 0
                return batch_id

        batch_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO operation_batches (id, mode, started_at, completed_at, success, error)
            VALUES (?, ?, ?, NULL, NULL, NULL)
            """,
            (batch_id, normalized_mode, ts),
        )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0
        return batch_id

    def complete_batch(self, batch_id: str, success: bool, error: Optional[str] = None) -> None:
        ts = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        conn.execute(
            """
            UPDATE operation_batches
            SET completed_at = ?, success = ?, error = ?
            WHERE id = ?
            """,
            (ts, 1 if success else 0, error, batch_id),
        )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0

    def log_operation(
        self,
        operation_type: str,
        scene_id: str,
        old_path: str,
        new_path: str,
        batch_id: Optional[str] = None,
        related_operation_id: Optional[str] = None,
        success: bool = True,
        error: Optional[str] = None,
    ) -> str:
        op_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc).isoformat()
        old_name = os.path.basename(old_path or "")
        new_name = os.path.basename(new_path or "")
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO file_operations (
                id, operation_type, batch_id, related_operation_id, created_at,
                scene_id, old_path, new_path, old_name, new_name,
                success, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                op_id,
                operation_type,
                batch_id,
                related_operation_id,
                ts,
                str(scene_id or ""),
                str(old_path or ""),
                str(new_path or ""),
                old_name,
                new_name,
                1 if success else 0,
                error,
            ),
        )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0
        return op_id

    def get_operation(self, op_id: str) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        row = conn.execute(
            """
            SELECT id, operation_type, batch_id, related_operation_id, created_at,
                   scene_id, old_path, new_path, old_name, new_name, success, error
            FROM file_operations
            WHERE id = ?
            """,
            (op_id,),
        ).fetchone()
        if not row:
            return None
        return dict(row)

    def list_rename_operations(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        rows = conn.execute(
            """
            SELECT
                r.id,
                r.operation_type,
                r.batch_id,
                r.related_operation_id,
                r.created_at,
                r.scene_id,
                r.old_path,
                r.new_path,
                r.old_name,
                r.new_name,
                r.success,
                r.error,
                CASE
                    WHEN LOWER(COALESCE(r.error, '')) LIKE '%no change (same path and filename)%' THEN 'warn'
                    WHEN r.success = 1 THEN 'success'
                    ELSE 'error'
                END AS status,
                EXISTS (
                    SELECT 1
                    FROM file_operations u
                    WHERE
                        u.operation_type = 'undo'
                        AND u.related_operation_id = r.id
                        AND u.success = 1
                ) AS undone
            FROM file_operations r
            WHERE r.operation_type = 'rename'
            ORDER BY r.created_at DESC, r.id DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def list_operation_batches(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        rows = conn.execute(
            """
            SELECT
              b.id,
              b.mode,
              b.started_at,
              b.completed_at,
              b.success,
              b.error,
              COUNT(o.id) AS operations_count,
              SUM(CASE WHEN o.success = 1 THEN 1 ELSE 0 END) AS success_count,
              SUM(
                CASE
                  WHEN o.success = 0 AND LOWER(COALESCE(o.error, '')) LIKE '%no change (same path and filename)%'
                  THEN 1
                  ELSE 0
                END
              ) AS warn_count,
              SUM(
                CASE
                  WHEN o.success = 0 AND LOWER(COALESCE(o.error, '')) NOT LIKE '%no change (same path and filename)%'
                  THEN 1
                  ELSE 0
                END
              ) AS error_count,
              SUM(CASE WHEN o.operation_type = 'rename' THEN 1 ELSE 0 END) AS rename_count,
              SUM(CASE WHEN o.operation_type = 'dry_run' THEN 1 ELSE 0 END) AS dry_run_count,
              SUM(CASE WHEN o.operation_type = 'undo' THEN 1 ELSE 0 END) AS undo_count
            FROM operation_batches b
            LEFT JOIN file_operations o ON o.batch_id = b.id
            GROUP BY b.id
            ORDER BY b.started_at DESC, b.id DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def list_batch_operations(self, batch_id: str) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        rows = conn.execute(
            """
            SELECT
                r.id,
                r.batch_id,
                r.operation_type,
                r.related_operation_id,
                r.created_at,
                r.scene_id,
                r.old_path,
                r.new_path,
                r.old_name,
                r.new_name,
                r.success,
                r.error,
                CASE
                    WHEN LOWER(COALESCE(r.error, '')) LIKE '%no change (same path and filename)%' THEN 'warn'
                    WHEN r.success = 1 THEN 'success'
                    ELSE 'error'
                END AS status,
                EXISTS (
                    SELECT 1
                    FROM file_operations u
                    WHERE
                        u.operation_type = 'undo'
                        AND u.related_operation_id = r.id
                        AND u.success = 1
                ) AS undone
            FROM file_operations r
            WHERE r.batch_id = ?
            ORDER BY r.created_at DESC, r.id DESC
            """,
            (batch_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def list_batch_undo_candidates(self, batch_id: str) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        rows = conn.execute(
            """
            SELECT
                r.id,
                r.scene_id,
                r.old_path,
                r.new_path
            FROM file_operations r
            WHERE
                r.batch_id = ?
                AND r.operation_type = 'rename'
                AND r.success = 1
                AND NOT EXISTS (
                    SELECT 1 FROM file_operations u
                    WHERE u.operation_type = 'undo' AND u.related_operation_id = r.id AND u.success = 1
                )
            ORDER BY r.created_at DESC, r.id DESC
            """,
            (batch_id,),
        ).fetchall()
        return [dict(r) for r in rows]

    def clear_history(self) -> Dict[str, Any]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0

        operations_row = conn.execute("SELECT COUNT(1) AS count FROM file_operations").fetchone()
        batches_row = conn.execute("SELECT COUNT(1) AS count FROM operation_batches").fetchone()
        deleted_operations = int(operations_row["count"]) if operations_row else 0
        deleted_batches = int(batches_row["count"]) if batches_row else 0

        conn.execute("DELETE FROM file_operations")
        conn.execute("DELETE FROM operation_batches")
        conn.commit()
        self._writes_since_commit = 0

        return {
            "deleted_operations": deleted_operations,
            "deleted_batches": deleted_batches,
        }

    def flush(self) -> None:
        if self._conn is None:
            return
        self._conn.commit()
        self._writes_since_commit = 0

    def save_template(
        self,
        name: str,
        filename_template: str,
        path_template: Optional[str],
        criteria_json: Optional[str] = None,
    ) -> Dict[str, Any]:
        template_id = str(uuid.uuid4())
        ts = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO rename_templates (
                id, name, filename_template, path_template, filter_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                template_id,
                str(name or "").strip(),
                str(filename_template or ""),
                str(path_template or ""),
                str(criteria_json or ""),
                ts,
            ),
        )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0
        return {
            "id": template_id,
            "name": str(name or "").strip(),
            "filename_template": str(filename_template or ""),
            "path_template": str(path_template or ""),
            "filter_json": str(criteria_json or ""),
            "created_at": ts,
        }

    def update_template(
        self,
        template_id: str,
        name: str,
        filename_template: str,
        path_template: Optional[str],
        criteria_json: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        conn = self._get_conn()
        conn.execute(
            """
            UPDATE rename_templates
            SET name = ?, filename_template = ?, path_template = ?, filter_json = ?
            WHERE id = ?
            """,
            (
                str(name or "").strip(),
                str(filename_template or ""),
                str(path_template or ""),
                str(criteria_json or ""),
                str(template_id or ""),
            ),
        )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0
        row = conn.execute(
            """
            SELECT id, name, filename_template, path_template, filter_json, created_at
            FROM rename_templates
            WHERE id = ?
            """,
            (str(template_id or ""),),
        ).fetchone()
        return dict(row) if row else None

    def delete_template(self, template_id: str) -> bool:
        conn = self._get_conn()
        conn.execute(
            "DELETE FROM hook_template_bindings WHERE template_id = ?",
            (str(template_id or ""),),
        )
        cur = conn.execute(
            "DELETE FROM rename_templates WHERE id = ?",
            (str(template_id or ""),),
        )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0
        return bool(cur.rowcount and cur.rowcount > 0)

    def list_templates(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        rows = conn.execute(
            """
            SELECT id, name, filename_template, path_template, filter_json, created_at
            FROM rename_templates
            ORDER BY created_at DESC, id DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def list_templates_by_ids(self, template_ids: List[str]) -> List[Dict[str, Any]]:
        ids = [str(x or "").strip() for x in (template_ids or []) if str(x or "").strip()]
        if not ids:
            return []
        placeholders = ",".join("?" for _ in ids)
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        rows = conn.execute(
            f"""
            SELECT id, name, filename_template, path_template, filter_json, created_at
            FROM rename_templates
            WHERE id IN ({placeholders})
            """,
            ids,
        ).fetchall()
        by_id = {str(r["id"]): dict(r) for r in rows}
        return [by_id[id_value] for id_value in ids if id_value in by_id]

    def get_hook_settings(self, hook_type: str) -> Dict[str, Any]:
        normalized_hook_type = str(hook_type or "").strip() or "Scene.Update.Post"
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        row = conn.execute(
            """
            SELECT hook_type, enabled, updated_at
            FROM hook_configs
            WHERE hook_type = ?
            """,
            (normalized_hook_type,),
        ).fetchone()
        template_rows = conn.execute(
            """
            SELECT template_id
            FROM hook_template_bindings
            WHERE hook_type = ?
            ORDER BY sort_order ASC, created_at ASC, id ASC
            """,
            (normalized_hook_type,),
        ).fetchall()
        return {
            "hook_type": normalized_hook_type,
            "enabled": bool(row["enabled"]) if row else False,
            "updated_at": str(row["updated_at"]) if row and row["updated_at"] else None,
            "template_ids": [str(r["template_id"]) for r in template_rows],
        }

    def save_hook_settings(
        self,
        hook_type: str,
        enabled: bool,
        template_ids: List[str],
    ) -> Dict[str, Any]:
        normalized_hook_type = str(hook_type or "").strip() or "Scene.Update.Post"
        normalized_ids: List[str] = []
        seen: Set[str] = set()
        for template_id in (template_ids or []):
            value = str(template_id or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            normalized_ids.append(value)

        ts = datetime.now(timezone.utc).isoformat()
        conn = self._get_conn()
        conn.execute(
            """
            INSERT INTO hook_configs (hook_type, enabled, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(hook_type) DO UPDATE SET enabled = excluded.enabled, updated_at = excluded.updated_at
            """,
            (normalized_hook_type, 1 if enabled else 0, ts),
        )
        conn.execute(
            "DELETE FROM hook_template_bindings WHERE hook_type = ?",
            (normalized_hook_type,),
        )
        for index, template_id in enumerate(normalized_ids):
            conn.execute(
                """
                INSERT INTO hook_template_bindings (id, hook_type, template_id, sort_order, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    normalized_hook_type,
                    template_id,
                    int(index),
                    ts,
                ),
            )

        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0

        return self.get_hook_settings(normalized_hook_type)

    def save_watchdog_config(
        self,
        config_id: Optional[str],
        path: str,
        operation: str,
        options: Optional[str],
        enabled: bool,
        sort_order: Optional[int] = None,
    ) -> Dict[str, Any]:
        normalized_id = str(config_id or "").strip() or str(uuid.uuid4())
        normalized_path = str(path or "").strip()
        normalized_operation = str(operation or "").strip()
        normalized_options = str(options or "")
        conn = self._get_conn()

        if sort_order is None:
            existing_row = conn.execute(
                """
                SELECT path, sort_order
                FROM watchdog_configs
                WHERE id = ?
                """,
                (normalized_id,),
            ).fetchone()
            if existing_row and str(existing_row["path"] or "").strip() == normalized_path:
                # Preserve current explicit user-defined order on in-place updates
                # (e.g. toggling enabled), unless caller explicitly sends sort_order.
                sort_order = int(existing_row["sort_order"])
            else:
                row = conn.execute(
                    "SELECT MAX(sort_order) as max_order FROM watchdog_configs WHERE path = ?",
                    (normalized_path,),
                ).fetchone()
                sort_order = (
                    row["max_order"] + 1
                    if row and row["max_order"] is not None
                    else 0
                )

        conn.execute(
            """
            INSERT INTO watchdog_configs (id, path, operation, options, enabled, sort_order)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                path = excluded.path,
                operation = excluded.operation,
                options = excluded.options,
                enabled = excluded.enabled,
                sort_order = excluded.sort_order
            """,
            (
                normalized_id,
                normalized_path,
                normalized_operation,
                normalized_options,
                1 if enabled else 0,
                sort_order,
            ),
        )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0

        row = conn.execute(
            """
            SELECT id, path, operation, options, enabled, sort_order
            FROM watchdog_configs
            WHERE id = ?
            """,
            (normalized_id,),
        ).fetchone()
        if not row:
            raise RuntimeError("Failed to persist watchdog config")
        return dict(row)

    def list_watchdog_configs(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        rows = conn.execute(
            """
            SELECT id, path, operation, options, enabled, sort_order
            FROM watchdog_configs
            ORDER BY path ASC, sort_order ASC, rowid DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def list_enabled_watchdog_configs(self) -> List[Dict[str, Any]]:
        conn = self._get_conn()
        conn.commit()
        self._writes_since_commit = 0
        rows = conn.execute(
            """
            SELECT id, path, operation, options, enabled, sort_order
            FROM watchdog_configs
            WHERE enabled = 1
            ORDER BY path ASC, sort_order ASC, rowid DESC
            """
        ).fetchall()
        return [dict(r) for r in rows]

    def reorder_watchdog_configs(self, path: str, config_ids: List[str]) -> None:
        conn = self._get_conn()
        normalized_path = str(path or "").strip()
        for index, config_id in enumerate(config_ids):
            conn.execute(
                """
                UPDATE watchdog_configs
                SET sort_order = ?
                WHERE id = ? AND path = ?
                """,
                (index, str(config_id).strip(), normalized_path),
            )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0

    def delete_watchdog_config(self, config_id: str) -> bool:
        normalized_id = str(config_id or "").strip()
        if not normalized_id:
            return False
        conn = self._get_conn()
        cur = conn.execute(
            "DELETE FROM watchdog_configs WHERE id = ?",
            (normalized_id,),
        )
        self._writes_since_commit += 1
        if self._writes_since_commit >= self._commit_every:
            conn.commit()
            self._writes_since_commit = 0
        return bool(cur.rowcount and cur.rowcount > 0)

    def close(self) -> None:
        if self._conn is None:
            return
        try:
            self._conn.commit()
        finally:
            self._conn.close()
            self._conn = None
            self._writes_since_commit = 0
