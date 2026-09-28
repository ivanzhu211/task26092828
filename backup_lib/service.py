"""High-level backup service (entry point for the scheduler).

Concurrency model: every public method runs its catalog changes inside one
``BEGIN IMMEDIATE`` transaction, so SQLite serializes writers across
processes and threads. Restore also takes the write lock, which guarantees
it can never observe a backup whose file is being unlinked by a cleanup
that already committed the catalog deletion.
"""

from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from . import files
from .chain import BRow, build_chain, compute_expired, compute_recoverable
from .errors import (
    ApprovalError,
    BaseCorruptedError,
    BaseMismatchError,
    DuplicateBackupError,
    RequestExpiredError,
    RestoreError,
    ValidationError,
)
from .models import (
    SECTOR_MIN_RETENTION_DAYS,
    ApprovalResult,
    BackupContent,
    BackupRecord,
    BackupType,
    CleanupPlan,
    ItemResult,
    ItemStatus,
    Sector,
    VerificationResult,
)
from .store import Store

RowSet = dict[str, bytes]

EXPIRY_AFTER = timedelta(hours=72)


def _norm_time(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValidationError("datetime must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat()


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value)


class BackupService:
    def __init__(self, db_path: str, storage_dir: str) -> None:
        self._store = Store(db_path, storage_dir)

    @contextmanager
    def _session(self):
        conn = self._store.connect()
        try:
            yield conn
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # sources
    # ------------------------------------------------------------------
    def register_source(self, source: str, sector: Sector,
                        retention_days: int) -> None:
        sector = Sector(sector)
        self._check_retention(sector, retention_days)
        with self._session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO sources(name, sector, retention_days)"
                    " VALUES (?, ?, ?)",
                    (source, sector.value, retention_days))
            except sqlite3.IntegrityError as exc:
                conn.execute("ROLLBACK")
                raise ValidationError(f"source {source!r} already registered") from exc
            conn.execute("COMMIT")

    def set_retention_days(self, source: str, retention_days: int) -> None:
        with self._session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = self._source_row(conn, source)
            self._check_retention(Sector(row["sector"]), retention_days)
            conn.execute("UPDATE sources SET retention_days=? WHERE id=?",
                         (retention_days, row["id"]))
            conn.execute("COMMIT")

    @staticmethod
    def _check_retention(sector: Sector, retention_days: int) -> None:
        minimum = SECTOR_MIN_RETENTION_DAYS.get(sector)
        if minimum is not None and retention_days < minimum:
            raise ValidationError(
                f"retention {retention_days}d below {sector.value} minimum {minimum}d")
        if retention_days < 0:
            raise ValidationError("retention days must be non-negative")

    @staticmethod
    def _source_row(conn: sqlite3.Connection, source: str) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM sources WHERE name=?",
                           (source,)).fetchone()
        if row is None:
            raise ValidationError(f"unknown source {source!r}")
        return row

    # ------------------------------------------------------------------
    # recording
    # ------------------------------------------------------------------
    def record_backup(self, source: str, backup_time: datetime,
                      backup_type: BackupType, content: BackupContent,
                      operator: str) -> BackupRecord:
        backup_type = BackupType(backup_type)
        self._validate_content(content, backup_type)
        time_text = _norm_time(backup_time)
        with self._session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            src = self._source_row(conn, source)
            latest = conn.execute(
                "SELECT id, backup_time, backup_type, corrupted FROM backups"
                " WHERE source_id=? AND deleted=0"
                " ORDER BY backup_time DESC, id DESC LIMIT 1",
                (src["id"],)).fetchone()
            if latest is not None and time_text <= latest["backup_time"]:
                raise ValidationError("backup_time must strictly increase")
            if latest is None and backup_type is not BackupType.FULL:
                raise BaseMismatchError("first backup of a source must be full")
            base_id = self._pick_base(conn, src["id"], backup_type)
            try:
                cur = conn.execute(
                    "INSERT INTO backups(source_id, backup_time, backup_type,"
                    " base_id, corrupted, deleted) VALUES (?,?,?,?,0,0)",
                    (src["id"], time_text, backup_type.value, base_id))
            except sqlite3.IntegrityError as exc:
                conn.execute("ROLLBACK")
                raise DuplicateBackupError(
                    f"backup at {time_text} already recorded") from exc
            backup_id = cur.lastrowid
            path, digest = files.write_backup_file(
                self._store.storage_dir, backup_id, content)
            conn.execute(
                "UPDATE backups SET file_path=?, sha256=? WHERE id=?",
                (str(path), digest, backup_id))
            conn.execute("COMMIT")
        return BackupRecord(id=backup_id, source=source,
                            backup_time=_parse_time(time_text),
                            backup_type=backup_type, base_id=base_id,
                            sha256=digest, corrupted=False, deleted=False)

    @staticmethod
    def _validate_content(content: BackupContent,
                          backup_type: BackupType) -> None:
        overlap = (set(content.added) & set(content.modified)
                   | set(content.added) & set(content.deleted)
                   | set(content.modified) & set(content.deleted))
        if overlap:
            raise ValidationError(
                f"row keys appear in multiple change groups: {sorted(overlap)}")
        if backup_type is BackupType.FULL and (
                content.modified or content.deleted):
            raise ValidationError(
                "a full backup carries the complete row set in 'added' only")

    @staticmethod
    def _pick_base(conn: sqlite3.Connection, source_id: int,
                   backup_type: BackupType) -> int | None:
        if backup_type is BackupType.FULL:
            return None
        if backup_type is BackupType.INCREMENTAL:
            base = conn.execute(
                "SELECT id, corrupted FROM backups"
                " WHERE source_id=? AND deleted=0"
                " ORDER BY backup_time DESC, id DESC LIMIT 1",
                (source_id,)).fetchone()
        else:  # DIFFERENTIAL: latest existing full
            base = conn.execute(
                "SELECT id, corrupted FROM backups"
                " WHERE source_id=? AND deleted=0"
                " AND backup_type=? ORDER BY backup_time DESC, id DESC LIMIT 1",
                (source_id, BackupType.FULL.value)).fetchone()
        if base is None:
            raise BaseMismatchError(
                f"no existing base available for {backup_type.value} backup")
        if base["corrupted"]:
            raise BaseCorruptedError(
                f"base backup {base['id']} is marked corrupted")
        return base["id"]

    # ------------------------------------------------------------------
    # restore
    # ------------------------------------------------------------------
    def restore(self, source: str, backup_id: int) -> RowSet:
        with self._session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            src = self._source_row(conn, source)
            rows = self._load_rows(conn, src["id"])
            by_id = {r.id: r for r in rows}
            try:
                chain = build_chain(backup_id, by_id)
            except KeyError as exc:
                conn.execute("ROLLBACK")
                raise RestoreError(
                    f"backup {exc.args[0]} missing from dependency chain",
                    backup_id=exc.args[0]) from exc
            state: RowSet = {}
            for row in chain:
                if row.corrupted:
                    conn.execute("ROLLBACK")
                    raise RestoreError(
                        f"backup {row.id} is marked corrupted",
                        backup_id=row.id)
                meta = conn.execute(
                    "SELECT file_path, sha256 FROM backups WHERE id=?",
                    (row.id,)).fetchone()
                path = self._store.storage_dir / "__missing__" \
                    if meta["file_path"] is None else files.Path(meta["file_path"])
                ok = path.is_file() and files.sha256_file(path) == meta["sha256"]
                if not ok:
                    conn.execute(
                        "UPDATE backups SET corrupted=1 WHERE id=?", (row.id,))
                    conn.execute("COMMIT")
                    raise RestoreError(
                        f"backup {row.id} file missing or sha256 mismatch;"
                        " marked corrupted", backup_id=row.id)
                state = self._apply(state, files.read_backup_file(path))
            conn.execute("COMMIT")
            return state

    @staticmethod
    def _apply(state: RowSet, content: BackupContent) -> RowSet:
        state = dict(state)
        for key in content.deleted:
            state.pop(key, None)
        state.update(content.added)
        state.update(content.modified)
        return state

    # ------------------------------------------------------------------
    # verification
    # ------------------------------------------------------------------
    def verify(self, source: str | None = None) -> VerificationResult:
        marked: list[int] = []
        checked = 0
        with self._session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if source is None:
                rows = conn.execute(
                    "SELECT id, file_path, sha256 FROM backups"
                    " WHERE deleted=0 AND corrupted=0").fetchall()
            else:
                src = self._source_row(conn, source)
                rows = conn.execute(
                    "SELECT id, file_path, sha256 FROM backups"
                    " WHERE deleted=0 AND corrupted=0 AND source_id=?",
                    (src["id"],)).fetchall()
            for row in rows:
                checked += 1
                path = files.Path(row["file_path"]) if row["file_path"] else None
                ok = (path is not None and path.is_file()
                      and files.sha256_file(path) == row["sha256"])
                if not ok:
                    conn.execute("UPDATE backups SET corrupted=1 WHERE id=?",
                                 (row["id"],))
                    marked.append(row["id"])
            conn.execute("COMMIT")
        return VerificationResult(checked=checked, marked_corrupted=tuple(marked))

    # ------------------------------------------------------------------
    # cleanup
    # ------------------------------------------------------------------
    def submit_cleanup(self, source: str, now: datetime,
                       operator: str) -> CleanupPlan:
        now_text = _norm_time(now)
        with self._session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            src = self._source_row(conn, source)
            rows = self._load_rows(conn, src["id"])
            start = _parse_time(now_text) - timedelta(days=src["retention_days"])
            expired = compute_expired(rows, start)
            cur = conn.execute(
                "INSERT INTO cleanup_requests(source_id, submitted_at,"
                " submitted_by) VALUES (?,?,?)",
                (src["id"], now_text, operator))
            request_id = cur.lastrowid
            conn.executemany(
                "INSERT INTO cleanup_items(request_id, backup_id) VALUES (?,?)",
                [(request_id, bid) for bid in expired])
            conn.execute("COMMIT")
        return CleanupPlan(request_id=request_id, source=source,
                           submitted_at=_parse_time(now_text),
                           submitted_by=operator,
                           expired_backup_ids=tuple(expired))

    def approve_cleanup(self, request_id: int, now: datetime,
                        operator: str) -> ApprovalResult:
        now_text = _norm_time(now)
        to_unlink: list[str] = []
        with self._session() as conn:
            conn.execute("BEGIN IMMEDIATE")
            req = conn.execute(
                "SELECT * FROM cleanup_requests WHERE id=?",
                (request_id,)).fetchone()
            if req is None:
                conn.execute("ROLLBACK")
                raise ValidationError(f"unknown cleanup request {request_id}")
            if req["approved_at"] is not None:
                result = self._stored_result(conn, req)
                conn.execute("COMMIT")
                return result
            if operator == req["submitted_by"]:
                conn.execute("ROLLBACK")
                raise ApprovalError("approver must differ from submitter")
            submitted_at = _parse_time(req["submitted_at"])
            if _parse_time(now_text) - submitted_at >= EXPIRY_AFTER:
                conn.execute("ROLLBACK")
                raise RequestExpiredError(
                    f"request {request_id} not approved within 72h")
            rows = self._load_rows(conn, req["source_id"])
            by_id = {r.id: r for r in rows}
            src = conn.execute("SELECT * FROM sources WHERE id=?",
                               (req["source_id"],)).fetchone()
            start = _parse_time(now_text) - timedelta(days=src["retention_days"])
            expired_now = set(compute_expired(rows, start))
            items: list[ItemResult] = []
            planned = conn.execute(
                "SELECT backup_id FROM cleanup_items WHERE request_id=?"
                " ORDER BY backup_id", (request_id,)).fetchall()
            for item in planned:
                bid = item["backup_id"]
                row = by_id.get(bid)
                if row is None or row.deleted:
                    status, reason = (ItemStatus.ALREADY_MISSING,
                                      "backup no longer exists")
                elif bid not in expired_now:
                    status, reason = (ItemStatus.STILL_REQUIRED,
                                      "still required under current state")
                else:
                    meta = conn.execute(
                        "SELECT file_path FROM backups WHERE id=?",
                        (bid,)).fetchone()
                    conn.execute("UPDATE backups SET deleted=1 WHERE id=?",
                                 (bid,))
                    if meta["file_path"]:
                        to_unlink.append(meta["file_path"])
                    status, reason = ItemStatus.DELETED, ""
                conn.execute(
                    "UPDATE cleanup_items SET final_status=?, final_reason=?"
                    " WHERE request_id=? AND backup_id=?",
                    (status.value, reason, request_id, bid))
                items.append(ItemResult(backup_id=bid, status=status,
                                        reason=reason))
            conn.execute(
                "UPDATE cleanup_requests SET approved_at=?, approved_by=?"
                " WHERE id=?", (now_text, operator, request_id))
            conn.execute("COMMIT")
        for path in to_unlink:
            files.delete_backup_file(files.Path(path))
        return ApprovalResult(request_id=request_id,
                              approved_at=_parse_time(now_text),
                              approved_by=operator, items=tuple(items))

    @staticmethod
    def _stored_result(conn: sqlite3.Connection,
                       req: sqlite3.Row) -> ApprovalResult:
        rows = conn.execute(
            "SELECT backup_id, final_status, final_reason FROM cleanup_items"
            " WHERE request_id=? ORDER BY backup_id", (req["id"],)).fetchall()
        items = tuple(
            ItemResult(backup_id=r["backup_id"],
                       status=ItemStatus(r["final_status"]),
                       reason=r["final_reason"] or "")
            for r in rows)
        return ApprovalResult(request_id=req["id"],
                              approved_at=_parse_time(req["approved_at"]),
                              approved_by=req["approved_by"], items=items)

    # ------------------------------------------------------------------
    # shared helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _load_rows(conn: sqlite3.Connection, source_id: int) -> list[BRow]:
        rows = conn.execute(
            "SELECT id, backup_time, base_id, corrupted, deleted FROM backups"
            " WHERE source_id=? ORDER BY backup_time, id",
            (source_id,)).fetchall()
        return [BRow(id=r["id"], backup_time=_parse_time(r["backup_time"]),
                     base_id=r["base_id"], corrupted=bool(r["corrupted"]),
                     deleted=bool(r["deleted"])) for r in rows]

    # exposed for tests / callers that need catalog state
    def list_backups(self, source: str) -> list[BackupRecord]:
        with self._session() as conn:
            src = self._source_row(conn, source)
            rows = conn.execute(
                "SELECT * FROM backups WHERE source_id=?"
                " ORDER BY backup_time, id", (src["id"],)).fetchall()
            return [BackupRecord(
                id=r["id"], source=source,
                backup_time=_parse_time(r["backup_time"]),
                backup_type=BackupType(r["backup_type"]),
                base_id=r["base_id"], sha256=r["sha256"] or "",
                corrupted=bool(r["corrupted"]),
                deleted=bool(r["deleted"])) for r in rows]

    def recoverable_ids(self, source: str) -> set[int]:
        with self._session() as conn:
            src = self._source_row(conn, source)
            rows = self._load_rows(conn, src["id"])
        rec = compute_recoverable(rows)
        return {bid for bid, ok in rec.items() if ok}
