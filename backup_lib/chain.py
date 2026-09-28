"""Pure in-memory dependency-chain and retention-set algorithms.

These functions never touch the database or the filesystem, which makes
them easy to test against an independent oracle. Rows are expected sorted
by (backup_time, id) ascending, so a base always precedes its dependents.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class BRow:
    id: int
    backup_time: datetime
    base_id: int | None
    corrupted: bool
    deleted: bool


def build_chain(target_id: int, by_id: dict[int, BRow]) -> list[BRow]:
    """Return the dependency chain of ``target_id``, root first.

    Raises KeyError with the id of the first missing link.
    """
    chain: list[BRow] = []
    seen: set[int] = set()
    cur: int | None = target_id
    while cur is not None:
        if cur in seen:
            raise ValueError(f"cycle in backup chain at {cur}")
        seen.add(cur)
        row = by_id.get(cur)
        if row is None or row.deleted:
            raise KeyError(cur)
        chain.append(row)
        cur = row.base_id
    chain.reverse()
    return chain


def compute_recoverable(rows: list[BRow]) -> dict[int, bool]:
    """Map backup id -> recoverable (chain intact, nothing corrupted)."""
    by_id = {r.id: r for r in rows}
    result: dict[int, bool] = {}
    for row in rows:
        if row.deleted or row.corrupted:
            result[row.id] = False
            continue
        base = by_id.get(row.base_id) if row.base_id is not None else None
        if row.base_id is not None and base is None:
            result[row.id] = False  # base already purged
            continue
        if row.base_id is not None and (base.deleted or not result.get(base.id, False)):
            result[row.id] = False
            continue
        result[row.id] = True
    return result


def compute_keep_set(rows: list[BRow], retention_start: datetime) -> set[int]:
    """Ids that must be kept under the retention rules.

    Anchors: every existing backup with backup_time >= retention_start
    (corrupted ones included), plus the latest recoverable backup with
    backup_time <= retention_start. Keep set = anchors plus their full
    dependency chains.
    """
    by_id = {r.id: r for r in rows}
    recoverable = compute_recoverable(rows)
    anchors: set[int] = set()
    best_before: BRow | None = None
    for row in rows:
        if row.deleted:
            continue
        if row.backup_time >= retention_start:
            anchors.add(row.id)
        if (row.backup_time <= retention_start and recoverable[row.id]):
            best_before = row  # rows sorted by time: last one wins
    if best_before is not None:
        anchors.add(best_before.id)
    keep: set[int] = set()
    for anchor in anchors:
        cur: int | None = anchor
        while cur is not None and cur not in keep:
            keep.add(cur)
            row = by_id.get(cur)
            cur = row.base_id if row is not None else None
    return keep


def compute_expired(rows: list[BRow], retention_start: datetime) -> list[int]:
    """Existing backups that are not in the keep set, id ascending."""
    keep = compute_keep_set(rows, retention_start)
    return sorted(r.id for r in rows if not r.deleted and r.id not in keep)
