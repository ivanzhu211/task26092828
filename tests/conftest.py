"""Shared fixtures: an independent oracle and a harness driving both the
service and the oracle through the same operation sequence."""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone

import pytest

from backup_lib import BackupContent, BackupService, BackupType, Sector

T0 = datetime(2019, 1, 1, tzinfo=timezone.utc)


def t(days: float) -> datetime:
    return T0 + timedelta(days=days)


class Oracle:
    """Independent re-implementation of the business rules, used to
    cross-check plans, approvals and restores."""

    def __init__(self) -> None:
        self.backups: dict[int, dict] = {}

    def add(self, bid: int, when: datetime, btype: BackupType,
            base_id: int | None, content: BackupContent) -> None:
        self.backups[bid] = {
            "time": when, "type": btype, "base_id": base_id,
            "content": content, "corrupted": False, "deleted": False,
        }

    def mark_corrupted(self, bid: int) -> None:
        self.backups[bid]["corrupted"] = True

    def chain_ids(self, bid: int) -> list[int]:
        out = []
        cur: int | None = bid
        while cur is not None:
            out.append(cur)
            cur = self.backups[cur]["base_id"]
        out.reverse()
        return out

    def is_recoverable(self, bid: int) -> bool:
        return all(not self.backups[c]["corrupted"]
                   and not self.backups[c]["deleted"]
                   for c in self.chain_ids(bid))

    def rows_at(self, bid: int) -> dict[str, bytes]:
        state: dict[str, bytes] = {}
        for cid in self.chain_ids(bid):
            content = self.backups[cid]["content"]
            for key in content.deleted:
                state.pop(key, None)
            state.update(content.added)
            state.update(content.modified)
        return state

    def keep_set(self, now: datetime, days: int) -> set[int]:
        start = now - timedelta(days=days)
        anchors = {bid for bid, b in self.backups.items()
                   if not b["deleted"] and b["time"] >= start}
        before = [bid for bid, b in self.backups.items()
                  if not b["deleted"] and b["time"] <= start
                  and self.is_recoverable(bid)]
        if before:
            anchors.add(max(before,
                            key=lambda bid: (self.backups[bid]["time"], bid)))
        keep: set[int] = set()
        for anchor in anchors:
            keep.update(self.chain_ids(anchor))
        return keep

    def expired(self, now: datetime, days: int) -> list[int]:
        keep = self.keep_set(now, days)
        return sorted(bid for bid, b in self.backups.items()
                      if not b["deleted"] and bid not in keep)


class Harness:
    def __init__(self, svc: BackupService, source: str) -> None:
        self.svc = svc
        self.source = source
        self.oracle = Oracle()

    def record(self, when: datetime, btype: BackupType,
               content: BackupContent, operator: str = "op"):
        rec = self.svc.record_backup(self.source, when, btype, content, operator)
        self.oracle.add(rec.id, when, btype, rec.base_id, content)
        return rec

    def corrupt_file(self, bid: int) -> None:
        path = os.path.join(self.svc._store.storage_dir, f"{bid}.bak")
        with open(path, "ab") as fh:
            fh.write(b"tampered")
        marked = self.svc.verify(self.source).marked_corrupted
        assert bid in marked
        self.oracle.mark_corrupted(bid)

    def delete_file(self, bid: int) -> None:
        os.unlink(os.path.join(self.svc._store.storage_dir, f"{bid}.bak"))
        marked = self.svc.verify(self.source).marked_corrupted
        assert bid in marked
        self.oracle.mark_corrupted(bid)

    def check_survivors(self) -> None:
        """Every surviving recoverable backup must restore exactly."""
        recoverable = self.svc.recoverable_ids(self.source)
        expected = {bid for bid in self.oracle.backups
                    if self.oracle.is_recoverable(bid)}
        assert recoverable == expected
        for bid in sorted(recoverable):
            got = self.svc.restore(self.source, bid)
            assert got == self.oracle.rows_at(bid), f"backup {bid}"


@pytest.fixture()
def svc(tmp_path):
    service = BackupService(str(tmp_path / "catalog.db"),
                            str(tmp_path / "files"))
    yield service


@pytest.fixture()
def fin(svc):
    svc.register_source("fin", Sector.FINANCE, 2555)
    return Harness(svc, "fin")
