"""Multi-process / multi-thread concurrency tests."""

import multiprocessing as mp
import os
import threading
from datetime import timedelta

import pytest

from backup_lib import BackupContent, BackupService, BackupType, ItemStatus, Sector
from backup_lib.errors import ValidationError
from conftest import T0, t


def _content_for(when):
    return BackupContent(added={f"k{when.timestamp()}": str(when.timestamp()).encode()})


def _recorder(db, files, source, count, q):
    svc = BackupService(db, files)
    done = 0
    while done < count:
        latest = max((b.backup_time for b in svc.list_backups(source)),
                     default=T0)
        when = latest + timedelta(seconds=1)
        try:
            svc.record_backup(source, when, BackupType.INCREMENTAL,
                              _content_for(when), "recorder")
            done += 1
        except ValidationError:
            continue  # lost the race for that timestamp
    q.put(done)


def _approver(db, files, request_id, when, q):
    svc = BackupService(db, files)
    res = svc.approve_cleanup(request_id, when, "bob")
    q.put([(i.backup_id, i.status.value) for i in res.items])


def _double_approver(db, files, request_id, when, name, q):
    svc = BackupService(db, files)
    res = svc.approve_cleanup(request_id, when, name)
    q.put([(i.backup_id, i.status.value) for i in res.items])


@pytest.fixture()
def mp_ctx():
    return mp.get_context("spawn")


def test_multiprocess_record_and_approve(tmp_path, mp_ctx):
    db = str(tmp_path / "c.db")
    files = str(tmp_path / "files")
    svc = BackupService(db, files)
    svc.register_source("fin", Sector.FINANCE, 2555)
    svc.set_retention_days("fin", 3000)
    svc.record_backup("fin", t(0), BackupType.FULL,
                      BackupContent(added={"base": b"0"}), "op")
    svc.record_backup("fin", t(1), BackupType.FULL,
                      BackupContent(added={"base": b"1"}), "op")

    now = t(3001) + timedelta(hours=12)  # start = t(1.5): backup 1 expired
    plan = svc.submit_cleanup("fin", now, "alice")
    assert plan.expired_backup_ids == (1,)

    q = mp_ctx.Queue()
    recorders = [mp_ctx.Process(target=_recorder,
                                args=(db, files, "fin", 25, q))
                 for _ in range(2)]
    approver = mp_ctx.Process(target=_approver,
                              args=(db, files, plan.request_id,
                                    now + timedelta(hours=1), q))
    for p in recorders + [approver]:
        p.start()
    for p in recorders + [approver]:
        p.join(60)
        assert p.exitcode == 0, f"worker {p.pid} exited {p.exitcode}"

    results = [q.get(timeout=10) for _ in range(3)]
    approval = next(r for r in results if isinstance(r, list))
    assert approval == [(1, "deleted")]

    # catalog consistency: no surviving backup has a deleted ancestor
    backups = {b.id: b for b in svc.list_backups("fin")}
    for b in backups.values():
        if b.deleted:
            continue
        cur = b.base_id
        while cur is not None:
            assert not backups[cur].deleted, f"backup {b.id} depends on deleted {cur}"
            cur = backups[cur].base_id

    # every recoverable backup restores exactly the replayed rows
    for b in svc.recoverable_ids("fin"):
        chain = []
        cur = backups[b]
        while cur is not None:
            chain.append(cur)
            cur = backups[cur.base_id] if cur.base_id is not None else None
        expected = {}
        for link in reversed(chain):
            if link.id == 1:
                content = BackupContent(added={"base": b"0"})
            elif link.id == 2:
                content = BackupContent(added={"base": b"1"})
            else:
                content = _content_for(link.backup_time)
            for key in content.deleted:
                expected.pop(key, None)
            expected.update(content.added)
            expected.update(content.modified)
        assert svc.restore("fin", b) == expected, f"backup {b}"


def test_concurrent_double_approval_threads(tmp_path):
    db = str(tmp_path / "c.db")
    files = str(tmp_path / "files")
    svc = BackupService(db, files)
    svc.register_source("fin", Sector.FINANCE, 2555)
    svc.set_retention_days("fin", 3000)
    svc.record_backup("fin", t(0), BackupType.FULL,
                      BackupContent(added={"a": b"1"}), "op")
    svc.record_backup("fin", t(1), BackupType.FULL,
                      BackupContent(added={"b": b"2"}), "op")
    now = t(3001) + timedelta(hours=12)
    plan = svc.submit_cleanup("fin", now, "alice")

    barrier = threading.Barrier(2)
    results, errors = [], []

    def approve(name):
        try:
            barrier.wait(10)
            results.append(svc.approve_cleanup(plan.request_id, now, name))
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=approve, args=(n,))
               for n in ("bob", "carol")]
    for th in threads:
        th.start()
    for th in threads:
        th.join(30)
    assert not errors
    assert len(results) == 2
    assert results[0] == results[1]  # executed exactly once
    assert [i.status for i in results[0].items] == [ItemStatus.DELETED]
    assert not (tmp_path / "files" / "1.bak").exists()
    assert (tmp_path / "files" / "2.bak").exists()


def test_concurrent_double_approval_processes(tmp_path, mp_ctx):
    db = str(tmp_path / "c.db")
    files = str(tmp_path / "files")
    svc = BackupService(db, files)
    svc.register_source("fin", Sector.FINANCE, 2555)
    svc.set_retention_days("fin", 3000)
    svc.record_backup("fin", t(0), BackupType.FULL,
                      BackupContent(added={"a": b"1"}), "op")
    svc.record_backup("fin", t(1), BackupType.FULL,
                      BackupContent(added={"b": b"2"}), "op")
    now = t(3001) + timedelta(hours=12)
    plan = svc.submit_cleanup("fin", now, "alice")

    q = mp_ctx.Queue()
    procs = [mp_ctx.Process(target=_double_approver,
                            args=(db, files, plan.request_id, now, n, q))
             for n in ("bob", "carol")]
    for p in procs:
        p.start()
    for p in procs:
        p.join(60)
        assert p.exitcode == 0
    r1, r2 = q.get(timeout=10), q.get(timeout=10)
    assert r1 == r2 == [(1, "deleted")]
    assert not (tmp_path / "files" / "1.bak").exists()
