"""Randomized cross-check of plans, approvals and restores against the
independent oracle."""

import random
from datetime import timedelta

from backup_lib import BackupContent, BackupType, ItemStatus
from conftest import Harness, t


def _rand_content(rng, day, btype):
    if btype is BackupType.FULL:
        return BackupContent(added={f"snap{day}": str(day).encode()})
    kind = rng.random()
    if kind < 0.5:
        return BackupContent(added={f"k{day}": str(day).encode()})
    if kind < 0.8:
        return BackupContent(modified={f"k{rng.randint(0, day)}": b"x"})
    return BackupContent(deleted=frozenset({f"k{rng.randint(0, day)}"}))


def test_randomized_plan_and_approval_match_oracle(fin):
    rng = random.Random(20260928)
    h = fin
    h.svc.set_retention_days("fin", 3000)
    day = 0
    h.record(t(day), BackupType.FULL, BackupContent(added={"k0": b"0"}))
    for _ in range(60):
        day += rng.randint(1, 30)
        btype = rng.choice([BackupType.FULL, BackupType.INCREMENTAL,
                            BackupType.INCREMENTAL, BackupType.DIFFERENTIAL])
        h.record(t(day), btype, _rand_content(rng, day, btype))
        if rng.random() < 0.15:
            victim = rng.choice(list(h.oracle.backups))
            if not h.oracle.backups[victim]["corrupted"]:
                h.corrupt_file(victim)

    # plan must match the oracle exactly at submission time
    submit_at = t(day + 3000) + timedelta(hours=rng.randint(0, 23))
    plan = h.svc.submit_cleanup("fin", submit_at, "alice")
    assert list(plan.expired_backup_ids) == h.oracle.expired(submit_at, 3000)

    # mutate state between submit and approve
    day += rng.randint(1, 10)
    h.record(t(day), BackupType.INCREMENTAL,
             _rand_content(rng, day, BackupType.INCREMENTAL))
    survivors = [bid for bid, b in h.oracle.backups.items()
                 if not b["corrupted"]]
    if survivors and rng.random() < 0.7:
        h.corrupt_file(rng.choice(survivors))
    if rng.random() < 0.5:
        h.svc.set_retention_days("fin", 3001)

    days = 3001 if rng.random() < 0.5 else 3000
    h.svc.set_retention_days("fin", days)
    approve_at = submit_at + timedelta(hours=rng.randint(1, 71))
    expected_expired = set(h.oracle.expired(approve_at, days))
    result = h.svc.approve_cleanup(plan.request_id, approve_at, "bob")

    for item in result.items:
        if item.backup_id in expected_expired:
            assert item.status is ItemStatus.DELETED, item
            h.oracle.backups[item.backup_id]["deleted"] = True
        else:
            assert item.status is ItemStatus.STILL_REQUIRED, item
    # nothing outside the plan was deleted
    deleted = {b.id for b in h.svc.list_backups("fin") if b.deleted}
    assert deleted <= set(plan.expired_backup_ids)
    h.check_survivors()


def test_repeated_cycles_eventually_expire_everything_old(fin):
    rng = random.Random(7)
    h = fin
    h.svc.set_retention_days("fin", 2555)
    day = 0
    h.record(t(day), BackupType.FULL, BackupContent(added={"k0": b"0"}))
    for cycle in range(6):
        for _ in range(10):
            day += rng.randint(1, 20)
            btype = rng.choice([BackupType.FULL, BackupType.INCREMENTAL,
                                BackupType.DIFFERENTIAL])
            h.record(t(day), btype, _rand_content(rng, day, btype))
        now = t(day + 2555)
        plan = h.svc.submit_cleanup("fin", now, "alice")
        assert list(plan.expired_backup_ids) == h.oracle.expired(now, 2555)
        result = h.svc.approve_cleanup(plan.request_id, now, "bob")
        for item in result.items:
            assert item.status is ItemStatus.DELETED
            h.oracle.backups[item.backup_id]["deleted"] = True
        h.check_survivors()
