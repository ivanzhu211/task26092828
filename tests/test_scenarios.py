"""Acceptance scenarios for retention planning and two-person cleanup."""

from datetime import timedelta

import pytest

from backup_lib import BackupContent, BackupType, ItemStatus, Sector
from backup_lib.errors import ApprovalError, RequestExpiredError
from conftest import Harness, t

C = BackupContent


def _approve_and_check(h, plan, now, days):
    """Approve and cross-check every item against the oracle."""
    expected_expired = set(h.oracle.expired(now, days))
    result = h.svc.approve_cleanup(plan.request_id, now, "bob")
    assert set(plan.expired_backup_ids) == {i.backup_id for i in result.items}
    for item in result.items:
        if item.backup_id in expected_expired:
            assert item.status is ItemStatus.DELETED, item
            h.oracle.backups[item.backup_id]["deleted"] = True
        else:
            assert item.status is ItemStatus.STILL_REQUIRED, item
    return result


def test_corrupted_latest_before_start_falls_back(fin):
    """The latest backup at/before the retention start is corrupted, so the
    anchor must fall back to an earlier recoverable one."""
    h = fin
    h.svc.set_retention_days("fin", 3000)
    h.record(t(0), BackupType.FULL, C(added={"a": b"1"}))
    h.record(t(1), BackupType.INCREMENTAL, C(added={"b": b"2"}))
    r3 = h.record(t(2), BackupType.INCREMENTAL, C(added={"c": b"3"}))
    h.record(t(3), BackupType.INCREMENTAL, C(added={"d": b"4"}))
    h.record(t(4), BackupType.FULL, C(added={"z": b"9"}))
    h.record(t(5), BackupType.INCREMENTAL, C(added={"y": b"8"}))
    h.corrupt_file(r3.id)  # breaks the t(2)/t(3) incrementals

    now = t(3004) + timedelta(hours=12)  # start = t(4.5)
    plan = h.svc.submit_cleanup("fin", now, "alice")
    assert list(plan.expired_backup_ids) == h.oracle.expired(now, 3000)
    # anchor before start must be the full at t(4), not the corrupt chain
    keep = h.oracle.keep_set(now, 3000)
    assert r3.id not in keep
    assert set(plan.expired_backup_ids) == {1, 2, 3, 4}
    _approve_and_check(h, plan, now, 3000)
    h.check_survivors()


def test_incremental_chain_crossing_start_is_kept(fin):
    """A pure incremental chain crossing the retention start keeps its
    pre-start links: nothing on the chain may expire."""
    h = fin
    h.svc.set_retention_days("fin", 3000)
    ids = [h.record(t(i), BackupType.FULL if i == 0 else BackupType.INCREMENTAL,
                    C(added={f"k{i}": str(i).encode()})).id
           for i in range(6)]
    now = t(3003) + timedelta(hours=12)  # start = t(3.5), chain crosses it
    plan = h.svc.submit_cleanup("fin", now, "alice")
    assert list(plan.expired_backup_ids) == h.oracle.expired(now, 3000) == []
    result = h.svc.approve_cleanup(plan.request_id, now, "bob")
    assert result.items == ()
    h.check_survivors()


def test_intermediate_incrementals_after_differential_expire(fin):
    """Incrementals superseded by a differential (based on the same full)
    are depended on by nothing kept and must be cleaned up."""
    h = fin
    h.svc.set_retention_days("fin", 3000)
    h.record(t(0), BackupType.FULL, C(added={"a": b"1"}))
    r2 = h.record(t(1), BackupType.INCREMENTAL, C(added={"b": b"2"}))
    r3 = h.record(t(2), BackupType.INCREMENTAL, C(added={"c": b"3"}))
    r4 = h.record(t(3), BackupType.DIFFERENTIAL, C(added={"d": b"4"}))
    r5 = h.record(t(4), BackupType.INCREMENTAL, C(added={"e": b"5"}))
    h.record(t(5), BackupType.FULL, C(added={"z": b"9"}))

    now = t(3004) + timedelta(hours=12)  # start = t(4.5)
    plan = h.svc.submit_cleanup("fin", now, "alice")
    assert list(plan.expired_backup_ids) == h.oracle.expired(now, 3000)
    assert set(plan.expired_backup_ids) == {r2.id, r3.id}
    assert r4.base_id == 1 and r5.base_id == r4.id  # chain skips r2, r3
    _approve_and_check(h, plan, now, 3000)
    h.check_survivors()


def test_new_corruption_between_submit_and_approve(fin):
    """A backup corrupted after submission becomes required (its chain is
    needed by the fall-back anchor), so approval must skip it."""
    h = fin
    h.svc.set_retention_days("fin", 3000)
    h.record(t(0), BackupType.FULL, C(added={"a": b"1"}))
    r2 = h.record(t(1), BackupType.INCREMENTAL, C(added={"b": b"2"}))
    r3 = h.record(t(2), BackupType.FULL, C(added={"z": b"9"}))
    h.record(t(3), BackupType.INCREMENTAL, C(added={"y": b"8"}))

    submit_at = t(3002) + timedelta(hours=12)  # start = t(2.5)
    plan = h.svc.submit_cleanup("fin", submit_at, "alice")
    assert set(plan.expired_backup_ids) == {1, r2.id}

    h.corrupt_file(r3.id)  # full at t(2) now corrupted -> anchor falls back
    approve_at = submit_at + timedelta(hours=1)
    expected = set(h.oracle.expired(approve_at, 3000))
    assert expected == set()  # everything is required again
    result = h.svc.approve_cleanup(plan.request_id, approve_at, "bob")
    assert all(i.status is ItemStatus.STILL_REQUIRED for i in result.items)
    assert all("still required" in i.reason for i in result.items)
    h.check_survivors()


def test_retention_extended_between_submit_and_approve(fin):
    """Lengthening the retention window after submission moves the start
    earlier; planned items inside the new window must be kept."""
    h = fin
    h.svc.set_retention_days("fin", 3000)
    h.record(t(0), BackupType.FULL, C(added={"a": b"1"}))
    h.record(t(1), BackupType.FULL, C(added={"b": b"2"}))
    h.record(t(2), BackupType.INCREMENTAL, C(added={"c": b"3"}))

    submit_at = t(3002) + timedelta(hours=12)  # start = t(2.5)
    plan = h.svc.submit_cleanup("fin", submit_at, "alice")
    assert set(plan.expired_backup_ids) == {1}  # F2 kept as I1's base

    h.svc.set_retention_days("fin", 3002)  # start moves to t(0.5)
    approve_at = submit_at + timedelta(hours=2)
    expected = set(h.oracle.expired(approve_at, 3002))
    assert expected == set()  # F1 now inside the window
    result = h.svc.approve_cleanup(plan.request_id, approve_at, "bob")
    assert all(i.status is ItemStatus.STILL_REQUIRED for i in result.items)
    h.check_survivors()


def test_new_backup_between_submit_and_approve(fin):
    """Backups recorded after submission are outside the plan and must
    survive approval untouched."""
    h = fin
    h.svc.set_retention_days("fin", 3000)
    h.record(t(0), BackupType.FULL, C(added={"a": b"1"}))
    h.record(t(1), BackupType.FULL, C(added={"b": b"2"}))

    submit_at = t(3001) + timedelta(hours=12)  # start = t(1.5)
    plan = h.svc.submit_cleanup("fin", submit_at, "alice")
    assert set(plan.expired_backup_ids) == {1}

    r3 = h.record(t(2), BackupType.INCREMENTAL, C(added={"c": b"3"}))
    approve_at = submit_at + timedelta(hours=3)
    result = h.svc.approve_cleanup(plan.request_id, approve_at, "bob")
    assert [i.backup_id for i in result.items] == [1]
    assert result.items[0].status is ItemStatus.DELETED
    h.oracle.backups[1]["deleted"] = True
    assert r3.id not in plan.expired_backup_ids
    h.check_survivors()


def test_already_missing_item_reported(fin, svc):
    """An item deleted by another approved request is reported as missing."""
    h = fin
    h.svc.set_retention_days("fin", 3000)
    h.record(t(0), BackupType.FULL, C(added={"a": b"1"}))
    h.record(t(1), BackupType.FULL, C(added={"b": b"2"}))
    now = t(3001) + timedelta(hours=12)
    p1 = h.svc.submit_cleanup("fin", now, "alice")
    p2 = h.svc.submit_cleanup("fin", now, "alice")
    assert p1.expired_backup_ids == p2.expired_backup_ids == (1,)
    r1 = h.svc.approve_cleanup(p1.request_id, now, "bob")
    assert r1.items[0].status is ItemStatus.DELETED
    h.oracle.backups[1]["deleted"] = True
    r2 = h.svc.approve_cleanup(p2.request_id, now, "carol")
    assert r2.items[0].status is ItemStatus.ALREADY_MISSING
    h.check_survivors()


def test_approval_guards(fin):
    h = fin
    h.svc.set_retention_days("fin", 3000)
    h.record(t(0), BackupType.FULL, C(added={"a": b"1"}))
    h.record(t(1), BackupType.FULL, C(added={"b": b"2"}))
    now = t(3001) + timedelta(hours=12)
    plan = h.svc.submit_cleanup("fin", now, "alice")

    with pytest.raises(ApprovalError):
        h.svc.approve_cleanup(plan.request_id, now, "alice")  # self-approval
    with pytest.raises(RequestExpiredError):
        h.svc.approve_cleanup(plan.request_id, now + timedelta(hours=72), "bob")
    # 71h59m still valid
    result = h.svc.approve_cleanup(plan.request_id,
                                   now + timedelta(hours=71, minutes=59), "bob")
    # idempotent: a different approver re-approving gets the stored result
    again = h.svc.approve_cleanup(plan.request_id,
                                  now + timedelta(hours=100), "carol")
    assert again == result
    h.oracle.backups[1]["deleted"] = True
    h.check_survivors()
