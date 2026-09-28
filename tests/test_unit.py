"""Recording rules, restore semantics, verification."""

from datetime import timedelta

import pytest

from backup_lib import BackupContent, BackupType, Sector
from backup_lib.errors import (
    ApprovalError,
    BaseCorruptedError,
    BaseMismatchError,
    DuplicateBackupError,
    RequestExpiredError,
    RestoreError,
    ValidationError,
)
from conftest import Harness, t


def test_first_backup_must_be_full(fin):
    with pytest.raises(BaseMismatchError):
        fin.record(t(0), BackupType.INCREMENTAL, BackupContent(added={"a": b"1"}))
    with pytest.raises(BaseMismatchError):
        fin.record(t(0), BackupType.DIFFERENTIAL, BackupContent(added={"a": b"1"}))


def test_time_must_strictly_increase(fin):
    fin.record(t(0), BackupType.FULL, BackupContent(added={"a": b"1"}))
    with pytest.raises(ValidationError):
        fin.record(t(0), BackupType.INCREMENTAL, BackupContent())
    with pytest.raises(ValidationError):
        fin.record(t(-1), BackupType.INCREMENTAL, BackupContent())


def test_naive_datetime_rejected(fin):
    from datetime import datetime
    with pytest.raises(ValidationError):
        fin.svc.record_backup("fin", datetime(2020, 1, 1), BackupType.FULL,
                              BackupContent(), "op")


def test_incremental_base_is_latest_existing(fin):
    r1 = fin.record(t(0), BackupType.FULL, BackupContent(added={"a": b"1"}))
    r2 = fin.record(t(1), BackupType.INCREMENTAL, BackupContent(added={"b": b"2"}))
    assert r2.base_id == r1.id
    r3 = fin.record(t(2), BackupType.INCREMENTAL, BackupContent(added={"c": b"3"}))
    assert r3.base_id == r2.id


def test_differential_base_is_latest_full(fin):
    r1 = fin.record(t(0), BackupType.FULL, BackupContent(added={"a": b"1"}))
    fin.record(t(1), BackupType.INCREMENTAL, BackupContent(added={"b": b"2"}))
    r3 = fin.record(t(2), BackupType.DIFFERENTIAL, BackupContent(added={"c": b"3"}))
    assert r3.base_id == r1.id
    r4 = fin.record(t(3), BackupType.FULL, BackupContent(added={"z": b"9"}))
    r5 = fin.record(t(4), BackupType.DIFFERENTIAL, BackupContent(added={"y": b"8"}))
    assert r5.base_id == r4.id


def test_corrupted_base_rejected(fin):
    r1 = fin.record(t(0), BackupType.FULL, BackupContent(added={"a": b"1"}))
    fin.corrupt_file(r1.id)
    with pytest.raises(BaseCorruptedError):
        fin.record(t(1), BackupType.INCREMENTAL, BackupContent(added={"b": b"2"}))
    with pytest.raises(BaseCorruptedError):
        fin.record(t(1), BackupType.DIFFERENTIAL, BackupContent(added={"b": b"2"}))


def test_retention_floor(svc):
    svc.register_source("f", Sector.FINANCE, 2555)
    svc.register_source("h", Sector.HEALTHCARE, 2190)
    svc.register_source("o", Sector.OTHER, 3)
    with pytest.raises(ValidationError):
        svc.set_retention_days("f", 2554)
    with pytest.raises(ValidationError):
        svc.set_retention_days("h", 2189)
    svc.set_retention_days("f", 3000)
    svc.set_retention_days("o", 0)
    with pytest.raises(ValidationError):
        svc.register_source("f2", Sector.FINANCE, 100)


def test_restore_full_rowset(fin):
    r1 = fin.record(t(0), BackupType.FULL,
                    BackupContent(added={"a": b"1", "b": b"2", "c": b"3"}))
    r2 = fin.record(t(1), BackupType.INCREMENTAL,
                    BackupContent(modified={"a": b"9"}, deleted=frozenset({"b"})))
    r3 = fin.record(t(2), BackupType.INCREMENTAL,
                    BackupContent(added={"d": b"4"}, modified={"c": b"33"}))
    assert fin.svc.restore("fin", r1.id) == {"a": b"1", "b": b"2", "c": b"3"}
    assert fin.svc.restore("fin", r2.id) == {"a": b"9", "c": b"3"}
    assert fin.svc.restore("fin", r3.id) == {"a": b"9", "c": b"33", "d": b"4"}


def test_restore_mismatch_marks_corrupted_and_identifies(fin):
    r1 = fin.record(t(0), BackupType.FULL, BackupContent(added={"a": b"1"}))
    r2 = fin.record(t(1), BackupType.INCREMENTAL, BackupContent(added={"b": b"2"}))
    path = fin.svc._store.storage_dir / f"{r1.id}.bak"
    path.write_bytes(b"tampered")
    with pytest.raises(RestoreError) as exc:
        fin.svc.restore("fin", r2.id)
    assert exc.value.backup_id == r1.id
    # marked corrupted, irrevocably
    assert fin.svc.recoverable_ids("fin") == set()
    fin.svc.verify("fin")
    assert fin.svc.recoverable_ids("fin") == set()


def test_restore_missing_chain_member(fin):
    r1 = fin.record(t(0), BackupType.FULL, BackupContent(added={"a": b"1"}))
    with pytest.raises(RestoreError) as exc:
        fin.svc.restore("fin", 9999)
    assert exc.value.backup_id == 9999


def test_verify_marks_missing_file(fin):
    r1 = fin.record(t(0), BackupType.FULL, BackupContent(added={"a": b"1"}))
    r2 = fin.record(t(1), BackupType.INCREMENTAL, BackupContent(added={"b": b"2"}))
    fin.delete_file(r1.id)
    assert fin.svc.recoverable_ids("fin") == set()
    res = fin.svc.verify("fin")
    assert res.marked_corrupted == ()  # already marked, not re-checked


def test_overlapping_change_groups_rejected(fin):
    with pytest.raises(ValidationError):
        fin.record(t(0), BackupType.FULL,
                   BackupContent(added={"a": b"1"}, modified={"a": b"2"}))


def test_unknown_source_rejected(svc):
    with pytest.raises(ValidationError):
        svc.record_backup("nope", t(0), BackupType.FULL, BackupContent(), "op")
    with pytest.raises(ValidationError):
        svc.submit_cleanup("nope", t(0), "op")
