"""Performance: 60k backups (hourly, ~7 years) on one source."""

import time
from datetime import timedelta

from backup_lib import BackupContent, BackupType
from conftest import T0, t


def _bulk_load(svc, source_id, count):
    times = [(T0 + timedelta(hours=i + 2)).isoformat()
             for i in range(count)]
    with svc._session() as conn:
        conn.execute("BEGIN IMMEDIATE")
        batch = []
        for i, ts in enumerate(times):
            bid = i + 1
            btype = "full" if i % 168 == 0 else "incremental"  # weekly full
            base = None if btype == "full" else bid - 1
            batch.append((source_id, ts, btype, base, f"{bid}.bak",
                          "x" * 64))
        conn.executemany(
            "INSERT INTO backups(source_id, backup_time, backup_type, base_id,"
            " file_path, sha256, corrupted, deleted)"
            " VALUES (?,?,?,?,?,?,0,0)", batch)
        conn.execute("COMMIT")


def test_plan_generation_under_3s_at_60k(tmp_path):
    from backup_lib import BackupService, Sector
    db = str(tmp_path / "p.db")
    files = str(tmp_path / "files")
    svc = BackupService(db, files)
    svc.register_source("s", Sector.OTHER, 2555)
    with svc._session() as conn:
        source_id = conn.execute(
            "SELECT id FROM sources WHERE name='s'").fetchone()["id"]
    _bulk_load(svc, source_id, 60000)

    # retention start lands mid-history so the chain crosses it
    now = T0 + timedelta(hours=60000 + 24 * 2555 - 1000)
    start = time.perf_counter()
    plan = svc.submit_cleanup("s", now, "alice")
    elapsed = time.perf_counter() - start
    assert elapsed < 3.0, f"plan took {elapsed:.2f}s"
    assert len(plan.expired_backup_ids) > 0
    # expired ids stay below the start region (sanity shape)
    assert plan.expired_backup_ids[0] < plan.expired_backup_ids[-1]


def test_record_latency_does_not_grow_with_history(tmp_path):
    from backup_lib import BackupService, Sector
    db = str(tmp_path / "p.db")
    files = str(tmp_path / "files")
    svc = BackupService(db, files)
    svc.register_source("s", Sector.OTHER, 2555)

    def record_one(when):
        return svc.record_backup(
            "s", when, BackupType.INCREMENTAL,
            BackupContent(added={"k": str(when).encode()}), "op")

    svc.record_backup("s", T0, BackupType.FULL,
                      BackupContent(added={"k": b"0"}), "op")
    start = time.perf_counter()
    record_one(T0 + timedelta(hours=1))
    early = time.perf_counter() - start

    with svc._session() as conn:
        source_id = conn.execute(
            "SELECT id FROM sources WHERE name='s'").fetchone()["id"]
    _bulk_load(svc, source_id, 60000)

    start = time.perf_counter()
    record_one(T0 + timedelta(hours=60002))
    late = time.perf_counter() - start
    assert late < max(0.5, early * 5 + 0.2), f"early={early:.4f} late={late:.4f}"
