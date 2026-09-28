# backup_lib

备份目录与过期清理库（无接口层，供上层备份调度服务直接调用）。

- 备份内容：本地文件（JSON + Base64，原子写入，文件名 `<backup_id>.bak`），写入时记录 SHA-256。
- 目录信息：单个 SQLite 库（WAL 模式），表 `sources` / `backups` / `cleanup_requests` / `cleanup_items`。
- 时间：全部由调用方传入且必须带时区，模块不读系统时钟；身份校验由调用方完成，只传操作人标识。

## 模块结构

- `backup_lib/models.py`：公开数据类型（`BackupType`、`Sector`、`BackupContent`、`CleanupPlan`、`ApprovalResult` 等）。
- `backup_lib/errors.py`：异常类型；恢复失败用 `RestoreError.backup_id` 指出链上的问题备份。
- `backup_lib/store.py`：SQLite 建表与连接（WAL、`busy_timeout`、外键）。
- `backup_lib/files.py`：备份文件序列化、SHA-256、原子写、删除。
- `backup_lib/chain.py`：纯内存、无副作用的依赖链/可恢复/保留集算法，便于与独立实现对照。
- `backup_lib/service.py`：`BackupService`，唯一入口。

## 主要 API

```python
from datetime import datetime, timezone
from backup_lib import BackupService, BackupType, Sector, BackupContent

svc = BackupService("/data/catalog.db", "/data/files")
svc.register_source("src-1", Sector.FINANCE, retention_days=2555)  # 医疗下限 2190
svc.set_retention_days("src-1", 3000)

rec = svc.record_backup("src-1", datetime(2026, 9, 28, tzinfo=timezone.utc),
                        BackupType.FULL,
                        BackupContent(added={"row-key": b"row-value"}), "alice")
rows = svc.restore("src-1", rec.id)
svc.verify("src-1")                       # 重算现存备份文件，缺文件/不符即标记损坏（不可撤销）

plan = svc.submit_cleanup("src-1", now, "alice")     # 一人提交，恰好列出全部过期备份
result = svc.approve_cleanup(plan.request_id, now, "bob")  # 另一人批准；72h 后失效
```

`ApprovalResult.items` 逐项给出 `deleted` / `already_missing` / `still_required` 及原因；
对同一申请重复批准直接返回首次结果，不重复执行。

## 规则要点

- 每个数据源备份时间严格递增；第一个必须是全量。增量基准=当前最近现存备份；差异基准=当前最近现存全量。基准不符或已损坏一律拒绝。
- 恢复沿依赖链应用：全量快照 → 增量/差异的 deleted/added/modified。链上缺失、损坏、文件或 SHA-256 不符即失败，指出具体备份；校验不符顺手标记损坏。
- 保留集 = `{时间 >= 保留起点的全部现存备份（含损坏）}` ∪ `{时间 <= 起点的可恢复备份中最近的一个}` 以及它们依赖链上的全部备份；其余为过期。起点时间恰有备份时，该备份同时充当两侧锚点。
- 批准时按批准时刻重新判定，只删“计划内且此刻仍过期”的备份，计划外绝不删除。

## 并发与性能

- 所有写操作（含恢复）在单个 `BEGIN IMMEDIATE` 事务内完成，SQLite 串行化跨进程/跨线程写者；恢复持写锁，保证不会读到“目录已删、文件正在删”的中间状态，恢复要么完整正确要么明确失败。
- 6 万备份（约七年每小时一次）生成清理计划实测约 0.2s（要求 ≤ 3s）；记录只查询最近一条/最近全量，耗时与历史长度无关。

## 测试

```
python3 -m pytest tests/ -q
```

- `test_unit.py`：记录/恢复/校验/保留下限规则。
- `test_scenarios.py`：起点前最近备份损坏需回退、增量链跨起点、差异前中间增量可清理、提交后出现损坏/调长保留/新备份、重复计划、双人批准与 72h 失效。
- `test_concurrency.py`：多进程记录增量与批准同时进行、双人（线程与进程）同时批准同一申请。
- `test_oracle.py`：随机操作序列下，计划与删除结果和独立 oracle 完全一致；每个幸存可恢复备份恢复结果与该时刻应有行集合完全一致。
- `test_perf.py`：6 万备份计划 < 3s、记录耗时不随历史增长。
