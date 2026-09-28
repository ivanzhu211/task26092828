"""Public data types."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import datetime


class BackupType(str, enum.Enum):
    FULL = "full"
    INCREMENTAL = "incremental"
    DIFFERENTIAL = "differential"


class Sector(str, enum.Enum):
    FINANCE = "finance"
    HEALTHCARE = "healthcare"
    OTHER = "other"


SECTOR_MIN_RETENTION_DAYS = {
    Sector.FINANCE: 2555,
    Sector.HEALTHCARE: 2190,
}


@dataclass(frozen=True)
class BackupContent:
    """Payload for a new backup.

    Only one of the three change groups should carry data for
    incremental/differential backups; full backups use ``added``.
    """

    added: dict[str, bytes] = field(default_factory=dict)
    modified: dict[str, bytes] = field(default_factory=dict)
    deleted: frozenset[str] = frozenset()


@dataclass(frozen=True)
class BackupRecord:
    id: int
    source: str
    backup_time: datetime
    backup_type: BackupType
    base_id: int | None
    sha256: str
    corrupted: bool
    deleted: bool


@dataclass(frozen=True)
class CleanupPlan:
    request_id: int
    source: str
    submitted_at: datetime
    submitted_by: str
    expired_backup_ids: tuple[int, ...]


class ItemStatus(str, enum.Enum):
    DELETED = "deleted"
    STILL_REQUIRED = "still_required"
    ALREADY_MISSING = "already_missing"
    EXPIRED = "expired"


@dataclass(frozen=True)
class ItemResult:
    backup_id: int
    status: ItemStatus
    reason: str = ""


@dataclass(frozen=True)
class ApprovalResult:
    request_id: int
    approved_at: datetime
    approved_by: str
    items: tuple[ItemResult, ...]


@dataclass(frozen=True)
class VerificationResult:
    checked: int
    marked_corrupted: tuple[int, ...]
