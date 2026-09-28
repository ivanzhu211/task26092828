"""Exception hierarchy for the backup library."""


class BackupLibError(Exception):
    """Base class for all library errors."""


class ValidationError(BackupLibError):
    """Caller-provided data is invalid."""


class DuplicateBackupError(ValidationError):
    """A backup with the same (source, backup_time) already exists."""


class BaseMismatchError(ValidationError):
    """The declared base does not match the rule for the backup type."""


class BaseCorruptedError(ValidationError):
    """The base backup is missing or marked corrupted."""


class RetentionError(ValidationError):
    """Retention days are below the sector-specific minimum."""


class RestoreError(BackupLibError):
    """The requested backup cannot be restored; item identifies the bad link."""

    def __init__(self, message: str, *, backup_id: int | None = None) -> None:
        super().__init__(message)
        self.backup_id = backup_id


class ApprovalError(BackupLibError):
    """Generic approval/state error."""


class RequestExpiredError(ApprovalError):
    """The cleanup request was not approved within 72 hours."""


class RequestFinalizedError(ApprovalError):
    """The request has already been approved (idempotent re-approval)."""
