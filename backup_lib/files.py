"""Backup payload files: serialization, hashing, deletion."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path

from .models import BackupContent


def serialize(content: BackupContent) -> bytes:
    doc = {
        "added": {k: base64.b64encode(v).decode("ascii")
                  for k, v in sorted(content.added.items())},
        "modified": {k: base64.b64encode(v).decode("ascii")
                     for k, v in sorted(content.modified.items())},
        "deleted": sorted(content.deleted),
    }
    return json.dumps(doc, sort_keys=True).encode("utf-8")


def deserialize(data: bytes) -> BackupContent:
    doc = json.loads(data.decode("utf-8"))
    return BackupContent(
        added={k: base64.b64decode(v) for k, v in doc["added"].items()},
        modified={k: base64.b64decode(v) for k, v in doc["modified"].items()},
        deleted=frozenset(doc["deleted"]),
    )


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_backup_file(storage_dir: Path, backup_id: int,
                      content: BackupContent) -> tuple[Path, str]:
    """Write payload atomically; returns (path, sha256)."""
    data = serialize(content)
    final = storage_dir / f"{backup_id}.bak"
    tmp = storage_dir / f".{backup_id}.tmp"
    with open(tmp, "wb") as fh:
        fh.write(data)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, final)
    return final, sha256_bytes(data)


def read_backup_file(path: Path) -> BackupContent:
    with open(path, "rb") as fh:
        return deserialize(fh.read())


def delete_backup_file(path: Path) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass
