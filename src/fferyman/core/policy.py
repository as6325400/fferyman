from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from pathlib import PurePosixPath, PureWindowsPath


class OnConflict(str, Enum):
    """What to do when a target path is taken by a *different* source."""
    OVERWRITE = "overwrite"
    DUPLICATE = "duplicate"
    ERROR = "error"


class OnChange(str, Enum):
    """What to do when the *same* source has new content."""
    REPLACE = "replace"   # new content takes prev.dest_path; old file overwritten
    VERSION = "version"   # prev is kept; new goes to duplicate_dir/name_N


class OnDelete(str, Enum):
    """What to do when a tracked source disappears."""
    DELETE_DEST = "delete_dest"
    KEEP_DEST = "keep_dest"
    ARCHIVE = "archive"


class HashPolicy(str, Enum):
    """When to compute content hashes."""
    ALWAYS = "always"
    METADATA_FAST_PATH = "metadata_fast_path"
    COPY_THEN_HASH = "copy_then_hash"


@dataclass(frozen=True)
class Policy:
    on_conflict: OnConflict = OnConflict.DUPLICATE
    on_change: OnChange = OnChange.VERSION
    on_delete: OnDelete = OnDelete.KEEP_DEST
    hash_policy: HashPolicy = HashPolicy.METADATA_FAST_PATH
    duplicate_dir: str = "duplicate"
    archive_dir: str = "archive"

    def __post_init__(self) -> None:
        # Defense in depth: even direct Policy(...) calls must produce a
        # subdir name that can't escape `dest`.
        _validate_subdir(self.duplicate_dir, "duplicate_dir")
        _validate_subdir(self.archive_dir, "archive_dir")

    def fingerprint(self) -> str:
        """Stable string representing this policy configuration.

        Included in DB mappings so that a policy change invalidates prior
        rows and forces re-processing on the next scan/reconcile.
        """
        return (
            f"oc={self.on_conflict.value}"
            f"|oh={self.on_change.value}"
            f"|od={self.on_delete.value}"
            f"|hp={self.hash_policy.value}"
            f"|dup={self.duplicate_dir}"
            f"|arc={self.archive_dir}"
        )


_ON_CONFLICT_VALUES = {e.value for e in OnConflict}
_ON_CHANGE_VALUES = {e.value for e in OnChange}
_ON_DELETE_VALUES = {e.value for e in OnDelete}
_HASH_POLICY_VALUES = {e.value for e in HashPolicy}


def _validate_subdir(name: str, field: str) -> str:
    """Reject anything that could escape `dest` when used as a subdirectory:
    empty, absolute, contains separators or `..` segments.
    """
    if not isinstance(name, str) or not name:
        raise ValueError(f"{field} must be a non-empty string")
    if os.sep in name or (os.altsep and os.altsep in name) or "/" in name or "\\" in name:
        raise ValueError(
            f"{field}={name!r} must be a single directory component (no path separators)"
        )
    if PurePosixPath(name).is_absolute() or PureWindowsPath(name).is_absolute():
        raise ValueError(f"{field}={name!r} cannot be absolute")
    if name in (".", "..") or ".." in PurePosixPath(name).parts:
        raise ValueError(f"{field}={name!r} cannot contain '..'")
    return name


def policy_from_dict(raw: dict) -> Policy:
    def _parse(key: str, enum_cls, valid_values, default):
        v = raw.get(key)
        if v is None:
            return default
        v = str(v)
        if v not in valid_values:
            raise ValueError(
                f"{key}={v!r} is not one of {sorted(valid_values)}"
            )
        return enum_cls(v)

    return Policy(
        on_conflict=_parse("on_conflict", OnConflict, _ON_CONFLICT_VALUES, OnConflict.DUPLICATE),
        on_change=_parse("on_change", OnChange, _ON_CHANGE_VALUES, OnChange.VERSION),
        on_delete=_parse("on_delete", OnDelete, _ON_DELETE_VALUES, OnDelete.KEEP_DEST),
        hash_policy=_parse(
            "hash_policy",
            HashPolicy,
            _HASH_POLICY_VALUES,
            HashPolicy.METADATA_FAST_PATH,
        ),
        duplicate_dir=_validate_subdir(str(raw.get("duplicate_dir", "duplicate")), "duplicate_dir"),
        archive_dir=_validate_subdir(str(raw.get("archive_dir", "archive")), "archive_dir"),
    )
