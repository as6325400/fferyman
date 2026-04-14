from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from fferyman.core.policy import Policy, policy_from_dict


_POLICY_KEYS = {"on_conflict", "on_change", "on_delete", "duplicate_dir", "archive_dir"}


@dataclass
class WatchSpec:
    name: str
    algorithm: str
    source: Path
    dest: Path
    params: dict[str, Any] = field(default_factory=dict)
    policy: Policy = field(default_factory=Policy)


@dataclass
class AppConfig:
    database: Path
    plugins_dir: Path | None
    log_level: str
    watches: list[WatchSpec]


def load(config_path: Path) -> AppConfig:
    config_path = Path(config_path)
    with config_path.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    if "watches" not in raw or not isinstance(raw["watches"], list):
        raise ValueError("config must contain a `watches` list")

    # Top-level defaults (optional). Watch-level fields override.
    top_defaults = {k: raw[k] for k in _POLICY_KEYS if k in raw}

    watches: list[WatchSpec] = []
    seen: set[str] = set()
    for i, w in enumerate(raw["watches"]):
        if not isinstance(w, dict):
            raise ValueError(f"watches[{i}] must be a mapping")
        for key in ("name", "algorithm", "source", "dest"):
            if key not in w:
                raise ValueError(f"watches[{i}] missing `{key}`")
        name = str(w["name"])
        if name in seen:
            raise ValueError(f"duplicate watch name {name!r}")
        seen.add(name)

        merged = {**top_defaults, **{k: w[k] for k in _POLICY_KEYS if k in w}}
        try:
            watch_policy = policy_from_dict(merged)
        except ValueError as e:
            raise ValueError(f"watches[{i}] ({name}): {e}") from None

        watches.append(
            WatchSpec(
                name=name,
                algorithm=str(w["algorithm"]),
                source=Path(w["source"]).expanduser(),
                dest=Path(w["dest"]).expanduser(),
                params=dict(w.get("params") or {}),
                policy=watch_policy,
            )
        )

    db_path = raw.get("database", "./fferyman.sqlite")
    plugins_dir = raw.get("plugins_dir")
    return AppConfig(
        database=Path(str(db_path)).expanduser(),
        plugins_dir=Path(str(plugins_dir)).expanduser() if plugins_dir else None,
        log_level=str(raw.get("log_level", "INFO")),
        watches=watches,
    )
