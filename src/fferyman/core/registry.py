from __future__ import annotations

import importlib.util
import logging
import sys
from importlib import metadata
from pathlib import Path

from fferyman.core.mapper import MapperSpec, get_spec

log = logging.getLogger("fferyman.registry")


class Registry:
    def __init__(self) -> None:
        self._specs: dict[str, MapperSpec] = {}

    def register(self, spec: MapperSpec) -> None:
        existing = self._specs.get(spec.name)
        if existing is not None and existing.fn is not spec.fn:
            raise ValueError(f"algorithm name collision: {spec.name}")
        self._specs[spec.name] = spec

    def get(self, name: str) -> MapperSpec:
        if name not in self._specs:
            raise KeyError(
                f"algorithm {name!r} not found. Known: {sorted(self._specs)}"
            )
        return self._specs[name]

    def names(self) -> list[str]:
        return sorted(self._specs)

    def load_from_directory(self, plugins_dir: Path) -> None:
        plugins_dir = Path(plugins_dir).resolve()
        if not plugins_dir.is_dir():
            log.warning("plugins_dir %s does not exist, skipping", plugins_dir)
            return
        for py in sorted(plugins_dir.glob("*.py")):
            if py.name.startswith("_"):
                continue
            self._load_module_from_file(py)

    def _load_module_from_file(self, py: Path) -> None:
        mod_name = f"fferyman_plugins.{py.stem}"
        spec = importlib.util.spec_from_file_location(mod_name, py)
        if spec is None or spec.loader is None:
            log.warning("cannot load spec for %s", py)
            return
        module = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = module
        try:
            spec.loader.exec_module(module)
        except Exception:
            log.exception("failed loading plugin %s", py)
            return
        for attr in dir(module):
            obj = getattr(module, attr)
            s = get_spec(obj)
            if s is not None:
                self.register(s)
                log.info("registered algorithm %s from %s", s.name, py.name)

    def load_from_entry_points(self) -> None:
        try:
            eps = metadata.entry_points(group="fferyman.algorithms")
        except Exception:
            log.exception("failed reading entry points")
            return
        for ep in eps:
            try:
                obj = ep.load()
            except Exception:
                log.exception("failed loading entry point %s", ep.name)
                continue
            s = get_spec(obj)
            if s is not None:
                self.register(s)
                log.info("registered algorithm %s from entry point", s.name)
