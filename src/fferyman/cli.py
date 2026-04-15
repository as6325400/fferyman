from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Callable, Iterable

from fferyman.config import AppConfig, load
from fferyman.core.db import Database
from fferyman.core.engine import Engine, Watch
from fferyman.core.registry import Registry


def _progress_factory(enabled: bool) -> Callable[[str], Callable[[list[Path]], Iterable[Path]] | None] | None:
    """Return a factory that builds a tqdm-backed wrap for a given watch name.

    Falls back to a no-op (streaming, no bar) when `enabled=False` or tqdm is
    not installed.
    """
    if not enabled:
        return None
    try:
        from tqdm import tqdm
    except ImportError:
        print(
            "[warn] --progress requested but `tqdm` is not installed; "
            "install with `pip install fferyman[progress]`. Continuing "
            "without a progress bar.",
            file=sys.stderr,
        )
        return None

    def factory(watch_name: str) -> Callable[[list[Path]], Iterable[Path]]:
        def wrap(units: list[Path]) -> Iterable[Path]:
            return tqdm(units, desc=watch_name, unit="unit")
        return wrap

    return factory


def _setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


def _build_registry(cfg: AppConfig) -> Registry:
    reg = Registry()
    if cfg.plugins_dir:
        reg.load_from_directory(cfg.plugins_dir)
    reg.load_from_entry_points()
    return reg


def _build_watches(cfg: AppConfig, reg: Registry, db: Database) -> list[Watch]:
    watches: list[Watch] = []
    for w in cfg.watches:
        spec = reg.get(w.algorithm)
        w.dest.mkdir(parents=True, exist_ok=True)
        if not w.source.is_dir():
            raise FileNotFoundError(f"source not found: {w.source}")
        store = db.scope(w.name, spec.name)
        watches.append(
            Watch(
                name=w.name,
                spec=spec,
                source=w.source,
                dest=w.dest,
                params=dict(w.params),
                store=store,
                policy=w.policy,
                debounce_seconds=w.debounce_seconds,
            )
        )
    return watches


def cmd_run(args: argparse.Namespace) -> int:
    cfg = load(args.config)
    _setup_logging(cfg.log_level)
    reg = _build_registry(cfg)
    db = Database(cfg.database)
    factory = _progress_factory(getattr(args, "progress", False))
    try:
        watches = _build_watches(cfg, reg, db)
        engine = Engine()
        for w in watches:
            engine.add(w)
        engine.run_forever(scan_wrap_factory=factory)
    finally:
        db.close()
    return 0


def cmd_scan(args: argparse.Namespace) -> int:
    cfg = load(args.config)
    _setup_logging(cfg.log_level)
    reg = _build_registry(cfg)
    db = Database(cfg.database)
    factory = _progress_factory(getattr(args, "progress", False))
    try:
        for w in _build_watches(cfg, reg, db):
            wrap = factory(w.name) if factory else None
            w.scan_once(wrap=wrap)
    finally:
        db.close()
    return 0


def cmd_reconcile(args: argparse.Namespace) -> int:
    cfg = load(args.config)
    _setup_logging(cfg.log_level)
    reg = _build_registry(cfg)
    db = Database(cfg.database)
    factory = _progress_factory(getattr(args, "progress", False))
    try:
        for w in _build_watches(cfg, reg, db):
            wrap = factory(w.name) if factory else None
            w.reconcile(wrap=wrap)
    finally:
        db.close()
    return 0


def cmd_list(args: argparse.Namespace) -> int:
    cfg = load(args.config)
    _setup_logging(cfg.log_level)
    reg = _build_registry(cfg)
    print("Registered algorithms:")
    for name in reg.names():
        spec = reg.get(name)
        print(f"  - {name}  (watch_mode={spec.watch_mode}, revision={spec.revision})")
    print("\nConfigured watches:")
    for w in cfg.watches:
        p = w.policy
        print(
            f"  - {w.name}: {w.source} -> {w.dest} via {w.algorithm}"
            f"  [on_conflict={p.on_conflict.value}, on_change={p.on_change.value}, on_delete={p.on_delete.value}]"
        )
    return 0


def cmd_doctor(args: argparse.Namespace) -> int:
    cfg = load(args.config)
    _setup_logging(cfg.log_level)
    reg = _build_registry(cfg)
    problems = 0
    for w in cfg.watches:
        try:
            reg.get(w.algorithm)
        except KeyError as e:
            print(f"[FAIL] {w.name}: {e}")
            problems += 1
            continue
        if not w.source.is_dir():
            print(f"[FAIL] {w.name}: source not found: {w.source}")
            problems += 1
            continue
        try:
            w.dest.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            print(f"[FAIL] {w.name}: cannot create dest {w.dest}: {e}")
            problems += 1
            continue
        print(f"[ OK ] {w.name}")
    try:
        Database(cfg.database).close()
        print(f"[ OK ] database: {cfg.database}")
    except Exception as e:
        print(f"[FAIL] database {cfg.database}: {e}")
        problems += 1
    return 0 if problems == 0 else 1


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="fferyman")
    sub = p.add_subparsers(dest="cmd", required=True)

    PROGRESS_CMDS = {"run", "scan", "reconcile"}

    for name, handler, help_ in (
        ("run", cmd_run, "watch and mirror continuously"),
        ("scan", cmd_scan, "one-shot full scan then exit"),
        ("reconcile", cmd_reconcile, "re-sync after policy/algorithm change"),
        ("list", cmd_list, "list registered algorithms and watches"),
        ("doctor", cmd_doctor, "validate config, paths, plugins"),
    ):
        sp = sub.add_parser(name, help=help_)
        sp.add_argument("--config", "-c", type=Path, required=True)
        if name in PROGRESS_CMDS:
            sp.add_argument(
                "--progress",
                action="store_true",
                help="show a tqdm progress bar during initial scan "
                "(install with `pip install fferyman[progress]`)",
            )
        sp.set_defaults(func=handler)
    return p


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
