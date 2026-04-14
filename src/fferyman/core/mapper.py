from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Optional

MapperFn = Callable[..., Optional[Path]]


@dataclass(frozen=True)
class MapperSpec:
    name: str
    fn: MapperFn
    watch_mode: str
    revision: int = 1


def algorithm(
    name: str, *, watch_mode: str = "file", revision: int = 1
) -> Callable[[MapperFn], MapperFn]:
    """Register a function as a fferyman algorithm.

    The function must accept `(src: Path, dest: Path, **params)` and return
    either a **canonical** target `Path` under dest (ignoring existing
    occupants), or `None` to skip this source. Collision / change / delete
    semantics are handled by the watch policy — the mapper does not need
    (and should not try) to reason about them.

    Bump `revision` when changing a mapper's target-selection logic so that
    existing mappings are invalidated on the next scan/reconcile.

    Example:
        @algorithm("flatten", revision=1)
        def flatten(src, dest, **_):
            return dest / src.name
    """
    def decorator(fn: MapperFn) -> MapperFn:
        fn._fferyman_spec = MapperSpec(  # type: ignore[attr-defined]
            name=name, fn=fn, watch_mode=watch_mode, revision=revision
        )
        return fn

    return decorator


def get_spec(obj: Any) -> MapperSpec | None:
    return getattr(obj, "_fferyman_spec", None)
