"""Flatten sendout-ID directories at any depth into dest.

A "sendout" directory is identified purely by its name matching
`<ID>-YYYYMMDD-HHMMSS` (e.g. `0E4R00183AA1-20260324-153909`). The regex watch
mode finds them at any depth under source; child file events bubble up so
directories that fill in over time are re-ingested once the debounce window
closes.

Collision / change / delete semantics come from the watch policy, not this
file. Typical config:

    on_conflict:   duplicate
    duplicate_dir: DUPLICATE
    on_change:     version
    on_delete:     keep_dest
    debounce_seconds: 5   # raise if producers write slowly

Put this file under `plugins_dir` or install as a console entry point.
"""
from fferyman import algorithm


@algorithm(
    "sendout_flatten",
    watch_mode=r"regex:^[A-Za-z0-9]+-\d{8}-\d{6}$",
    revision=1,
)
def sendout_flatten(src, dest, **_):
    return dest / src.name
