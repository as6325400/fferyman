from fferyman import algorithm


@algorithm("flatten", revision=1)
def flatten(src, dest, **_):
    """Flatten any file under source into `dest/<basename>`.

    Conflict / change / delete semantics are driven by the watch policy
    (see `on_conflict`, `on_change`, `on_delete` in the config). The mapper
    itself only answers "where does this source want to land?".
    """
    return dest / src.name
