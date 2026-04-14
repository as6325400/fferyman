import fnmatch

from fferyman import algorithm


@algorithm("mirror_matching_dirs", watch_mode="dir:1", revision=1)
def mirror_matching_dirs(src, dest, *, include="*", exclude="", **_):
    """Mirror a first-level source directory into `dest/<dirname>/`.

    Params:
      include: glob on directory name (default "*")
      exclude: glob to exclude (default "")

    Conflict / change / delete semantics live on the watch policy.
    """
    name = src.name
    if not fnmatch.fnmatch(name, include):
        return None
    if exclude and fnmatch.fnmatch(name, exclude):
        return None
    return dest / name
