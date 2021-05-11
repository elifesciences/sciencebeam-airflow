from typing import List, Optional, T


def get_nested_prop(
    obj: dict,
    path: List[str],
    default_value: Optional[T] = None,
    required: bool = False
) -> Optional[T]:
    current = obj
    for path_fragment in path:
        if not current:
            continue
        current = current.get(path_fragment)
    if not current:
        current = default_value
    if not current and required:
        raise RuntimeError('value with path not found but was required: %r' % path)
    return current
