from typing import Any, Dict, List


def merge_schemas_dict(objs: List[Any]) -> Dict[str, Any]:
    """Convert and merge a list of dataclasses into a single dict.

    Args:
        objs (List[Any]): A list of dataclasses to merge.

    Returns:
        A dict containing the merged contents of the input dicts.
    """
    merged = {}
    for obj in objs:
        merged.update(obj.dict())
    return merged
