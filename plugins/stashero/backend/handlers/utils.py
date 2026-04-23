import json
from typing import Any, List


def ensure_list_of_strings(value: Any, field_name: str) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"'{field_name}' must be a list")
    return [str(x).strip() for x in value if str(x).strip()]


def ensure_list_of_dicts(value: Any, field_name: str) -> List[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"'{field_name}' must be a list")
    out: List[dict] = []
    for item in value:
        if not isinstance(item, dict):
            raise ValueError(f"'{field_name}' entries must be objects")
        out.append(item)
    return out


def to_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in ("true", "1", "yes", "on")


def criteria_from_template_payload(payload: Any) -> List[dict]:
    if payload is None:
        return []
    return ensure_list_of_dicts(payload, "template.criteria")
