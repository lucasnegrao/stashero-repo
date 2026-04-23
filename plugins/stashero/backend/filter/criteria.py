from typing import Any, Dict, List, Optional


def normalize_criteria(criteria: List[Any]) -> List[dict]:
    out: List[dict] = []

    def _to_id(value: Any) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, dict):
            if value.get("id") is not None:
                return str(value.get("id"))
            if value.get("value") is not None:
                return str(value.get("value"))
            return None
        raw = str(value).strip()
        return raw or None

    def _normalize_value(value: Any) -> Any:
        if isinstance(value, list):
            items: List[str] = []
            for item in value:
                normalized = _to_id(item)
                if normalized:
                    items.append(normalized)
            return items
        if isinstance(value, dict) and isinstance(value.get("items"), list):
            items = []
            for item in value.get("items") or []:
                normalized = _to_id(item)
                if normalized:
                    items.append(normalized)
            excluded = []
            for item in value.get("excluded") or []:
                normalized = _to_id(item)
                if normalized:
                    excluded.append(normalized)
            if excluded or "depth" in value:
                return {
                    "items": items,
                    "excluded": excluded,
                    "depth": int(value.get("depth") or 0),
                }
            return items
        if isinstance(value, str):
            lower = value.strip().lower()
            if lower == "true":
                return True
            if lower == "false":
                return False
        return value

    for raw in criteria:
        if not isinstance(raw, dict):
            continue
        if "type" in raw and "modifier" in raw:
            out.append(
                {
                    "type": raw.get("type"),
                    "modifier": raw.get("modifier"),
                    "value": _normalize_value(raw.get("value")),
                }
            )
            continue

        option = raw.get("criterionOption")
        ctype = option.get("type") if isinstance(option, dict) else None
        modifier = raw.get("_modifier")
        value = _normalize_value(raw.get("_value"))
        if not ctype or not modifier:
            continue
        out.append({"type": ctype, "modifier": modifier, "value": value})

    return out


def criterion_to_scene_condition(criterion: dict) -> Optional[dict]:
    ctype = str(criterion.get("type") or "").strip()
    modifier = str(criterion.get("modifier") or "").strip().upper()
    value = criterion.get("value")
    if not ctype or not modifier:
        return None

    if ctype == "movies":
        ctype = "groups"

    def _ids_from_value(v: Any) -> List[str]:
        def _to_id(value: Any) -> Optional[str]:
            if value is None:
                return None
            if isinstance(value, dict):
                if value.get("id") is not None:
                    return str(value.get("id"))
                if value.get("value") is not None:
                    return str(value.get("value"))
                return None
            raw = str(value).strip()
            return raw or None

        if isinstance(v, list):
            out: List[str] = []
            for item in v:
                normalized = _to_id(item)
                if normalized:
                    out.append(normalized)
            return out
        if isinstance(v, dict):
            items = v.get("items")
            if isinstance(items, list):
                ids: List[str] = []
                for item in items:
                    normalized = _to_id(item)
                    if normalized:
                        ids.append(normalized)
                return [x for x in ids if x.strip()]
            if v.get("id") is not None:
                raw = str(v.get("id")).strip()
                return [raw] if raw else []
        if v is None:
            return []
        s = str(v).strip()
        return [s] if s else []

    if ctype == "organized":
        if isinstance(value, dict) and "value" in value:
            value = value.get("value")
        if modifier == "EQUALS":
            if isinstance(value, bool):
                return {"organized": value}
            if isinstance(value, str):
                lv = value.strip().lower()
                if lv == "true":
                    return {"organized": True}
                if lv == "false":
                    return {"organized": False}
        return None

    def _string_from_value(v: Any) -> Optional[str]:
        if v is None:
            return None
        if isinstance(v, str):
            s = v.strip()
            return s or None
        if isinstance(v, dict):
            # String criteria can arrive as {"value": "..."} in saved payloads.
            if "value" in v:
                return _string_from_value(v.get("value"))
            # Backward-compatible handling if old payload encoded list-like values.
            if isinstance(v.get("items"), list):
                for item in v.get("items") or []:
                    s = _string_from_value(item)
                    if s:
                        return s
                return None
        if isinstance(v, list):
            for item in v:
                s = _string_from_value(item)
                if s:
                    return s
            return None
        s = str(v).strip()
        return s or None

    if ctype == "path":
        if modifier in ("IS_NULL", "NOT_NULL"):
            return {"path": {"modifier": modifier}}
        string_value = _string_from_value(value)
        if not string_value:
            return None
        return {"path": {"modifier": modifier, "value": string_value}}

    if ctype in ("tags", "groups", "performers", "studios"):
        if modifier in ("IS_NULL", "NOT_NULL"):
            return {ctype: {"modifier": modifier}}
        ids = _ids_from_value(value)
        excludes: List[str] = []
        if isinstance(value, dict):
            excludes = _ids_from_value(value.get("excluded"))
        if not ids and not excludes:
            return None
        payload: Dict[str, Any] = {"modifier": modifier}
        if ids:
            payload["value"] = ids
        if excludes:
            payload["excludes"] = excludes
        return {ctype: payload}

    return None


def combine_scene_filters(
    left: Optional[dict], right: Optional[dict]
) -> Optional[dict]:
    if left is None:
        return right
    if right is None:
        return left
    out = dict(right)
    out["AND"] = left
    return out


def build_scene_filter(
    scene_filter: Optional[dict], criteria_opt: Optional[List[Any]], logger=None
) -> Optional[dict]:
    if criteria_opt is None:
        return scene_filter
    criteria = normalize_criteria(criteria_opt)
    if logger:
        logger.debug(
            f"Received raw criteria={len(criteria_opt)} normalized={len(criteria)}"
        )
    if criteria_opt and not criteria and logger:
        logger.warning(
            "Criteria payload was provided but no valid criteria entries were normalized"
        )

    criteria_scene_filter: Optional[dict] = None
    for entry in criteria:
        cond = criterion_to_scene_condition(entry)
        if cond is None and logger:
            logger.warning(
                f"Unsupported or invalid criterion dropped: type={entry.get('type')} modifier={entry.get('modifier')}"
            )
        criteria_scene_filter = combine_scene_filters(criteria_scene_filter, cond)

    if criteria_scene_filter is not None:
        if scene_filter is not None and not isinstance(scene_filter, dict):
            raise ValueError("'scene_filter' must be an object/dict")
        scene_filter = combine_scene_filters(scene_filter, criteria_scene_filter)

    if logger:
        logger.debug("Criteria normalized and attached to scene_filter")
    return scene_filter
