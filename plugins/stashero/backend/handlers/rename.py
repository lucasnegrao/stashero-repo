from typing import Any, Dict, List
from backend.handlers.context import AppContext
from backend.handlers.utils import ensure_list_of_strings, ensure_list_of_dicts, to_bool
from backend.filter.criteria import build_scene_filter
from backend.filter.scenes import fetch_scenes_by_filters, exclude_scenes_by_ids


def handle_rename(options: Dict[str, Any], ctx: AppContext):
    return _execute_rename_workflow("rename:run", options, ctx)


def handle_preview_dry_run(options: Dict[str, Any], ctx: AppContext):
    return _execute_rename_workflow("rename:preview_dry_run", options, ctx)


def _execute_rename_workflow(mode: str, options: Dict[str, Any], ctx: AppContext):
    filename_template = str(options.get("filename_template") or "").strip()
    if not filename_template:
        raise ValueError("filename_template is required")
    path_template = options.get("path_template") or None

    ids = ensure_list_of_strings(options.get("ids"), "ids")
    criteria_opt = ensure_list_of_dicts(options.get("criteria"), "criteria")
    excluded_scene_ids = ensure_list_of_strings(
        options.get("excluded_scene_ids"),
        "excluded_scene_ids",
    )

    find_filter = options.get("find_filter")
    if find_filter is not None and not isinstance(find_filter, dict):
        raise ValueError("'find_filter' must be an object/dict")
    include_warn_error = to_bool(options.get("include_warn_error", False))
    batch_id_opt = str(options.get("batch_id") or "").strip() or None
    batch_mode_opt = str(options.get("batch_mode") or "").strip() or None

    if isinstance(find_filter, dict):
        find_filter = dict(find_filter)
        find_filter["page"] = 1
        find_filter["per_page"] = 250

    scene_filter = build_scene_filter(
        None,
        criteria_opt,
        logger=ctx.logger if ctx.debug_mode else None,
    )

    scenes = fetch_scenes_by_filters(
        gql_call=ctx.gql.call,
        tagger=ctx.tagger,
        scene_filter=scene_filter,
        ids=ids or None,
        find_filter=find_filter,
        filename_template=filename_template,
        path_template=path_template,
    )

    if excluded_scene_ids:
        before = len(scenes)
        scenes = exclude_scenes_by_ids(scenes, excluded_scene_ids)
        removed = before - len(scenes)
        ctx.logger.debug(
            f"Excluded {removed} scene(s) by selected IDs; remaining {len(scenes)}"
        )

    if mode == "rename:preview_dry_run":
        operations = ctx.engine.preview_run(
            filename_template=filename_template,
            path_template=path_template,
            scene_rows=scenes,
        )
        return {"operations": operations}

    if ctx.dry_run:
        ops = ctx.engine.edit_run(
            filename_template=filename_template,
            path_template=path_template,
            scenes=scenes,
            collect_operations=ctx.collect_operations,
            batch_id=None,
        )
        all_operations: List[dict] = ops or []
        if not include_warn_error:
            filtered_operations: List[dict] = []
            for row in all_operations:
                if not isinstance(row, dict):
                    continue
                status = str(row.get("status") or "").strip().lower()
                if status in ("warn", "warning", "error", "fail", "skipped"):
                    continue
                message = str(row.get("log") or row.get("error") or "").strip().lower()
                if "no change (same path and filename)" in message:
                    continue
                filtered_operations.append(row)
            all_operations = filtered_operations
        return {"operations": all_operations} if ctx.collect_operations else None

    batch_id = ctx.mover.start_batch(
        mode=batch_mode_opt or "rename",
        fixed_batch_id=batch_id_opt,
    )
    try:
        ops = ctx.engine.edit_run(
            filename_template=filename_template,
            path_template=path_template,
            scenes=scenes,
            collect_operations=ctx.collect_operations,
            batch_id=batch_id,
        )
        ctx.mover.complete_batch(batch_id=batch_id, success=True, error=None)
        return (
            {"batch_id": batch_id, "operations": ops or []}
            if ctx.collect_operations
            else None
        )
    except Exception as e:
        ctx.mover.complete_batch(batch_id=batch_id, success=False, error=str(e))
        raise
