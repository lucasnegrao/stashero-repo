import json
import re
from typing import Any, Dict, List
from backend.handlers.context import AppContext
from backend.handlers.utils import (
    ensure_list_of_strings,
    criteria_from_template_payload,
    to_bool,
)
from backend.filter.criteria import build_scene_filter, combine_scene_filters
from backend.filter.scenes import fetch_scenes_by_filters

RUN_PLUGIN_TASK_MUTATION = """
mutation RunPluginTask($plugin_id: ID!, $description: String, $args_map: Map) {
    runPluginTask(
        plugin_id: $plugin_id,
        description: $description,
        args_map: $args_map
    )
}
"""


def _hook_batch_identity(hook_type: str) -> Dict[str, str]:
    raw_hook = str(hook_type or "").strip() or "Scene.Update.Post"
    normalized = re.sub(r"[^a-zA-Z0-9]+", "_", raw_hook).strip("_").lower()
    if not normalized:
        normalized = "scene_update_post"
    return {
        "batch_id": f"stashero_hook_batch__{normalized}",
        "batch_mode": f"hook:{raw_hook}",
    }


def handle_get_settings(options: Dict[str, Any], ctx: AppContext):
    hook_type = str(options.get("hook_type") or "Scene.Update.Post")
    return {"hook_settings": ctx.templates.get_hook_settings(hook_type=hook_type)}


def handle_save_settings(options: Dict[str, Any], ctx: AppContext):
    hook_type = str(options.get("hook_type") or "Scene.Update.Post")
    enabled = to_bool(options.get("enabled", False))
    template_ids = ensure_list_of_strings(options.get("template_ids"), "template_ids")
    saved = ctx.templates.save_hook_settings(
        hook_type=hook_type,
        enabled=enabled,
        template_ids=template_ids,
    )
    return {"hook_settings": saved}


def handle_run(options: Dict[str, Any], ctx: AppContext):
    hook_context = options.get("hookContext")
    if not isinstance(hook_context, dict):
        raise ValueError("hookContext is required for hook:run")

    hook_type = str(hook_context.get("type") or options.get("hook_type") or "").strip()
    if not hook_type:
        raise ValueError("hookContext.type is required for hook:run")

    object_id = str(hook_context.get("id") or "").strip()
    if not object_id:
        raise ValueError("hookContext.id is required for hook:run")

    hook_settings = ctx.templates.get_hook_settings(hook_type=hook_type)
    if not hook_settings.get("enabled"):
        return {
            "hook_type": hook_type,
            "scene_id": object_id,
            "enabled": False,
            "executed": [],
        }

    template_ids = ensure_list_of_strings(
        hook_settings.get("template_ids"),
        "hook_settings.template_ids",
    )
    hook_batch = _hook_batch_identity(hook_type)
    configured_templates = ctx.templates.list_templates_by_ids(
        template_ids=template_ids
    )
    if not configured_templates:
        return {
            "hook_type": hook_type,
            "scene_id": object_id,
            "enabled": True,
            "executed": [],
        }

    executed: List[Dict[str, Any]] = []
    for row in configured_templates:
        template_id = str(row.get("id") or "")
        template_name = str(row.get("name") or "")
        filename_template = str(row.get("filename_template") or "").strip()
        path_template = row.get("path_template") or None
        if not template_id or not filename_template:
            continue

        parsed_criteria: List[dict] = []
        raw_filter_json = str(row.get("filter_json") or "").strip()
        ctx.logger.debug(
            f"Hook template '{template_name}' ({template_id}) raw filter_json: {raw_filter_json}"
        )
        if raw_filter_json:
            try:
                raw_payload = json.loads(raw_filter_json)
                parsed_criteria = criteria_from_template_payload(raw_payload)
            except Exception:
                parsed_criteria = []

        criteria_opt = parsed_criteria
        ctx.logger.debug(
            "Hook template "
            f"'{template_name}' ({template_id}) criteria from DB: "
            f"{json.dumps(criteria_opt, ensure_ascii=False)}"
        )

        scene_filter = build_scene_filter(
            None,
            criteria_opt,
            logger=ctx.logger if ctx.debug_mode else None,
        )
        scene_filter_for_object = scene_filter
        object_id_int = None
        try:
            object_id_int = int(object_id)
        except Exception:
            pass

        if object_id_int is not None:
            id_filter = {"id": {"modifier": "EQUALS", "value": object_id_int}}
            scene_filter_for_object = combine_scene_filters(
                scene_filter_for_object, id_filter
            )
            ctx.logger.debug(
                f"Hook template '{template_name}' ({template_id}) combined scene_filter+id: "
                f"{json.dumps(scene_filter_for_object, ensure_ascii=False)}"
            )
        else:
            ctx.logger.warning(
                f"Hook object_id '{object_id}' is not numeric; falling back to ids argument matching"
            )

        scenes = fetch_scenes_by_filters(
            gql_call=ctx.gql.call,
            tagger=ctx.tagger,
            scene_filter=scene_filter_for_object,
            ids=None if object_id_int is not None else [object_id],
            find_filter=None,
            filename_template=filename_template,
            path_template=path_template,
        )
        if len(scenes) == 0:
            executed.append(
                {
                    "template_id": template_id,
                    "template_name": str(row.get("name") or ""),
                    "matched": False,
                }
            )
            continue

        args_map = {
            "mode": "rename:run",
            "filename_template": filename_template,
            "ids": [object_id],
            "batch_id": hook_batch["batch_id"],
            "batch_mode": hook_batch["batch_mode"],
        }
        if path_template:
            args_map["path_template"] = path_template

        variables = {
            "plugin_id": "stashero",
            "description": f"Stash Renamer: Hook execution for {template_name}",
            "args_map": args_map,
        }

        try:
            data = ctx.gql.call(RUN_PLUGIN_TASK_MUTATION, variables)
            job_id = (data or {}).get("runPluginTask")
            executed.append(
                {
                    "template_id": template_id,
                    "template_name": str(row.get("name") or ""),
                    "matched": True,
                    "job_id": job_id,
                }
            )

        except Exception as e:
            raise

    return {
        "hook_type": hook_type,
        "scene_id": object_id,
        "enabled": True,
        "executed": executed,
    }
