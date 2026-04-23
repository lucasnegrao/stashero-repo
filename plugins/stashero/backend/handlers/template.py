import json
from typing import Any, Dict
from backend.handlers.context import AppContext
from backend.handlers.utils import criteria_from_template_payload


def handle_list_templates(options: Dict[str, Any], ctx: AppContext):
    return {"templates": ctx.templates.list_templates()}


def handle_save_template(options: Dict[str, Any], ctx: AppContext):
    template_name = str(options.get("template_name") or "").strip()
    filename_tpl = str(options.get("filename_template") or "").strip()
    path_tpl = str(options.get("path_template") or "")
    criteria_payload = criteria_from_template_payload(options.get("criteria"))
    try:
        criteria_json = json.dumps(criteria_payload, ensure_ascii=False)
    except Exception as e:
        raise ValueError(f"Invalid criteria payload: {e}")

    if not template_name:
        raise ValueError("template_name is required for template:save_template")
    if not filename_tpl:
        raise ValueError("filename_template is required for template:save_template")

    return {
        "template": ctx.templates.save_template(
            name=template_name,
            filename_template=filename_tpl,
            path_template=path_tpl,
            criteria_json=criteria_json,
        )
    }


def handle_update_template(options: Dict[str, Any], ctx: AppContext):
    template_id = str(options.get("template_id") or "").strip()
    template_name = str(options.get("template_name") or "").strip()
    filename_tpl = str(options.get("filename_template") or "").strip()
    path_tpl = str(options.get("path_template") or "")
    criteria_payload = criteria_from_template_payload(options.get("criteria"))
    try:
        criteria_json = json.dumps(criteria_payload, ensure_ascii=False)
    except Exception as e:
        raise ValueError(f"Invalid criteria payload: {e}")

    if not template_id:
        raise ValueError("template_id is required for template:update_template")
    if not template_name:
        raise ValueError("template_name is required for template:update_template")
    if not filename_tpl:
        raise ValueError("filename_template is required for template:update_template")

    updated = ctx.templates.update_template(
        template_id=template_id,
        name=template_name,
        filename_template=filename_tpl,
        path_template=path_tpl,
        criteria_json=criteria_json,
    )
    if not updated:
        raise ValueError(f"Template not found: {template_id}")
    return {"template": updated}


def handle_delete_template(options: Dict[str, Any], ctx: AppContext):
    template_id = str(options.get("template_id") or "").strip()
    if not template_id:
        raise ValueError("template_id is required for template:delete_template")
    return {
        "deleted": bool(ctx.templates.delete_template(template_id)),
        "template_id": template_id,
    }
