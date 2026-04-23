from typing import Any, Dict
import sys
from backend.handlers.context import AppContext

CONFIGURE_PLUGIN_MUTATION = """
mutation ConfigurePluginRuntime($plugin_id: ID!, $input: PluginConfigInput!) {
  configurePlugin(plugin_id: $plugin_id, input: $input)
}
"""


def handle_list_selectors(options: Dict[str, Any], ctx: AppContext):
    return ctx.tagger.build_selectors_catalog()


def handle_ffmpeg_proxy_enable(options: Dict[str, Any], ctx: AppContext):
    return {"ffmpeg_proxy": ctx.ffmpeg_proxy.enable(options)}


def handle_ffmpeg_proxy_reverse(options: Dict[str, Any], ctx: AppContext):
    return {"ffmpeg_proxy": ctx.ffmpeg_proxy.reverse(options)}


def handle_runtime_service_install(options: Dict[str, Any], ctx: AppContext):
    plugin_id = str(options.get("plugin_id") or "stash_renamer").strip()
    python_path = str(options.get("python_path") or sys.executable or "").strip()
    if not plugin_id:
        raise ValueError("plugin_id is required")

    try:
        ctx.gql.call(
            CONFIGURE_PLUGIN_MUTATION,
            {
                "plugin_id": plugin_id,
                "input": {
                    "installed": True,
                    "pythonPath": python_path,
                },
            },
        )
        return {
            "runtime_service": {
                "installed": True,
                "python_path": python_path,
            }
        }
    except Exception:
        # Best-effort rollback signal in config for failed runtime setup.
        try:
            ctx.gql.call(
                CONFIGURE_PLUGIN_MUTATION,
                {
                    "plugin_id": plugin_id,
                    "input": {
                        "installed": False,
                    },
                },
            )
        except Exception:
            pass
        raise
