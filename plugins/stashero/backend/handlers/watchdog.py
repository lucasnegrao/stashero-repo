from typing import Any, Dict

from backend.handlers.context import AppContext


def handle_watchdog_run(options: Dict[str, Any], ctx: AppContext):
    return {"watchdog": ctx.watchdog.run(options)}


def handle_watchdog_stop(options: Dict[str, Any], ctx: AppContext):
    return {"watchdog": ctx.watchdog.stop(options)}


def handle_watchdog_status(options: Dict[str, Any], ctx: AppContext):
    return {"watchdog": ctx.watchdog.status(options)}


def handle_watchdog_restart(options: Dict[str, Any], ctx: AppContext):
    return {"watchdog": ctx.watchdog.restart(options)}


def handle_watchdog_configure(options: Dict[str, Any], ctx: AppContext):
    # Backward-compatible alias.
    return {"watchdog": ctx.watchdog.restart(options)}


def handle_watchdog_save_config(options: Dict[str, Any], ctx: AppContext):
    return {"watchdog": ctx.watchdog.save_config(options)}


def handle_watchdog_list_config(options: Dict[str, Any], ctx: AppContext):
    return {"watchdog": ctx.watchdog.list_configs(options)}


def handle_watchdog_reorder(options: Dict[str, Any], ctx: AppContext):
    return {"watchdog": ctx.watchdog.reorder_configs(options)}


def handle_watchdog_delete_config(options: Dict[str, Any], ctx: AppContext):
    return {"watchdog": ctx.watchdog.delete_config(options)}
