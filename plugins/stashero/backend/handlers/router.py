from typing import Callable, Dict, Any
from backend.handlers.context import AppContext

from backend.handlers.undo import (
    handle_undo,
    handle_list_operations,
    handle_list_operation_batches,
    handle_list_batch_operations,
    handle_undo_batch_operation,
    handle_clear_history,
)
from backend.handlers.template import (
    handle_list_templates,
    handle_save_template,
    handle_update_template,
    handle_delete_template,
)
from backend.handlers.hook import handle_get_settings, handle_save_settings, handle_run
from backend.handlers.rename import handle_rename, handle_preview_dry_run
from backend.handlers.system import (
    handle_list_selectors,
    handle_run_with_stash_enable,
    handle_run_with_stash_reverse,
    handle_runtime_service_install,
)
from backend.handlers.watchdog import (
    handle_watchdog_run,
    handle_watchdog_stop,
    handle_watchdog_status,
    handle_watchdog_restart,
    handle_watchdog_configure,
    handle_watchdog_save_config,
    handle_watchdog_list_config,
    handle_watchdog_reorder,
    handle_watchdog_delete_config,
)

HandlerFunc = Callable[[Dict[str, Any], AppContext], Any]

ROUTES: Dict[str, HandlerFunc] = {
    "undo:undo": handle_undo,
    "undo:list_operations": handle_list_operations,
    "undo:list_operation_batches": handle_list_operation_batches,
    "undo:list_batch_operations": handle_list_batch_operations,
    "undo:undo_batch_operation": handle_undo_batch_operation,
    "undo:clear_history": handle_clear_history,
    "template:list_templates": handle_list_templates,
    "template:save_template": handle_save_template,
    "template:update_template": handle_update_template,
    "template:delete_template": handle_delete_template,
    "hook:get_settings": handle_get_settings,
    "hook:save_settings": handle_save_settings,
    "hook:run": handle_run,
    "rename:run": handle_rename,
    "rename:preview_dry_run": handle_preview_dry_run,
    "system:list_selectors": handle_list_selectors,
    "system:run_with_stash_enable": handle_run_with_stash_enable,
    "system:run_with_stash_reverse": handle_run_with_stash_reverse,
    "system:runtime_service_install": handle_runtime_service_install,
    "watchdog:run": handle_watchdog_run,
    "watchdog:stop": handle_watchdog_stop,
    "watchdog:status": handle_watchdog_status,
    "watchdog:restart": handle_watchdog_restart,
    "watchdog:configure": handle_watchdog_configure,
    "watchdog:save_config": handle_watchdog_save_config,
    "watchdog:list_config": handle_watchdog_list_config,
    "watchdog:reorder": handle_watchdog_reorder,
    "watchdog:delete_config": handle_watchdog_delete_config,
}
