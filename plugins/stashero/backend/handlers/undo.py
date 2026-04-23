from typing import Any, Dict
from backend.handlers.context import AppContext


def handle_undo(options: Dict[str, Any], ctx: AppContext):
    undo_operation_id = str(options.get("undo_operation_id") or "").strip()
    if not undo_operation_id:
        raise ValueError("undo_operation_id is required for mode=undo:undo")
    undo_result = ctx.undo.undo_rename(undo_operation_id)
    return [undo_result] if ctx.collect_operations else None


def handle_list_operations(options: Dict[str, Any], ctx: AppContext):
    ops = ctx.undo.list_operations()
    return ops if ctx.collect_operations else None


def handle_list_operation_batches(options: Dict[str, Any], ctx: AppContext):
    return {"batches": ctx.undo.list_batches()}


def handle_list_batch_operations(options: Dict[str, Any], ctx: AppContext):
    batch_id = str(options.get("batch_id") or "")
    if not batch_id:
        raise ValueError("batch_id is required for undo:list_batch_operations")
    return {"operations": ctx.undo.list_batch_operations(batch_id=batch_id)}


def handle_undo_batch_operation(options: Dict[str, Any], ctx: AppContext):
    batch_id = str(options.get("batch_id") or "")
    if not batch_id:
        raise ValueError("batch_id is required for undo:undo_batch_operation")
    return ctx.undo.undo_batch(batch_id=batch_id)


def handle_clear_history(options: Dict[str, Any], ctx: AppContext):
    return ctx.undo.clear_history()
