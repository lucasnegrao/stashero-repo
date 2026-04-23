from typing import Any, Dict, List

from backend.services.file_mover import FileMover


class UndoService:
    def __init__(self, mover: FileMover):
        self._mover = mover

    def undo_rename(self, operation_id: str) -> Dict[str, Any]:
        return self._mover.undo_rename(operation_id)

    def undo_batch(self, batch_id: str) -> Dict[str, Any]:
        return self._mover.undo_batch_operation(batch_id)

    def list_operations(self) -> List[Dict[str, Any]]:
        return self._mover.list_rename_operations()

    def list_batches(self) -> List[Dict[str, Any]]:
        return self._mover.list_operation_batches()

    def list_batch_operations(self, batch_id: str) -> List[Dict[str, Any]]:
        return self._mover.list_batch_operations(batch_id)

    def clear_history(self) -> Dict[str, Any]:
        return self._mover.clear_history()
