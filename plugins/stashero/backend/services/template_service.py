from typing import Any, Dict, List, Optional

from backend.services.file_mover import FileMover


class TemplateService:
    def __init__(self, mover: FileMover):
        self._mover = mover

    def list_templates(self) -> List[Dict[str, Any]]:
        return self._mover.list_templates()

    def save_template(
        self,
        name: str,
        filename_template: str,
        path_template: Optional[str],
        criteria_json: Optional[str] = None,
    ) -> Dict[str, Any]:
        return self._mover.save_template(
            name=name,
            filename_template=filename_template,
            path_template=path_template,
            criteria_json=criteria_json,
        )

    def update_template(
        self,
        template_id: str,
        name: str,
        filename_template: str,
        path_template: Optional[str],
        criteria_json: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        return self._mover.update_template(
            template_id=template_id,
            name=name,
            filename_template=filename_template,
            path_template=path_template,
            criteria_json=criteria_json,
        )

    def delete_template(self, template_id: str) -> bool:
        return self._mover.delete_template(template_id=template_id)

    def get_hook_settings(self, hook_type: str) -> Dict[str, Any]:
        return self._mover.get_hook_settings(hook_type=hook_type)

    def save_hook_settings(
        self,
        hook_type: str,
        enabled: bool,
        template_ids: List[str],
    ) -> Dict[str, Any]:
        return self._mover.save_hook_settings(
            hook_type=hook_type,
            enabled=enabled,
            template_ids=template_ids,
        )

    def list_templates_by_ids(self, template_ids: List[str]) -> List[Dict[str, Any]]:
        return self._mover.list_templates_by_ids(template_ids=template_ids)
