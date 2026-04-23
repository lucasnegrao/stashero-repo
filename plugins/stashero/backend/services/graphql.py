from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


@dataclass
class GraphQLConfig:
    server_url: str
    cookie_name: str
    cookie_value: str


class GraphQLService:
    def __init__(self, config: GraphQLConfig):
        self.config = config

    def call(
        self,
        query: str,
        variables: Optional[dict] = None,
        timeout_seconds: Optional[float] = None,
    ) -> Dict[str, Any]:
        if not self.config.server_url:
            raise RuntimeError("CONFIG.server_url missing")
        if not (self.config.cookie_name and self.config.cookie_value):
            raise RuntimeError(
                "Cookie auth required: cookie_name and cookie_value must be set"
            )

        headers = {
            "Accept-Encoding": "gzip, deflate, br",
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Connection": "keep-alive",
            "DNT": "1",
            "Cookie": f"{self.config.cookie_name}={self.config.cookie_value}",
        }

        payload: Dict[str, Any] = {"query": query}
        if variables is not None:
            payload["variables"] = variables

        resp = requests.post(
            self.config.server_url,
            json=payload,
            headers=headers,
            timeout=timeout_seconds,
        )
        if resp.status_code != 200:
            raise Exception(
                f"GraphQL query failed:{resp.status_code} - {resp.content}. Query: {query}. Variables: {variables}"
            )
        result = resp.json()

        errors = []
        if isinstance(result.get("errors"), list):
            errors.extend(result.get("errors") or [])
        wrapped = result.get("error")
        if isinstance(wrapped, dict) and isinstance(wrapped.get("errors"), list):
            errors.extend(wrapped.get("errors") or [])
        if errors:
            message = " | ".join(str(err) for err in errors)
            raise Exception(
                f"GraphQL errors: {message}. Query: {query}. Variables: {variables}"
            )
        if result.get("data") is None:
            raise Exception("GraphQL response missing 'data'")
        return result["data"]
