#!/usr/bin/env python3
"""Stashero plugin entrypoint."""

import json
import sys
import traceback
from typing import Any, Dict, Optional

from backend.services.logger import LoggerService
from backend.services.runtime_preflight import run_preflight, to_json_error

log = LoggerService(debug_mode=True)


def read_json_input() -> Optional[Dict[str, Any]]:
    raw = sys.stdin.read()
    if not raw:
        return None
    return json.loads(raw)


def parse_plugin_value_input(v: Any) -> Any:
    """Decode PluginValueInput-like payloads: {str|i|b|f|o|a}."""
    if not isinstance(v, dict):
        return v
    if "str" in v:
        return v.get("str")
    if "i" in v:
        return v.get("i")
    if "b" in v:
        return v.get("b")
    if "f" in v:
        return v.get("f")
    if "o" in v:
        obj: Dict[str, Any] = {}
        for item in v.get("o") or []:
            if not isinstance(item, dict):
                continue
            key = item.get("key")
            if not key:
                continue
            obj[str(key)] = parse_plugin_value_input(item.get("value"))
        return obj
    if "a" in v:
        return [parse_plugin_value_input(item) for item in (v.get("a") or [])]
    return v


def normalize_input_args(raw_args: Any) -> Dict[str, Any]:
    """Accept either plain map/object or PluginArgInput list."""
    if raw_args is None:
        return {}
    if isinstance(raw_args, dict):
        return raw_args
    if isinstance(raw_args, list):
        out: Dict[str, Any] = {}
        for item in raw_args:
            if not isinstance(item, dict):
                continue
            key = item.get("key")
            if not key:
                continue
            out[str(key)] = parse_plugin_value_input(item.get("value"))
        return out
    raise Exception("Expected input args to be a map/object or PluginArgInput list")


def _build_error_payload(error: Exception, stage: str) -> Dict[str, Any]:
    payload = to_json_error(error)
    details = payload.get("details")
    if not isinstance(details, dict):
        details = {}
        payload["details"] = details
    details["stage"] = stage
    details["exception_type"] = error.__class__.__name__
    return payload


def main() -> int:
    output: Dict[str, Any] = {}
    exit_code = 0

    try:
        run_preflight()

        input_data = read_json_input()
        if not input_data:
            raise ValueError("No input received from Stash")

        log.trace(f"Input data: {input_data}")
        args = normalize_input_args(input_data.get("args"))
        server_conn = input_data.get("server_connection") or {}

        scheme = server_conn.get("Scheme", "http")
        host = server_conn.get("Host", "localhost")
        port = server_conn.get("Port", 9999)
        session_cookie = server_conn.get("SessionCookie") or {}

        options: Dict[str, Any] = dict(args)
        options["server_url"] = f"{scheme}://{host}:{port}/graphql"
        options["cookie_name"] = session_cookie.get("Name", "")
        options["cookie_value"] = session_cookie.get("Value", "")
        options["PluginDir"] = server_conn.get("PluginDir", "")

        from backend.app import run

        result = run(options, collect_operations=True)
        if isinstance(result, dict):
            output["output"] = result
        else:
            operations = result or []
            output["output"] = {"operations": operations}

    except Exception as error:
        exit_code = 1
        payload = _build_error_payload(error, stage="entrypoint")
        output["error"] = payload
        log.error(f"Stashero failed: {error}")
        try:
            log.error(json.dumps(payload, ensure_ascii=False))
        except Exception:
            pass
        log.error(traceback.format_exc())

    try:
        print(json.dumps(output), flush=True)
    except Exception as print_error:
        fallback = {
            "error": {
                "code": "OUTPUT_SERIALIZATION_FAILED",
                "message": str(print_error),
                "details": {"exception_type": print_error.__class__.__name__},
            }
        }
        print(json.dumps(fallback), flush=True)
        return 1

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
