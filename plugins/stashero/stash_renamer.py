#!/usr/bin/env python3
"""Scene Renamer plugin entrypoint."""

import json
import sys
from typing import Any, Dict, Optional

import backend.services.stash_log as log
from runtime_preflight import run_preflight, to_json_error


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


def main() -> None:
    output: Dict[str, Any] = {}
    try:
        run_preflight()

        input_data = read_json_input()
        if not input_data:
            raise Exception("No input received from Stash")

        log.LogTrace(f"Input data: {input_data}")
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

        source_map = {k: "args" for k in args.keys()}
        source_map["server_url"] = "server_connection"
        source_map["cookie_name"] = "server_connection"
        source_map["cookie_value"] = "server_connection"

        from backend.app import run

        result = run(options, collect_operations=True)
        if isinstance(result, dict):
            output["output"] = result
        else:
            operations = result or []
            output["output"] = {"operations": operations}

    except Exception as e:
        import traceback

        log.LogError(f"Error in stash_renamer: {e}")
        try:
            log.LogError(
                f"Structured error details: {json.dumps(to_json_error(e), ensure_ascii=False)}"
            )
        except Exception:
            pass
        log.LogError(traceback.format_exc())
        output["error"] = to_json_error(e)

    print(json.dumps(output), flush=True)


if __name__ == "__main__":
    main()
