#!/usr/bin/env python3
"""Wait for Stash system status to be OK, then start stash_renamer watchdog task."""

import argparse
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, Optional

import requests


SYSTEM_STATUS_QUERY = """
query {
  systemStatus {
    status
  }
}
"""

START_WATCHDOG_MUTATION = """
mutation {
  runPluginTask(
    plugin_id: "stash_renamer",
    description: "Starting watchdog",
    args_map: { mode: "watchdog:run" }
  )
}
"""


@dataclass
class Config:
    server_url: str
    api_key: str
    wait_seconds: float
    retries: int


def _normalize_server_url(server_address: str) -> str:
    raw = str(server_address or "").strip()
    if not raw:
        raise ValueError("server_address is required")
    if not raw.startswith(("http://", "https://")):
        raw = f"http://{raw}"
    if raw.endswith("/graphql"):
        return raw
    if raw.endswith("/"):
        return f"{raw}graphql"
    return f"{raw}/graphql"


def __callGraphQL(config: Config, query: str, variables: Optional[dict] = None):
    headers = {
        "Accept-Encoding": "gzip, deflate, br",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Connection": "keep-alive",
        "DNT": "1",
    }
    if config.api_key:
        headers["ApiKey"] = config.api_key

    payload: Dict[str, Any] = {"query": query}
    if variables is not None:
        payload["variables"] = variables

    response = requests.post(config.server_url, json=payload, headers=headers, timeout=30)

    if response.status_code == 200:
        result = response.json()
        if result.get("error", None):
            for error in result["error"]["errors"]:
                raise Exception("GraphQL error: {}".format(error))
        if result.get("errors", None):
            for error in result["errors"]:
                raise Exception("GraphQL error: {}".format(error))
        if result.get("data", None):
            return result.get("data")
        raise Exception("GraphQL response missing data field")
    raise Exception(
        "GraphQL query failed:{} - {}. Query: {}. Variables: {}".format(
            response.status_code, response.content, query, variables
        )
    )


def _system_status(config: Config) -> str:
    data = __callGraphQL(config, SYSTEM_STATUS_QUERY)
    return str((data or {}).get("systemStatus", {}).get("status") or "").strip().upper()


def _start_watchdog(config: Config) -> str:
    data = __callGraphQL(config, START_WATCHDOG_MUTATION)
    task_id = str((data or {}).get("runPluginTask") or "").strip()
    if not task_id:
        raise RuntimeError("runPluginTask returned empty task id")
    return task_id


def run(config: Config) -> int:
    attempts = max(1, int(config.retries))
    for attempt in range(1, attempts + 1):
        if config.wait_seconds > 0:
            time.sleep(config.wait_seconds)

        try:
            status = _system_status(config)
            print(f"[Attempt {attempt}/{attempts}] systemStatus={status}", flush=True)
            if status == "OK":
                task_id = _start_watchdog(config)
                print(f"Watchdog start task queued successfully: {task_id}", flush=True)
                return 0
        except Exception as exc:
            print(f"[Attempt {attempt}/{attempts}] error: {exc}", flush=True)

    print("Failed to start watchdog: system status never reached OK within retries.", flush=True)
    return 1


def parse_args(argv: list[str]) -> Config:
    parser = argparse.ArgumentParser(
        description=(
            "Wait for Stash systemStatus=OK and run stash_renamer watchdog:run via runPluginTask."
        )
    )
    parser.add_argument("server_address", help="Stash server address (host[:port] or full URL)")
    parser.add_argument(
        "--api-key",
        default="",
        help="Stash API key (optional; sent as ApiKey header when provided)",
    )
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=2.0,
        help="Seconds to wait before each status check (default: 2.0)",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=10,
        help="Number of attempts before giving up (default: 10)",
    )
    args = parser.parse_args(argv)

    return Config(
        server_url=_normalize_server_url(args.server_address),
        api_key=str(args.api_key or "").strip(),
        wait_seconds=max(0.0, float(args.wait_seconds)),
        retries=max(1, int(args.retries)),
    )


def main(argv: list[str]) -> int:
    try:
        config = parse_args(argv)
    except Exception as exc:
        print(f"Invalid arguments: {exc}", file=sys.stderr, flush=True)
        return 2

    return run(config)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
