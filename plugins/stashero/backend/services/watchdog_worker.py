import argparse
import json
import queue
import signal
import threading
import time
import requests
from pathlib import Path
from typing import Any, Dict, Optional, Set

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from backend.services.graphql import GraphQLConfig, GraphQLService


class TriggeringHandler(FileSystemEventHandler):
    def __init__(self, path_key: str, event_types: Set[str], trigger_callback):
        super().__init__()
        self._path_key = path_key
        self._event_types = event_types
        self._trigger_callback = trigger_callback

    def on_modified(self, event):
        if "modified" in self._event_types:
            self._trigger_callback(self._path_key, "modified", event.src_path)

    def on_created(self, event):
        if "created" in self._event_types:
            self._trigger_callback(self._path_key, "created", event.src_path)

    def on_deleted(self, event):
        if "deleted" in self._event_types:
            self._trigger_callback(self._path_key, "deleted", event.src_path)

    def on_moved(self, event):
        if "moved" in self._event_types:
            self._trigger_callback(self._path_key, "moved", event.src_path)


class PathState:
    def __init__(self):
        self.last_trigger: float = 0.0
        self.in_queue_or_processing: bool = False
        self.pending_retrigger: bool = False


class GraphQLWatchdogWorker:
    def __init__(self, runtime_dir: Path):
        self.runtime_dir = runtime_dir
        self.status_path = runtime_dir / "status.json"
        self.config_path = runtime_dir / "config.json"
        self.log_path = runtime_dir / "watchdog.log"

        self._stop_event = threading.Event()
        self._pending_event = threading.Event()
        self._lock = threading.Lock()
        
        self._queue = queue.Queue()
        self._server_online = False

        config = self._load_config()
        gql_config = GraphQLConfig(
            server_url=str(config.get("server_url") or ""),
            cookie_name=str(config.get("cookie_name") or ""),
            cookie_value=str(config.get("cookie_value") or ""),
        )
        self.gql = GraphQLService(gql_config)

        raw_paths = config.get("watch_paths") or []
        self.watch_paths: Dict[str, Dict[str, Any]] = {}
        self._path_states: Dict[str, PathState] = {}
        
        for path_obj in raw_paths:
            if not isinstance(path_obj, dict):
                continue
            path_key = str(path_obj.get("path") or "").strip()
            if not path_key:
                continue
            self.watch_paths[path_key] = path_obj
            self._path_states[path_key] = PathState()

        self.observer: Optional[Observer] = None

    def run(self) -> None:
        self._write_status("running")
        self._log(f"watchdog worker started with {len(self.watch_paths)} paths")

        self.observer = Observer()
        for path_key, path_obj in self.watch_paths.items():
            event_types = set(path_obj.get("event_types") or ["modified"])
            handler = TriggeringHandler(path_key, event_types, self._on_fs_event)
            self.observer.schedule(
                handler,
                path_key,
                recursive=bool(path_obj.get("recursive", True)),
            )

        self.observer.start()

        observer_thread = threading.Thread(target=self._connection_observer_loop, daemon=True)
        observer_thread.start()

        dispatch_thread = threading.Thread(target=self._dispatch_loop, daemon=True)
        dispatch_thread.start()
        
        worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
        worker_thread.start()

        while not self._stop_event.is_set():
            time.sleep(0.25)

        self._shutdown()

    def _shutdown(self) -> None:
        try:
            if self.observer:
                self.observer.stop()
                self.observer.join(timeout=3.0)
        finally:
            self._write_status("stopped")
            self._log("watchdog worker stopped")

    def _on_fs_event(self, path_key: str, event_type: str, path: str) -> None:
        with self._lock:
            state = self._path_states.get(path_key)
            if not state:
                return
            
            if state.in_queue_or_processing:
                state.pending_retrigger = True
            else:
                state.last_trigger = time.monotonic()
        
        self._pending_event.set()
        self._log(
            f"filesystem event received: path_key={path_key}, type={event_type}, path={path}"
        )

    def _connection_observer_loop(self) -> None:
        self._server_online = False
        while not self._stop_event.is_set():
            try:
                self.gql.call("{ version { version } }", timeout_seconds=5.0)
                if not self._server_online:
                    self._log("Server connection established/restored.")
                self._server_online = True
            except requests.exceptions.RequestException as exc:
                if self._server_online:
                    self._log(f"Server connection lost: {exc}")
                self._server_online = False
            except Exception as exc:
                # Other exceptions like authentication failure might not mean offline
                # but we'll assume it's up if it's responding with valid JSON errors.
                self._server_online = True
                
            self._stop_event.wait(timeout=10.0)

    def _dispatch_loop(self) -> None:
        while not self._stop_event.is_set():
            if not self._pending_event.wait(timeout=0.25):
                continue

            ready_keys = []
            with self._lock:
                now = time.monotonic()
                for path_key, state in self._path_states.items():
                    if state.in_queue_or_processing:
                        continue
                    
                    if state.last_trigger > 0:
                        path_obj = self.watch_paths.get(path_key) or {}
                        debounce = self._to_float(path_obj.get("debounce_seconds"), default=1.0)
                        if (now - state.last_trigger) >= debounce:
                            ready_keys.append(path_key)
                            state.in_queue_or_processing = True
                            state.last_trigger = 0.0

            for path_key in ready_keys:
                self._queue.put(path_key)

            with self._lock:
                has_debouncing = any(
                    s.last_trigger > 0 and not s.in_queue_or_processing 
                    for s in self._path_states.values()
                )
                if not has_debouncing:
                    self._pending_event.clear()

    def _worker_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if not self._server_online:
                    self._stop_event.wait(timeout=1.0)
                    continue

                path_key = self._queue.get(timeout=1.0)
            except queue.Empty:
                continue

            if self._stop_event.is_set():
                break

            success = self._execute_path(path_key)

            if not success and not self._server_online:
                self._log(f"Execution failed while server offline for {path_key}, re-queuing.")
                self._queue.put(path_key)
                continue

            with self._lock:
                state = self._path_states.get(path_key)
                if state:
                    state.in_queue_or_processing = False
                    if state.pending_retrigger:
                        state.pending_retrigger = False
                        state.last_trigger = time.monotonic()
                        self._pending_event.set()
            
            self._queue.task_done()

    def _execute_path(self, path_key: str) -> bool:
        path_obj = self.watch_paths.get(path_key) or {}
        operations = path_obj.get("operations") or []
        success = True
        
        for op in operations:
            if self._stop_event.is_set() or not self._server_online:
                return False
                
            op_id = op.get("id", "unknown")
            query = str(op.get("operation") or "").strip()
            if not query:
                self._log(f"watchdog config {op_id} on path={path_key} has empty operation; skipping")
                continue

            variables = op.get("variables") if isinstance(op.get("variables"), dict) else None
            timeout_seconds = self._to_float(op.get("request_timeout_seconds"), default=30.0)

            try:
                result = self.gql.call(query, variables=variables, timeout_seconds=timeout_seconds)
                keys = list((result or {}).keys()) if isinstance(result, dict) else []
                self._log(
                    f"graphql operation executed for config={op_id} on path={path_key}; data keys={keys}"
                )
            except requests.exceptions.RequestException as exc:
                self._log(f"graphql network operation failed for config={op_id} on path={path_key}: {exc}")
                self._server_online = False
                return False
            except Exception as exc:
                error_str = str(exc)
                if any(code in error_str for code in ["502", "503", "504", "111"]):
                    self._log(f"graphql network operation failed for config={op_id} on path={path_key}: {exc}")
                    self._server_online = False
                    return False
                
                self._log(f"graphql operation failed for config={op_id} on path={path_key}: {exc}")
                pass
                
        return success

    def _load_config(self) -> Dict[str, Any]:
        if not self.config_path.exists():
            raise RuntimeError(f"watchdog config file not found: {self.config_path}")
        config = json.loads(self.config_path.read_text(encoding="utf-8"))
        if not isinstance(config, dict):
            raise RuntimeError("invalid watchdog config payload")

        required = ["server_url", "cookie_name", "cookie_value", "watch_paths"]
        for key in required:
            if not config.get(key):
                raise RuntimeError(f"watchdog config missing required key: {key}")
        if not isinstance(config.get("watch_paths"), list):
            raise RuntimeError("watch_paths must be a list")
        return config

    def _write_status(self, status: str) -> None:
        payload = {
            "status": status,
            "pid": None if status == "stopped" else os_getpid(),
            "updated_at_epoch": int(time.time()),
        }
        self.status_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _log(self, message: str) -> None:
        line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}\n"
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(line)

    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        if value is None:
            return default
        try:
            parsed = float(value)
            if parsed <= 0:
                return default
            return parsed
        except Exception:
            return default


def os_getpid() -> int:
    import os

    return os.getpid()


def main() -> None:
    parser = argparse.ArgumentParser(description="Detached watchdog worker for stash_renamer")
    parser.add_argument("--runtime-dir", required=True)
    args = parser.parse_args()

    runtime_dir = Path(args.runtime_dir).expanduser().resolve()
    runtime_dir.mkdir(parents=True, exist_ok=True)

    worker = GraphQLWatchdogWorker(runtime_dir)

    def handle_signal(_signum, _frame):
        worker._stop_event.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        worker.run()
    except Exception as exc:
        worker._log(f"worker fatal error: {exc}")
        worker._write_status("stopped")
        raise


if __name__ == "__main__":
    main()
