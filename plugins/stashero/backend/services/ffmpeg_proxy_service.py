import json
import os
import platform
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlparse


GET_CONFIG_FFMPEG_QUERY = """
query GetConfigFFmpegPath {
  configuration {
    general {
      ffmpegPath
    }
  }
}
"""


CONFIGURE_GENERAL_MUTATION = """
mutation ConfigureGeneral($input: ConfigGeneralInput!) {
  configureGeneral(input: $input) {
    ffmpegPath
  }
}
"""


class FFmpegProxyService:
    _SCRIPT_TEMPLATE_SH = "ffmpeg_proxy.sh.tpl"
    _SCRIPT_TEMPLATE_CMD = "ffmpeg_proxy.cmd.tpl"

    def __init__(
        self,
        gql_call: Callable[[str, Optional[dict]], dict],
        log_print: Callable[[str], None],
        python_executable: Optional[str] = None,
    ):
        self._gql_call = gql_call
        self._log_print = log_print
        self._python_executable = str(python_executable or "").strip() or str(
            sys.executable or "python3"
        )

    def enable(self, options: Dict[str, Any]) -> Dict[str, Any]:
        runtime_dir = self._runtime_dir(options)
        script_path = self._script_path(runtime_dir)
        state_path = self._state_path(runtime_dir)
        log_path = runtime_dir / "ffmpeg_proxy_commands.log"
        watchdog_log_path = runtime_dir / "ffmpeg_proxy_watchdog.log"
        startup_script_path = self._watchdog_startup_script_path(options)
        python_executable = str(
            options.get("python_path") or self._python_executable or "python3"
        ).strip()
        server_address = self._localhost_server_address(options)
        api_key = str(
            options.get("apiKey") or options.get("api_key") or ""
        ).strip()

        current_path = self._current_ffmpeg_path()
        state = self._read_state(state_path)
        script_path_str = str(script_path)

        if current_path == script_path_str:
            if state and str(state.get("original_ffmpeg_path") or "").strip():
                original_path = str(state.get("original_ffmpeg_path")).strip()
            else:
                raise ValueError(
                    "ffmpegPath already points to proxy script but original path is unknown"
                )
        else:
            original_path = current_path

        if not original_path:
            raise ValueError("Current ffmpegPath is empty; cannot create proxy")

        script_body = self._script_body(
            original_ffmpeg_path=original_path,
            log_path=log_path,
            watchdog_log_path=watchdog_log_path,
            python_executable=python_executable,
            startup_script_path=startup_script_path,
            server_address=server_address,
            api_key=api_key,
        )
        script_path.write_text(script_body, encoding="utf-8")
        if os.name != "nt":
            script_path.chmod(0o755)

        new_state = {
            "script_path": script_path_str,
            "log_path": str(log_path),
            "watchdog_log_path": str(watchdog_log_path),
            "original_ffmpeg_path": original_path,
            "watchdog_startup_script_path": startup_script_path,
            "python_executable": python_executable,
            "server_address": server_address,
            "api_key_present": bool(api_key),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        state_path.write_text(
            json.dumps(new_state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        self._set_ffmpeg_path(script_path_str)
        self._log_print(
            f"FFmpeg proxy enabled. Original='{original_path}' Proxy='{script_path_str}'"
        )

        return {
            "enabled": True,
            "script_path": script_path_str,
            "log_path": str(log_path),
            "watchdog_log_path": str(watchdog_log_path),
            "original_ffmpeg_path": original_path,
            "ffmpeg_path": script_path_str,
            "watchdog_startup_script_path": startup_script_path,
            "python_executable": python_executable,
            "server_address": server_address,
            "api_key_present": bool(api_key),
        }

    def reverse(self, options: Dict[str, Any]) -> Dict[str, Any]:
        runtime_dir = self._runtime_dir(options)
        script_path = self._script_path(runtime_dir)
        state_path = self._state_path(runtime_dir)
        state = self._read_state(state_path)
        script_exists = script_path.exists()

        if not script_exists:
            return {
                "reverted": False,
                "reason": f"Proxy script not found: {script_path}",
                "script_path": str(script_path),
            }

        original_path = str((state or {}).get("original_ffmpeg_path") or "").strip()
        if not original_path:
            raise ValueError(
                "Cannot reverse ffmpeg proxy because original_ffmpeg_path is missing from state"
            )

        self._set_ffmpeg_path(original_path)
        self._log_print(
            f"FFmpeg proxy reversed. Restored ffmpegPath='{original_path}'"
        )
        return {
            "reverted": True,
            "script_path": str(script_path),
            "restored_ffmpeg_path": original_path,
        }

    def _current_ffmpeg_path(self) -> str:
        data = self._gql_call(GET_CONFIG_FFMPEG_QUERY, None)
        configuration = (data or {}).get("configuration") or {}
        general = configuration.get("general") or {}
        configured_path = str(general.get("ffmpegPath") or "").strip()
        if configured_path:
            return configured_path
        env_ffmpeg = self._resolve_ffmpeg_from_env()
        if env_ffmpeg:
            self._log_print(
                f"ffmpegPath is empty in configuration; using environment ffmpeg: {env_ffmpeg}"
            )
        return env_ffmpeg

    def _set_ffmpeg_path(self, ffmpeg_path: str) -> None:
        variables = {"input": {"ffmpegPath": str(ffmpeg_path or "")}}
        self._gql_call(CONFIGURE_GENERAL_MUTATION, variables)

    @staticmethod
    def _resolve_ffmpeg_from_env() -> str:
        # Prefer explicit env override if present.
        explicit = str(os.environ.get("FFMPEG_PATH") or "").strip()
        if explicit:
            return explicit
        discovered = shutil.which("ffmpeg")
        return str(discovered or "").strip()

    @staticmethod
    def _runtime_dir(options: Dict[str, Any]) -> Path:
        plugin_dir = str(
            options.get("PluginPath") or options.get("PluginDir") or ""
        ).strip()
        base = Path(plugin_dir).resolve() if plugin_dir else Path.cwd().resolve()
        runtime_dir = base / ".ffmpeg_proxy"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        return runtime_dir

    @staticmethod
    def _watchdog_startup_script_path(options: Dict[str, Any]) -> str:
        plugin_dir = str(
            options.get("PluginPath") or options.get("PluginDir") or ""
        ).strip()
        base = Path(plugin_dir).resolve() if plugin_dir else Path.cwd().resolve()
        return str(base / "watchdog_startup.py")

    @staticmethod
    def _localhost_server_address(options: Dict[str, Any]) -> str:
        raw_server_url = str(options.get("server_url") or "").strip()
        parsed = urlparse(raw_server_url) if raw_server_url else None
        port = parsed.port if parsed and parsed.port else 9999
        return f"http://localhost:{int(port)}"

    @staticmethod
    def _script_path(runtime_dir: Path) -> Path:
        extension = ".cmd" if os.name == "nt" else ".sh"
        return runtime_dir / f"ffmpeg_proxy{extension}"

    @staticmethod
    def _state_path(runtime_dir: Path) -> Path:
        return runtime_dir / "ffmpeg_proxy_state.json"

    @staticmethod
    def _read_state(state_path: Path) -> Optional[Dict[str, Any]]:
        if not state_path.exists():
            return None
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _script_body(
        original_ffmpeg_path: str,
        log_path: Path,
        watchdog_log_path: Path,
        python_executable: str,
        startup_script_path: str,
        server_address: str,
        api_key: str,
    ) -> str:
        trigger_args = (
            "-hide_banner -v warning -hwaccel_device 0 -f lavfi -i "
            "color=c=red:s=1280x720 -t 0.1 -c:v h264_nvenc -profile p7 "
            "-tune hq -profile high -rc vbr -rc-lookahead 60 -surfaces 64 "
            "-spatial-aq 1 -aq-strength 15 -cq 15 -coder cabac "
            "-b_ref_mode middle -vf format=nv12,hwupload_cuda,scale_cuda=-2:480 "
            "-f null -"
        )

        if os.name == "nt" or platform.system().lower().startswith("win"):
            original_escaped = original_ffmpeg_path.replace('"', '""')
            log_escaped = str(log_path).replace('"', '""')
            wd_log_escaped = str(watchdog_log_path).replace('"', '""')
            py_escaped = python_executable.replace('"', '""')
            startup_escaped = startup_script_path.replace('"', '""')
            server_escaped = server_address.replace('"', '""')
            api_key_escaped = api_key.replace('"', '""')
            trigger_escaped = trigger_args.replace('"', '""')
            template = FFmpegProxyService._load_script_template(
                FFmpegProxyService._SCRIPT_TEMPLATE_CMD
            )
            return FFmpegProxyService._render_script_template(
                template=template,
                replacements={
                    "{{LOG_FILE}}": log_escaped,
                    "{{WATCHDOG_LOG_FILE}}": wd_log_escaped,
                    "{{PYTHON_EXE}}": py_escaped,
                    "{{WATCHDOG_SCRIPT}}": startup_escaped,
                    "{{WATCHDOG_SERVER}}": server_escaped,
                    "{{WATCHDOG_API_KEY}}": api_key_escaped,
                    "{{TRIGGER_ARGS}}": trigger_escaped,
                    "{{ORIGINAL_FFMPEG_PATH}}": original_escaped,
                },
            )

        escaped_original = original_ffmpeg_path.replace("\\", "\\\\").replace('"', '\\"')
        escaped_log = str(log_path).replace("\\", "\\\\").replace('"', '\\"')
        escaped_wd_log = str(watchdog_log_path).replace("\\", "\\\\").replace('"', '\\"')
        escaped_python = python_executable.replace("\\", "\\\\").replace('"', '\\"')
        escaped_startup = startup_script_path.replace("\\", "\\\\").replace('"', '\\"')
        escaped_server = server_address.replace("\\", "\\\\").replace('"', '\\"')
        escaped_api_key = api_key.replace("\\", "\\\\").replace('"', '\\"')
        escaped_trigger = trigger_args.replace("\\", "\\\\").replace('"', '\\"')
        template = FFmpegProxyService._load_script_template(
            FFmpegProxyService._SCRIPT_TEMPLATE_SH
        )
        return FFmpegProxyService._render_script_template(
            template=template,
            replacements={
                "{{LOG_FILE}}": escaped_log,
                "{{WATCHDOG_LOG_FILE}}": escaped_wd_log,
                "{{PYTHON_EXE}}": escaped_python,
                "{{WATCHDOG_SCRIPT}}": escaped_startup,
                "{{WATCHDOG_SERVER}}": escaped_server,
                "{{WATCHDOG_API_KEY}}": escaped_api_key,
                "{{TRIGGER_ARGS}}": escaped_trigger,
                "{{ORIGINAL_FFMPEG_PATH}}": escaped_original,
            },
        )

    @staticmethod
    def _script_templates_dir() -> Path:
        return Path(__file__).resolve().parent / "templates"

    @staticmethod
    def _load_script_template(template_name: str) -> str:
        path = FFmpegProxyService._script_templates_dir() / template_name
        if not path.exists():
            raise FileNotFoundError(f"Missing ffmpeg proxy script template: {path}")
        return path.read_text(encoding="utf-8")

    @staticmethod
    def _render_script_template(template: str, replacements: Dict[str, str]) -> str:
        content = str(template or "")
        for key, value in replacements.items():
            content = content.replace(key, str(value))
        return content
