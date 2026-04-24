#!/usr/bin/env python3
"""Minimal runtime preflight for Stashero.

Behavior:
- Enforce minimum Python version.
- Enforce local venv usage (.venv_runtime at plugin root).
- Install requirements inside venv when requirements.txt changes.
- Re-exec current process with venv python when needed.
"""

from __future__ import annotations

import fcntl
import hashlib
import importlib
import json
import os
import re
import subprocess
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

from backend.services import stash_log as log

MIN_PYTHON_VERSION: Tuple[int, int] = (3, 9)
VENV_ACTIVE_ENV = "STASHERO_VENV_ACTIVE"
VENV_DIR_ENV = "STASHERO_VENV_DIR"
REQUIREMENT_IMPORT_OVERRIDES: Dict[str, str] = {
    "python-liquid": "liquid",
}


class RuntimePreflightError(Exception):
    def __init__(self, code: str, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.code = str(code)
        self.message = str(message)
        self.details = details or {}

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "code": self.code,
            "message": self.message,
        }
        if self.details:
            payload["details"] = self.details
        return payload


def _info(message: str) -> None:
    log.LogInfo(f"[runtime_preflight] {message}")


def _debug(message: str) -> None:
    log.LogDebug(f"[runtime_preflight] {message}")


def _warn(message: str) -> None:
    log.LogWarning(f"[runtime_preflight] {message}")


def _tail(text: str, limit: int = 4000) -> str:
    return text if len(text) <= limit else text[-limit:]


def _python_version_str(v: Sequence[int]) -> str:
    major = int(v[0]) if len(v) > 0 else 0
    minor = int(v[1]) if len(v) > 1 else 0
    micro = int(v[2]) if len(v) > 2 else 0
    return f"{major}.{minor}.{micro}"


def _check_python_version(min_version: Tuple[int, int]) -> None:
    current = sys.version_info
    required = (int(min_version[0]), int(min_version[1]))
    if (current.major, current.minor) >= required:
        return
    raise RuntimePreflightError(
        code="PYTHON_VERSION_UNSUPPORTED",
        message=(
            f"Python {required[0]}.{required[1]}+ is required, "
            f"but current runtime is {_python_version_str((current.major, current.minor, current.micro))}."
        ),
        details={
            "required": f"{required[0]}.{required[1]}+",
            "current": _python_version_str((current.major, current.minor, current.micro)),
            "python_executable": sys.executable,
        },
    )


def _project_root(requirements_path: Optional[Path] = None) -> Path:
    if requirements_path:
        return requirements_path.resolve().parent
    return Path(__file__).resolve().parents[2]


def _requirements_path(requirements_path: Optional[Path] = None) -> Path:
    return requirements_path or (_project_root(requirements_path) / "requirements.txt")


def _venv_dir(requirements_path: Path) -> Path:
    override = str(os.environ.get(VENV_DIR_ENV) or "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return (requirements_path.parent / ".venv_runtime").resolve()


def _venv_python(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _is_virtualenv_python() -> bool:
    if bool(os.environ.get("VIRTUAL_ENV")):
        return True
    return bool(getattr(sys, "base_prefix", sys.prefix) != sys.prefix)


def _requirements_hash(requirements_path: Path) -> str:
    return hashlib.sha256(requirements_path.read_bytes()).hexdigest()


def _parse_requirement_names(requirements_path: Path) -> List[str]:
    if not requirements_path.exists():
        raise RuntimePreflightError(
            code="REQUIREMENTS_FILE_NOT_FOUND",
            message=f"requirements file not found: {requirements_path}",
            details={"requirements_path": str(requirements_path)},
        )

    names: List[str] = []
    pattern = re.compile(r"^\s*([A-Za-z0-9_.-]+)")
    for line in requirements_path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#"):
            continue
        match = pattern.match(text)
        if match:
            names.append(match.group(1))
    return names


def _module_for_requirement(requirement_name: str) -> str:
    key = str(requirement_name or "").strip().lower()
    if key in REQUIREMENT_IMPORT_OVERRIDES:
        return REQUIREMENT_IMPORT_OVERRIDES[key]
    return str(requirement_name or "").strip().replace("-", "_")


def _find_import_failures(requirement_names: Sequence[str]) -> List[Dict[str, str]]:
    failures: List[Dict[str, str]] = []
    for req_name in requirement_names:
        module_name = _module_for_requirement(req_name)
        try:
            importlib.invalidate_caches()
            sys.modules.pop(module_name, None)
            importlib.import_module(module_name)
        except Exception as exc:
            failures.append(
                {
                    "requirement": str(req_name),
                    "module": module_name,
                    "error": f"{exc.__class__.__name__}: {exc}",
                }
            )
    return failures


def _install_requirements(requirements_path: Path, python_executable: str) -> None:
    cmd = [
        str(python_executable),
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
        "-r",
        str(requirements_path),
    ]
    _info(f"installing requirements with: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode == 0:
        _debug("pip install completed successfully")
        return

    stdout_tail = _tail(proc.stdout or "")
    stderr_tail = _tail(proc.stderr or "")
    if stdout_tail:
        for line in stdout_tail.splitlines():
            _warn(f"pip stdout: {line}")
    if stderr_tail:
        for line in stderr_tail.splitlines():
            _warn(f"pip stderr: {line}")

    raise RuntimePreflightError(
        code="REQUIREMENTS_INSTALL_FAILED",
        message="failed to install Python requirements in virtual environment",
        details={
            "requirements_path": str(requirements_path),
            "python_executable": str(python_executable),
            "exit_code": proc.returncode,
            "stdout": stdout_tail,
            "stderr": stderr_tail,
        },
    )


@contextmanager
def _preflight_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as handle:
        _debug(f"waiting for lock: {lock_path}")
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _ensure_venv(requirements_path: Path, min_python: Tuple[int, int]) -> str:
    venv_dir = _venv_dir(requirements_path)
    venv_python = _venv_python(venv_dir)
    marker_path = venv_dir / ".stashero_requirements.sha256"

    if not venv_python.exists():
        _info(f"creating venv at: {venv_dir}")
        proc = subprocess.run(
            [sys.executable, "-m", "venv", str(venv_dir)],
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            raise RuntimePreflightError(
                code="VENV_CREATE_FAILED",
                message="failed to create local virtual environment",
                details={
                    "venv_dir": str(venv_dir),
                    "python_executable": sys.executable,
                    "exit_code": proc.returncode,
                    "stdout": _tail(proc.stdout or ""),
                    "stderr": _tail(proc.stderr or ""),
                },
            )

    if not venv_python.exists():
        raise RuntimePreflightError(
            code="VENV_CREATE_FAILED",
            message="venv created but python executable not found",
            details={"venv_dir": str(venv_dir), "expected_python": str(venv_python)},
        )

    # Check venv python version
    version_check = subprocess.run(
        [
            str(venv_python),
            "-c",
            (
                "import sys;"
                f"raise SystemExit(0 if (sys.version_info.major, sys.version_info.minor) >= ({int(min_python[0])}, {int(min_python[1])}) else 1)"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if version_check.returncode != 0:
        raise RuntimePreflightError(
            code="PYTHON_VERSION_UNSUPPORTED",
            message="venv python does not meet minimum required version",
            details={
                "required": f"{int(min_python[0])}.{int(min_python[1])}+",
                "venv_python": str(venv_python),
            },
        )

    current_hash = _requirements_hash(requirements_path)
    previous_hash = ""
    if marker_path.exists():
        try:
            previous_hash = marker_path.read_text(encoding="utf-8").strip()
        except Exception:
            previous_hash = ""

    if previous_hash != current_hash:
        _install_requirements(requirements_path, str(venv_python))
        marker_path.write_text(current_hash, encoding="utf-8")
    else:
        _debug("venv requirements marker is up to date")

    return str(venv_python)


def _reexec_into_venv(venv_python: str, venv_dir: str) -> None:
    _info(f"restarting process with venv python: {venv_python}")
    env = dict(os.environ)
    env[VENV_ACTIVE_ENV] = "1"
    env["VIRTUAL_ENV"] = venv_dir
    venv_bin = str(Path(venv_dir) / ("Scripts" if os.name == "nt" else "bin"))
    current_path = str(env.get("PATH") or "")
    env["PATH"] = f"{venv_bin}{os.pathsep}{current_path}" if current_path else venv_bin
    os.execve(venv_python, [venv_python, *sys.argv], env)


def get_runtime_python_path(requirements_path: Optional[Path] = None) -> str:
    req_path = _requirements_path(requirements_path)
    venv_py = _venv_python(_venv_dir(req_path))
    if venv_py.exists():
        return str(venv_py)
    return str(sys.executable)


def run_preflight(
    requirements_path: Optional[Path] = None,
    min_python: Tuple[int, int] = MIN_PYTHON_VERSION,
) -> str:
    req_path = _requirements_path(requirements_path)
    lock_path = _project_root(req_path) / ".runtime_preflight.lock"

    with _preflight_lock(lock_path):
        _debug(f"python={sys.executable} version={sys.version}")
        _debug(f"requirements_path={req_path}")

        _check_python_version(min_python)

        if (
            not _is_virtualenv_python()
            and str(os.environ.get(VENV_ACTIVE_ENV) or "").strip() != "1"
        ):
            _info("not running in virtualenv; bootstrapping local venv")
            venv_python = _ensure_venv(req_path, min_python)
            venv_dir = str(Path(venv_python).parent.parent)
            _reexec_into_venv(venv_python, venv_dir)

        requirement_names = _parse_requirement_names(req_path)
        import_failures = _find_import_failures(requirement_names)
        if import_failures:
            _warn("detected missing imports in active python; reinstalling requirements")
            _install_requirements(req_path, str(sys.executable))
            import_failures = _find_import_failures(requirement_names)
            if import_failures:
                raise RuntimePreflightError(
                    code="REQUIREMENTS_VALIDATION_FAILED",
                    message="requirements are not importable in active python",
                    details={
                        "requirements_path": str(req_path),
                        "import_failures": import_failures,
                    },
                )

        _info("runtime preflight validated successfully")
        return str(sys.executable)


def to_json_error(error: Exception) -> Dict[str, Any]:
    if isinstance(error, RuntimePreflightError):
        return error.to_payload()
    return {
        "code": "RUNTIME_PREFLIGHT_FAILED",
        "message": str(error),
        "details": {},
    }


def main() -> int:
    try:
        run_preflight()
        return 0
    except Exception as exc:
        log.LogError(f"[runtime_preflight] failed: {exc}")
        payload = {"error": to_json_error(exc)}
        print(json.dumps(payload), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
