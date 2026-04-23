#!/usr/bin/env python3
"""Runtime preflight checks for Scene Renamer.

This module validates the Python runtime and dependency installation before the
plugin bootstraps backend imports.
"""

from __future__ import annotations

import importlib.metadata
import importlib
import hashlib
import json
import os
import re
import subprocess
import sys
import traceback
import fcntl
import site
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

MIN_PYTHON_VERSION: Tuple[int, int] = (3, 9)
REQUIREMENT_IMPORT_OVERRIDES: Dict[str, str] = {
    "python-liquid": "liquid",
}
VENV_ACTIVE_ENV = "STASH_RENAMER_VENV_ACTIVE"
VENV_DIR_ENV = "STASH_RENAMER_VENV_DIR"


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


def _stderr(message: str) -> None:
    print(f"[runtime_preflight] {message}", file=sys.stderr, flush=True)


def _tail(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


def _log_attempt_output(prefix: str, output_text: str) -> None:
    text = str(output_text or "").strip()
    if not text:
        return
    _stderr(prefix)
    for line in _tail(text).splitlines():
        _stderr(f"  {line}")


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


def _is_virtualenv_python() -> bool:
    if bool(os.environ.get("VIRTUAL_ENV")):
        return True
    return bool(getattr(sys, "base_prefix", sys.prefix) != sys.prefix)


def _venv_dir_from_requirements_path(requirements_path: Path) -> Path:
    override = str(os.environ.get(VENV_DIR_ENV) or "").strip()
    if override:
        return Path(override).expanduser().resolve()
    return (requirements_path.parent / ".venv_runtime").resolve()


def _venv_python_path(venv_dir: Path) -> Path:
    if os.name == "nt":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _requirements_hash(requirements_path: Path) -> str:
    payload = requirements_path.read_bytes()
    return hashlib.sha256(payload).hexdigest()


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
        if not match:
            continue
        names.append(match.group(1))
    return names


def _find_missing_distributions(requirement_names: Sequence[str]) -> List[str]:
    missing: List[str] = []
    for raw_name in requirement_names:
        name = str(raw_name).strip()
        if not name:
            continue
        try:
            importlib.metadata.version(name)
        except importlib.metadata.PackageNotFoundError:
            missing.append(name)
    return missing


def _module_name_for_requirement(requirement_name: str) -> str:
    key = str(requirement_name or "").strip().lower()
    if key in REQUIREMENT_IMPORT_OVERRIDES:
        return REQUIREMENT_IMPORT_OVERRIDES[key]
    return str(requirement_name or "").strip().replace("-", "_")


def _find_import_failures(requirement_names: Sequence[str]) -> List[Dict[str, str]]:
    failures: List[Dict[str, str]] = []
    for raw_name in requirement_names:
        req_name = str(raw_name or "").strip()
        if not req_name:
            continue
        module_name = _module_name_for_requirement(req_name)
        try:
            importlib.invalidate_caches()
            sys.modules.pop(module_name, None)
            importlib.import_module(module_name)
        except Exception as exc:
            failures.append(
                {
                    "requirement": req_name,
                    "module": module_name,
                    "error": f"{exc.__class__.__name__}: {exc}",
                }
            )
    return failures


def _install_requirements(
    requirements_path: Path,
    package_names: Optional[Sequence[str]] = None,
    force_reinstall: bool = False,
    python_executable: Optional[str] = None,
    allow_break_system_fallback: bool = True,
) -> None:
    python_cmd = str(python_executable or sys.executable)
    base_cmd = [
        python_cmd,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
    ]
    if force_reinstall:
        base_cmd += ["--upgrade", "--force-reinstall"]

    target_parts: List[str]
    if package_names:
        target_parts = [str(name) for name in package_names if str(name).strip()]
    else:
        target_parts = ["-r", str(requirements_path)]

    attempts = [base_cmd + target_parts]
    attempted_details: List[Dict[str, Any]] = []

    last_proc: Optional[subprocess.CompletedProcess[str]] = None
    for index, cmd in enumerate(attempts):
        _stderr(f"pip attempt #{index + 1}: {' '.join(cmd)}")
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        last_proc = proc
        attempt_item = {
            "command": cmd,
            "exit_code": proc.returncode,
            "stdout": _tail(proc.stdout or ""),
            "stderr": _tail(proc.stderr or ""),
        }
        attempted_details.append(attempt_item)
        if proc.returncode == 0:
            _stderr(f"pip attempt #{index + 1} succeeded")
            return

        stderr_text = (proc.stderr or "").lower()
        _stderr(
            f"pip attempt #{index + 1} failed with exit code {proc.returncode}"
        )
        _log_attempt_output(f"pip attempt #{index + 1} stdout tail", proc.stdout or "")
        _log_attempt_output(f"pip attempt #{index + 1} stderr tail", proc.stderr or "")
        if (
            allow_break_system_fallback
            and
            index == 0
            and "externally-managed-environment" in stderr_text
            and "--break-system-packages" not in " ".join(cmd)
        ):
            # Homebrew-managed Python / PEP-668: fall back to user install.
            _stderr(
                "Detected externally managed Python environment; retrying with "
                "--break-system-packages (system), then --user --break-system-packages."
            )
            attempts.append(base_cmd + ["--break-system-packages"] + target_parts)
            attempts.append(
                base_cmd + ["--user", "--break-system-packages"] + target_parts
            )

    if last_proc is None:
        raise RuntimePreflightError(
            code="REQUIREMENTS_INSTALL_FAILED",
            message="failed to install one or more Python requirements",
            details={
                "requirements_path": str(requirements_path),
                "exit_code": -1,
                "stdout": "",
                "stderr": "pip install did not execute",
                "attempts": attempted_details,
            },
        )

    raise RuntimePreflightError(
        code="REQUIREMENTS_INSTALL_FAILED",
        message="failed to install one or more Python requirements",
        details={
            "requirements_path": str(requirements_path),
            "package_names": list(package_names or []),
            "force_reinstall": bool(force_reinstall),
            "python_executable": python_cmd,
            "exit_code": last_proc.returncode,
            "stdout": _tail(last_proc.stdout or ""),
            "stderr": _tail(last_proc.stderr or ""),
            "attempts": attempted_details,
        },
    )


@contextmanager
def _preflight_lock(lock_path: Path):
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("w", encoding="utf-8") as lock_handle:
        _stderr(f"waiting for preflight lock: {lock_path}")
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
        _stderr("acquired preflight lock")
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            _stderr("released preflight lock")


def _ensure_venv(requirements_path: Path, min_python: Tuple[int, int]) -> str:
    venv_dir = _venv_dir_from_requirements_path(requirements_path)
    python_path = _venv_python_path(venv_dir)
    marker_path = venv_dir / ".stash_renamer_requirements.sha256"

    if not python_path.exists():
        _stderr(f"creating local venv at: {venv_dir}")
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

    if not python_path.exists():
        raise RuntimePreflightError(
            code="VENV_CREATE_FAILED",
            message="venv created but python executable was not found",
            details={"venv_dir": str(venv_dir), "expected_python": str(python_path)},
        )

    current_hash = _requirements_hash(requirements_path)
    previous_hash = ""
    if marker_path.exists():
        try:
            previous_hash = marker_path.read_text(encoding="utf-8").strip()
        except Exception:
            previous_hash = ""

    if previous_hash != current_hash:
        _stderr(f"installing requirements in venv using: {python_path}")
        _install_requirements(
            requirements_path=requirements_path,
            python_executable=str(python_path),
            allow_break_system_fallback=False,
        )
        marker_path.write_text(current_hash, encoding="utf-8")
    else:
        _stderr("venv requirements are up to date")

    proc = subprocess.run(
        [str(python_path), "-c", "import sys; print(sys.version)"],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimePreflightError(
            code="VENV_VALIDATION_FAILED",
            message="venv python validation failed",
            details={
                "venv_python": str(python_path),
                "exit_code": proc.returncode,
                "stdout": _tail(proc.stdout or ""),
                "stderr": _tail(proc.stderr or ""),
            },
        )

    # Ensure venv interpreter also satisfies minimum version.
    proc = subprocess.run(
        [
            str(python_path),
            "-c",
            (
                "import sys; "
                "ok=(sys.version_info.major, sys.version_info.minor)>="
                f"({int(min_python[0])}, {int(min_python[1])}); "
                "raise SystemExit(0 if ok else 1)"
            ),
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimePreflightError(
            code="PYTHON_VERSION_UNSUPPORTED",
            message="venv python does not meet minimum required version",
            details={
                "required": f"{int(min_python[0])}.{int(min_python[1])}+",
                "venv_python": str(python_path),
            },
        )

    return str(python_path)


def _reexec_into_venv(venv_python: str, venv_dir: str) -> None:
    _stderr(f"restarting process with venv python: {venv_python}")
    env = dict(os.environ)
    env[VENV_ACTIVE_ENV] = "1"
    env["VIRTUAL_ENV"] = venv_dir
    current_path = str(env.get("PATH") or "")
    venv_bin = str(Path(venv_dir) / ("Scripts" if os.name == "nt" else "bin"))
    env["PATH"] = f"{venv_bin}{os.pathsep}{current_path}" if current_path else venv_bin
    os.execve(venv_python, [venv_python, *sys.argv], env)


def run_preflight(
    requirements_path: Optional[Path] = None,
    min_python: Tuple[int, int] = MIN_PYTHON_VERSION,
) -> None:
    req_path = requirements_path or (Path(__file__).resolve().parent / "requirements.txt")
    lock_path = Path("/tmp/stash_renamer_runtime_preflight.lock")
    with _preflight_lock(lock_path):
        _stderr(f"starting preflight; python={sys.executable} version={sys.version}")
        _stderr(f"requirements path: {req_path}")
        _stderr(f"in_virtualenv: {_is_virtualenv_python()}")
        _stderr(f"usersite: {site.getusersitepackages()}")
        try:
            _stderr(f"sites: {site.getsitepackages()}")
        except Exception:
            _stderr("sites: unavailable")
        _stderr(f"sys.path entries: {len(sys.path)}")

        _check_python_version(min_python)

        if (
            not _is_virtualenv_python()
            and str(os.environ.get(VENV_ACTIVE_ENV) or "").strip() != "1"
        ):
            _stderr("not running inside virtualenv; bootstrapping local venv")
            venv_python = _ensure_venv(req_path, min_python)
            venv_dir = str(Path(venv_python).parent.parent)
            _reexec_into_venv(venv_python, venv_dir)

        requirement_names = _parse_requirement_names(req_path)
        _stderr(
            "parsed requirements: "
            + (", ".join(requirement_names) if requirement_names else "(none)")
        )
        missing = _find_missing_distributions(requirement_names)
        import_failures = _find_import_failures(requirement_names)

        if missing:
            _stderr("missing requirements: " + ", ".join(missing))
            _install_requirements(req_path)

        if import_failures:
            failed_req_names = [item["requirement"] for item in import_failures]
            _stderr(
                "requirements with import failures: "
                + ", ".join(
                    f"{item['requirement']} ({item['module']}: {item['error']})"
                    for item in import_failures
                )
            )
            _stderr(
                "attempting force reinstall for import-failed packages: "
                + ", ".join(failed_req_names)
            )
            _install_requirements(
                req_path,
                package_names=failed_req_names,
                force_reinstall=True,
            )

        still_missing = _find_missing_distributions(requirement_names)
        still_import_failures = _find_import_failures(requirement_names)
        if still_missing or still_import_failures:
            for item in still_import_failures:
                if (
                    "IsADirectoryError" in str(item.get("error") or "")
                    and "/etc/localtime" in str(item.get("error") or "")
                ):
                    _stderr(
                        "Detected import failure tied to /etc/localtime being a directory. "
                        "In Docker, mount /etc/localtime as a file (or remove that mount) "
                        "and ensure timezone files are valid."
                    )
            raise RuntimePreflightError(
                code="REQUIREMENTS_VALIDATION_FAILED",
                message="requirements installation completed but some dependencies are still unavailable",
                details={
                    "requirements_path": str(req_path),
                    "missing": still_missing,
                    "import_failures": still_import_failures,
                },
            )

        if not missing and not import_failures:
            _stderr("all requirements already installed and importable")
        _stderr("requirements installation validated successfully")


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
        _stderr("preflight failed")
        if isinstance(exc, RuntimePreflightError):
            _stderr(f"error code: {exc.code}")
            _stderr(f"message: {exc.message}")
            details = exc.details or {}
            for key in ["requirements_path", "required", "current", "python_executable"]:
                if key in details:
                    _stderr(f"{key}: {details[key]}")
            if "missing" in details:
                _stderr(f"missing: {details.get('missing')}")
            if "import_failures" in details:
                _stderr(f"import_failures: {details.get('import_failures')}")
            attempts = details.get("attempts")
            if isinstance(attempts, list):
                for idx, attempt in enumerate(attempts, start=1):
                    cmd = attempt.get("command")
                    code = attempt.get("exit_code")
                    _stderr(f"attempt[{idx}] exit_code={code} command={cmd}")
                    out = str(attempt.get("stdout") or "").strip()
                    err = str(attempt.get("stderr") or "").strip()
                    if out:
                        _stderr(f"attempt[{idx}] stdout tail:\n{out}")
                    if err:
                        _stderr(f"attempt[{idx}] stderr tail:\n{err}")
        else:
            _stderr(traceback.format_exc())

        payload = {
            "error": to_json_error(exc),
        }
        print(json.dumps(payload), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
