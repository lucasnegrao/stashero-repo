#!/usr/bin/env python3
"""Runtime preflight checks for Scene Renamer.

This module validates the Python runtime and dependency installation before the
plugin bootstraps backend imports.
"""

from __future__ import annotations

import importlib.metadata
import importlib
import json
import re
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

MIN_PYTHON_VERSION: Tuple[int, int] = (3, 9)
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


def _stderr(message: str) -> None:
    print(f"[runtime_preflight] {message}", file=sys.stderr, flush=True)


def _tail(text: str, limit: int = 4000) -> str:
    if len(text) <= limit:
        return text
    return text[-limit:]


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
) -> None:
    base_cmd = [
        sys.executable,
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
        if (
            index == 0
            and "externally-managed-environment" in stderr_text
            and "--break-system-packages" not in cmd
        ):
            # Homebrew-managed Python / PEP-668: fall back to user install.
            _stderr(
                "Detected externally managed Python environment; retrying with "
                "--user --break-system-packages."
            )
            attempts.append(
                base_cmd
                + ["--user", "--break-system-packages", "-r", str(requirements_path)]
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
            "exit_code": last_proc.returncode,
            "stdout": _tail(last_proc.stdout or ""),
            "stderr": _tail(last_proc.stderr or ""),
            "attempts": attempted_details,
        },
    )


def run_preflight(
    requirements_path: Optional[Path] = None,
    min_python: Tuple[int, int] = MIN_PYTHON_VERSION,
) -> None:
    req_path = requirements_path or (Path(__file__).resolve().parent / "requirements.txt")
    _stderr(f"starting preflight; python={sys.executable} version={sys.version}")
    _stderr(f"requirements path: {req_path}")

    _check_python_version(min_python)

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
