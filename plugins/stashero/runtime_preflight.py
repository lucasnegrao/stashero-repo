#!/usr/bin/env python3
"""Runtime preflight checks for Scene Renamer.

This module validates the Python runtime and dependency installation before the
plugin bootstraps backend imports.
"""

from __future__ import annotations

import importlib.metadata
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

MIN_PYTHON_VERSION: Tuple[int, int] = (3, 9)


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


def _install_requirements(requirements_path: Path) -> None:
    base_cmd = [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--disable-pip-version-check",
    ]
    attempts = [
        base_cmd + ["-r", str(requirements_path)],
    ]

    last_proc: Optional[subprocess.CompletedProcess[str]] = None
    for index, cmd in enumerate(attempts):
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
        )
        last_proc = proc
        if proc.returncode == 0:
            return

        stderr_text = (proc.stderr or "").lower()
        if (
            index == 0
            and "externally-managed-environment" in stderr_text
            and "--break-system-packages" not in cmd
        ):
            # Homebrew-managed Python / PEP-668: fall back to user install.
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
            },
        )

    raise RuntimePreflightError(
        code="REQUIREMENTS_INSTALL_FAILED",
        message="failed to install one or more Python requirements",
        details={
            "requirements_path": str(requirements_path),
            "exit_code": last_proc.returncode,
            "stdout": (last_proc.stdout or "")[-4000:],
            "stderr": (last_proc.stderr or "")[-4000:],
        },
    )


def run_preflight(
    requirements_path: Optional[Path] = None,
    min_python: Tuple[int, int] = MIN_PYTHON_VERSION,
) -> None:
    req_path = requirements_path or (Path(__file__).resolve().parent / "requirements.txt")

    _check_python_version(min_python)

    requirement_names = _parse_requirement_names(req_path)
    missing = _find_missing_distributions(requirement_names)
    if not missing:
        return

    _install_requirements(req_path)

    still_missing = _find_missing_distributions(requirement_names)
    if still_missing:
        raise RuntimePreflightError(
            code="REQUIREMENTS_VALIDATION_FAILED",
            message="requirements installation completed but some dependencies are still unavailable",
            details={
                "requirements_path": str(req_path),
                "missing": still_missing,
            },
        )


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
        payload = {
            "error": to_json_error(exc),
        }
        print(json.dumps(payload), flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
