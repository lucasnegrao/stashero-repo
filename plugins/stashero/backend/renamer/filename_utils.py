import os
import re
from typing import Callable, Dict, List


def sanitize_filename(name: str) -> str:
    if not name:
        return ""
    name = "".join(char for char in name if ord(char) >= 32 and ord(char) != 127)
    name = re.sub(r"[<>:/\\|?*`,]+", "", name)
    name = re.sub(r"&", "and", name)
    name = re.sub(r"(?<=\\w)'(?=\\w)|(?<=\\w)'(?=\\s|$)", "", name)
    name = re.sub(r"\s{2,}", " ", name)
    name = re.sub(r"(?<!\.)\.{3}(?!\.)", ".", name)
    return name.strip(". ")


def sanitize_path_component(seg: str) -> str:
    return sanitize_filename(seg or "")


def apply_extension_if_missing(filename_core: str, file_extension: str) -> str:
    core = str(filename_core or "")
    ext = str(file_extension or "")
    if not ext:
        return core
    if core.lower().endswith(ext.lower()):
        return core
    return core + ext


def build_target_directory(
    current_directory: str,
    tag_context: Dict[str, object],
    path_template: str,
    make_filename: Callable[[str, Dict[str, object]], str],
) -> str:
    raw = str(path_template or "")
    is_absolute = raw.startswith("/") or raw.startswith("\\")
    replaced = make_filename(raw, tag_context)
    replaced = re.sub(r"[\\/]+", "/", replaced).strip()
    parts = [p.strip() for p in replaced.split("/")]

    if is_absolute:
        stack: List[str] = []
        for seg in parts:
            if not seg or seg == ".":
                continue
            if seg == "..":
                if stack:
                    stack.pop()
                continue
            stack.append(sanitize_path_component(seg))
        return os.sep + os.path.join(*stack) if stack else os.sep

    base = current_directory
    for seg in parts:
        if not seg or seg == ".":
            continue
        if seg == "..":
            parent = os.path.dirname(base.rstrip(os.sep)) or base
            base = parent if parent else base
        else:
            base = os.path.join(base, sanitize_path_component(seg))
    return base


def make_filename(
    query: str,
    tag_context: Dict[str, object],
    tag_render: Callable[[str, Dict[str, object]], str],
) -> str:
    s = tag_render(str(query or ""), tag_context)
    s = re.sub(r"(?:\s*-\s*){2,}", " - ", s)
    s = re.sub(r"^\s*[-–—_:|,]+\s*", "", s)
    s = re.sub(r"\s*[-–—_:|,]+\s*$", "", s)
    s = re.sub(r"\[\W*\]", "", s)
    s = re.sub(r"\(\W*\)", "", s)
    s = re.sub(r"\{\W*\}", "", s)
    return re.sub(r"\s{2,}", " ", s).strip()


def shorten_filename(name: str, max_len: int) -> str:
    if len(name) <= max_len:
        return name

    words = name.split()
    deduped = []
    for w in words:
        if not deduped or deduped[-1].lower() != w.lower():
            deduped.append(w)

    reduced = " ".join(deduped)
    if len(reduced) > max_len:
        reduced = reduced[:max_len]

    reduced = re.sub(r"\s*[-–—_:|,]+\s*$", "", reduced).strip()
    return reduced
