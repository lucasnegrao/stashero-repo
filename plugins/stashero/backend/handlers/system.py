from typing import Any, Dict
from backend.handlers.context import AppContext


def handle_list_selectors(options: Dict[str, Any], ctx: AppContext):
    return ctx.tagger.build_selectors_catalog()


def handle_ffmpeg_proxy_enable(options: Dict[str, Any], ctx: AppContext):
    return {"ffmpeg_proxy": ctx.ffmpeg_proxy.enable(options)}


def handle_ffmpeg_proxy_reverse(options: Dict[str, Any], ctx: AppContext):
    return {"ffmpeg_proxy": ctx.ffmpeg_proxy.reverse(options)}
