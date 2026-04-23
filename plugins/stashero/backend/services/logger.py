from typing import Optional

USING_STASH_LOG = False
stash_log = None

try:
    import backend.services.stash_log as stash_log  # type: ignore

    USING_STASH_LOG = True
except ImportError:
    try:
        import log as stash_log  # type: ignore

        USING_STASH_LOG = True
    except ImportError:
        pass


class LoggerService:
    def __init__(self, debug_mode: bool = True):
        self.debug_mode = debug_mode

    def emit_progress(self, progress: float) -> None:
        if not (USING_STASH_LOG and stash_log):
            return
        try:
            stash_log.LogProgress(progress)
        except Exception:
            pass

    def trace(self, msg: str) -> None:
        if not msg:
            return
        if USING_STASH_LOG and stash_log:
            stash_log.LogTrace(msg)
        else:
            print(msg)

    def debug(self, msg: str) -> None:
        if not self.debug_mode:
            return
        if not msg:
            return
        if USING_STASH_LOG and stash_log:
            stash_log.LogDebug(msg)
        else:
            print(msg)

    def info(self, msg: str) -> None:
        if not msg:
            return
        if USING_STASH_LOG and stash_log:
            stash_log.LogInfo(msg)
        else:
            print(msg)

    def warning(self, msg: str) -> None:
        if not msg:
            return
        if USING_STASH_LOG and stash_log:
            stash_log.LogWarning(msg)
        else:
            print(msg)

    def error(self, msg: str) -> None:
        if not msg:
            return
        if USING_STASH_LOG and stash_log:
            stash_log.LogError(msg)
        else:
            print(msg)
