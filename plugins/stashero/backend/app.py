from typing import Any, Dict
from backend.renamer.engine import RenamerEngine
from backend.services.file_mover import FileMover
from backend.services.graphql import GraphQLConfig, GraphQLService
from backend.services.logger import LoggerService
from backend.services.template_service import TemplateService
from backend.services.GraphQLTagger import GraphQLTagger
from backend.services.undo_service import UndoService
from backend.services.watchdog_service import WatchdogService
from backend.services.ffmpeg_proxy_service import FFmpegProxyService
from backend.handlers.context import AppContext
from backend.handlers.router import ROUTES
from backend.handlers.utils import to_bool


def run(options: dict, collect_operations: bool = False):
    server_url = options.get("server_url")
    cookie_name = options.get("cookie_name")
    cookie_value = options.get("cookie_value")
    if not server_url or not cookie_name or not cookie_value:
        raise ValueError(
            "server_url, cookie_name, and cookie_value are required in options"
        )

    mode = str(options.get("mode") or "").strip().lower()
    if not mode:
        raise ValueError("mode is required")

    using_log = to_bool(options.get("using_log", True))
    dry_run = to_bool(options.get("dry_run", False))
    debug_mode = to_bool(options.get("debug_mode", True))
    logger = LoggerService(debug_mode=debug_mode)
    logger.info(f"Running in {mode} mode, options: {options}")
    runtime_python_path = str(
        options.get("python_path") or options.get("pythonPath") or ""
    ).strip()

    gql_config = GraphQLConfig(
        server_url=str(server_url),
        cookie_name=str(cookie_name),
        cookie_value=str(cookie_value),
    )
    watchdog = WatchdogService(
        db_path=str(options.get("operations_db_path") or "rename_operations.db"),
        gql_config=gql_config,
        log_print=logger.debug if using_log else (lambda _msg: None),
        python_executable=runtime_python_path or None,
    )

    gql = GraphQLService(gql_config)
    ffmpeg_proxy = FFmpegProxyService(
        gql_call=gql.call,
        log_print=logger.debug if using_log else (lambda _msg: None),
        python_executable=runtime_python_path or None,
    )

    mover = FileMover(
        gql_call=gql.call,
        db_path=str(options.get("operations_db_path") or "rename_operations.db"),
        log_print=logger.debug if using_log else (lambda _msg: None),
        emit_progress=logger.emit_progress,
    )

    tagger = GraphQLTagger(
        gql_call=gql.call,
        root_types={"scene": "Scene", "group": "Group", "performer": "Performer"},
    )
    try:
        tagger.introspect()
        logger.debug(f"Tagger ready with roots: {', '.join(tagger.available_roots())}")
    except Exception as e:
        logger.warning(
            f"GraphQL introspection failed, template tags may be incomplete: {e}"
        )

    templates = TemplateService(mover)
    undo = UndoService(mover)
    engine = RenamerEngine(
        gql_call=gql.call,
        tagger=tagger,
        mover=mover,
        logger=logger,
        dry_run=dry_run,
        debug_mode=debug_mode,
        options=options,
    )

    ctx = AppContext(
        gql=gql,
        logger=logger,
        mover=mover,
        tagger=tagger,
        templates=templates,
        undo=undo,
        engine=engine,
        watchdog=watchdog,
        ffmpeg_proxy=ffmpeg_proxy,
        collect_operations=collect_operations,
        debug_mode=debug_mode,
        dry_run=dry_run,
    )

    handler = ROUTES.get(mode)
    if not handler:
        mover.flush()
        mover.close()
        watchdog.close()
        raise ValueError(f"Unsupported mode: {mode}")

    try:
        logger.emit_progress(0.0)
        return handler(options, ctx)
    finally:
        mover.flush()
        mover.close()
        watchdog.close()
