from dataclasses import dataclass
from backend.services.file_mover import FileMover
from backend.services.graphql import GraphQLService
from backend.services.logger import LoggerService
from backend.services.template_service import TemplateService
from backend.services.GraphQLTagger import GraphQLTagger
from backend.services.undo_service import UndoService
from backend.services.watchdog_service import WatchdogService
from backend.services.ffmpeg_proxy_service import FFmpegProxyService
from backend.renamer.engine import RenamerEngine


@dataclass
class AppContext:
    gql: GraphQLService
    logger: LoggerService
    mover: FileMover
    tagger: GraphQLTagger
    templates: TemplateService
    undo: UndoService
    engine: RenamerEngine
    watchdog: WatchdogService
    ffmpeg_proxy: FFmpegProxyService
    collect_operations: bool
    debug_mode: bool
    dry_run: bool
