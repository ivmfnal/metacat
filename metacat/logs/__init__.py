from .logs import Logged, Logger, AbstractLogger, init
from .log_file import LogFile, LogStream

init_logger = init     # for backward compatibility