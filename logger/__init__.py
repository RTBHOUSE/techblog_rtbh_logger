import logging
import os

from logger.network import StructuredLogHandler
from logger.scope import LoggerScopeDecorator, NamedScopeDecorator, ScopeWithValueDecorator
from logger.sender import LocalLogSender

LoggerScopeDecorator.log_sender = LocalLogSender()

named_scope = NamedScopeDecorator
scope_with_value = ScopeWithValueDecorator


def add_default_handlers(logger):
    if os.getenv('RTBH_LOGGER_STDERR_DISABLED', '0') == '0':
        console_handler = logging.StreamHandler()
        logger.addHandler(console_handler)

    structured_handler = StructuredLogHandler(LoggerScopeDecorator.log_sender)
    logger.addHandler(structured_handler)


# Do not use this logger directly! Use get_rtbh_logger(__name__) instead.
internal_rtbh_logger = logging.getLogger("rtbh")


def setup_logging(level=logging.DEBUG):
    """
    Handles all loggers that start with "rtbh.*".
    """
    internal_rtbh_logger.setLevel(level)
    if not internal_rtbh_logger.handlers:
        add_default_handlers(internal_rtbh_logger)


def get_rtbh_logger(module_name: str):
    """
    Pass __name__ as the argument.
    """
    if not internal_rtbh_logger.handlers:
        setup_logging()

    return logging.getLogger(f"rtbh.{module_name}")
