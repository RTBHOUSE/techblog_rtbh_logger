import logging

from logger.scope import create_log_entry
from logger.structs import LogSender


class StructuredLogHandler(logging.Handler):
    """
    Sends structured logs using LogSender.

    Usually this means that structured logs are sent to local Log Relay and then to central log database.
    """

    def __init__(self, log_sender: LogSender):
        super().__init__()
        self.log_sender = log_sender

    def emit(self, record: logging.LogRecord):
        log_entry = create_log_entry(file=record.filename, line=record.lineno, level=record.levelname,
                                     message=record.getMessage(), args=record.args, exc_info=record.exc_info)
        self.log_sender.send_entries(log_entry)
