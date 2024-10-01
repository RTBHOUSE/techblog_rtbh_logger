import inspect
import logging
import os
import socket
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from typing import List, Optional, Tuple, TypeVar, Union

from logger.structs import (
    FilePath,
    LogEntryMessage,
    LogicalScope,
    LogLevel,
    LogSender,
    LogSystemMessage,
    ScopeEndMessage,
    ScopeStartMessage,
    ThreadDescription,
    Uid,
)

T = TypeVar("T")


class LoggerThreadLocal(threading.local):
    def __init__(self, unsafe_process_scope_id: Optional[str] = None):  # pylint: disable=super-init-not-called
        """
        :param unsafe_process_scope_id: Replaces parent scope with this value. Do not use unless you know EXACTLY what
        you are doing.
        """
        process_name = sys.argv[0][::-1].replace("yp.", "")[::-1]
        hostname = socket.gethostname()

        # These env variables must be set by the user. They are used to efficiently
        # query all log entries that belong to a specific job_name/build_id.
        job_name = os.environ["RTBH_JOB_NAME"]
        build_id = os.environ["RTBH_BUILD_ID"]

        self.__dict__['thread_desc'] = ThreadDescription(job_name=job_name,
                                                         build_id=build_id,
                                                         hostname=hostname,
                                                         process_name=process_name,
                                                         pid=os.getpid(),
                                                         thread_id=threading.get_ident(),
                                                         uid=str(uuid.uuid4()))
        self.__dict__['thread_desc_sent'] = False
        logical_scopes = []
        process_scope_id = unsafe_process_scope_id or os.getenv("RTBH_LOGGER_SCOPE_ID")
        if process_scope_id:
            logical_scopes.append(LogicalScope(job_name=job_name, build_id=build_id,
                                               uid=process_scope_id, name='<inherited>', value=None, start_time=time.time()))
        self.__dict__['logical_scopes'] = logical_scopes


_logger_context = LoggerThreadLocal()


class LoggerScopeDecorator:
    # pylint: disable=no-member
    """
    Calling decorated function will create new logger scope, nested inside parent scope.
    """
    log_sender: LogSender = None

    def __init__(self, name=None, key=None):
        self.name = name
        self.key = key

    def __call__(self, fun: T) -> T:
        if self.name is None:
            self.name = fun.__name__

        def wrapped_f(*args, **kwargs):
            if self.key:
                bound_args = inspect.signature(fun).bind(*args, **kwargs)
                bound_args.apply_defaults()
                key_value = str(bound_args.arguments[self.key])
            else:
                key_value = None

            LoggerScopeDecorator.enter_scope(self.name, key_value)
            try:
                result = fun(*args, **kwargs)
            finally:
                LoggerScopeDecorator.leave_scope()
            return result

        return wrapped_f

    @staticmethod
    def enter_scope(scope_name, key_value):
        uid = str(uuid.uuid4())
        thread_desc = _logger_context.thread_desc
        scope = LogicalScope(job_name=thread_desc.job_name, build_id=thread_desc.build_id,
                             uid=uid, name=scope_name, value=key_value, start_time=time.time())
        _logger_context.logical_scopes.append(scope)
        LoggerScopeDecorator.log_sender.send_entries(create_scope_start_message())

    @staticmethod
    def leave_scope():
        LoggerScopeDecorator.log_sender.send_entries(create_scope_end_message())
        _logger_context.logical_scopes.pop()


@contextmanager
def manual_scope(scope_name: str, scope_value=None):
    """Contextmanager that behaves like LoggerScopeDecorator."""

    key_value = str(scope_value) if scope_value is not None else None
    LoggerScopeDecorator.enter_scope(scope_name, key_value)
    try:
        yield
    finally:
        LoggerScopeDecorator.leave_scope()


class ScopeWithValueDecorator(LoggerScopeDecorator):
    def __init__(self, value=None):
        """Uses argument named `value` as a scope value."""
        LoggerScopeDecorator.__init__(self, None, value)


class NamedScopeDecorator(LoggerScopeDecorator):
    def __init__(self, name, value=None):
        LoggerScopeDecorator.__init__(self, name, value)


def new_scope(fun: T) -> T:
    def wrapped_f(*args, **kwargs):
        return LoggerScopeDecorator(None)(fun)(*args, **kwargs)

    return wrapped_f


def get_context() -> Tuple[bool, List[LogicalScope], ThreadDescription]:
    # pylint: disable=no-member
    pid = os.getpid()
    thread_desc = _logger_context.thread_desc
    logical_scopes = _logger_context.logical_scopes

    if pid != thread_desc.pid:
        thread_desc = LoggerThreadLocal().thread_desc
        _logger_context.__dict__['thread_desc'] = thread_desc
        _logger_context.__dict__['thread_desc_sent'] = False

    thread_desc_outdated = not _logger_context.__dict__['thread_desc_sent']

    if thread_desc_outdated:
        _logger_context.__dict__['thread_desc_sent'] = True

    return thread_desc_outdated, logical_scopes, thread_desc


def get_current_scope_id() -> Optional[Uid]:
    # pylint: disable=no-member
    logical_scopes = _logger_context.logical_scopes
    if logical_scopes:
        return logical_scopes[-1].uid
    return None


def maybe_add_exc_text_to_args_and_msg(
        exc_info: Optional[Union[Tuple, BaseException]],
        message: str,
        args: Union[tuple, dict]) -> Tuple[str, Union[tuple, dict]]:
    """Returns a tuple (message_with_exception_text, args_with_exception_text).
    If exc_info is None, returns original (message, args).
    """
    if not exc_info:
        return message, args
    if not isinstance(exc_info, tuple):
        exc_info = (type(exc_info), exc_info, exc_info.__traceback__)
    exc_text = logging.Formatter().formatException(exc_info)
    message = f'{message}\n{exc_text}'
    if isinstance(args, (tuple, list)):
        return message, (tuple(args) + tuple([exc_text]))
    else:
        result = dict(args or {})
        result.update(dict(__exc_text=exc_text))
        return message, result


def create_log_entry(
        level: LogLevel,
        file: FilePath,
        line: int,
        message: Optional[str],
        args: Union[tuple, dict],
        exc_info: Optional[BaseException]) -> List[LogSystemMessage]:

    time_now = time.time()
    thread_desc_outdated, logical_scopes, thread_desc = get_context()
    message, args = maybe_add_exc_text_to_args_and_msg(exc_info, message, args)

    log_entry = LogEntryMessage(
        thread_id=thread_desc.uid,
        scope_id=logical_scopes[-1].uid if logical_scopes else None,
        timestamp=time_now,
        message=message,
        level=level,
        file=file,
        line=line,
        args=args)

    if thread_desc_outdated:
        return [thread_desc, log_entry]

    return [log_entry]


def create_scope_start_message() -> List[LogSystemMessage]:
    thread_desc_outdated, logical_scopes, thread_desc = get_context()
    scope_message = ScopeStartMessage(uid=logical_scopes[-1].uid, scope_path=logical_scopes,
                                      job_name=thread_desc.job_name, build_id=thread_desc.build_id)

    if thread_desc_outdated:
        return [thread_desc, scope_message]

    return [scope_message]


def create_scope_end_message() -> List[LogSystemMessage]:
    time_now = time.time()
    thread_desc_outdated, logical_scopes, thread_desc = get_context()
    scope_message = ScopeEndMessage(uid=logical_scopes[-1].uid, end_time=time_now,
                                    job_name=thread_desc.job_name, build_id=thread_desc.build_id)

    if thread_desc_outdated:
        return [thread_desc, scope_message]

    return [scope_message]
