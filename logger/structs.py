import json
from typing import List, NamedTuple, Optional, Union

JobName = str
BuildId = str
Hostname = str
ProcessName = str
Pid = int
TimeStamp = float
ThreadId = int
Uid = str
LogLevel = str
FilePath = str


class LogicalScope(NamedTuple):
    job_name: JobName
    build_id: BuildId
    uid: Uid

    name: str
    value: Optional[str]
    start_time: TimeStamp

    def to_dict(self):
        return self._asdict()  # pylint: disable=no-member


class ThreadDescription(NamedTuple):
    job_name: JobName
    build_id: BuildId
    hostname: Hostname
    process_name: ProcessName
    pid: Pid
    thread_id: ThreadId
    uid: Uid

    def to_dict(self):
        return self._asdict()  # pylint: disable=no-member


class ScopeStartMessage(NamedTuple):
    job_name: JobName
    build_id: BuildId
    uid: Uid

    scope_path: List[LogicalScope]

    def to_dict(self):
        return dict(
            job_name=self.job_name,
            build_id=self.build_id,
            uid=self.uid,
            scope_path=[s.to_dict() for s in self.scope_path],
        )


class ScopeEndMessage(NamedTuple):
    job_name: JobName
    build_id: BuildId
    uid: Uid

    end_time: TimeStamp

    def to_dict(self):
        return dict(
            job_name=self.job_name,
            build_id=self.build_id,
            uid=self.uid,
            end_time=self.end_time
        )


ThreadStartMessage = ThreadDescription


class LogEntryMessage(NamedTuple):
    thread_id: Uid
    scope_id: Uid

    timestamp: TimeStamp
    level: LogLevel
    file: FilePath
    line: int

    message: str
    args: Union[dict, list]

    def to_dict(self):
        args = self.args
        try:
            json.dumps(self.args)
        except Exception:  # failed to serialize argument to JSON
            args = str(self.args)

        return dict(
            thread_id=self.thread_id,
            scope_id=self.scope_id,
            timestamp=self.timestamp,
            level=self.level,
            file=self.file,
            line=self.line,
            message=self.message,
            args=args
        )


LogSystemMessage = Union[ScopeStartMessage, ScopeEndMessage, LogEntryMessage, ThreadStartMessage]


class LogSender:
    """See LocalLogSender."""
    def send_entry(self, log_entry: LogEntryMessage):
        pass

    def send_entries(self, log_entries: List[LogSystemMessage]):
        for entry in log_entries:
            self.send_entry(entry)
