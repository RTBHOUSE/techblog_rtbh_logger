# Used to send logs over network to the arango db.
# Uses multiple processes for performance.
# One process transfers ~40 entries/second.
import json
import multiprocessing as mp
import queue
from typing import Dict, NamedTuple, Optional

from arango import ArangoClient, DocumentInsertError

__all__ = ["arango_sender_thread", "SendRequest", "SendResult"]

from logger.rtbh_log_relay import local_logger


class SendRequest(NamedTuple):
    entry_id: bytes
    entry: bytes


class SendResult(NamedTuple):
    entry_id: bytes
    exception: Optional[Exception]  # 'None' means success


class ArangoParallelLogSender:
    """
    Dispatches LogSystem messages to appropriate ArangoDB collections.
    """

    def __init__(self, work_queue: mp.Queue, result_queue: mp.Queue, work_done: mp.Event):
        local_logger.info("Log sender started!")

        self.client = ArangoClient(hosts='http://arango-central-db.example:9966')
        self.logger_db = self.client.db('logging')

        self.scope_starts = self.logger_db.collection('scope_starts')
        self.scope_ends = self.logger_db.collection('scope_ends')
        self.threads = self.logger_db.collection('threads')
        self.messages = self.logger_db.collection('messages')
        self.qa_traces = self.logger_db.collection('qa_traces')

        self.work_queue = work_queue
        self.result_queue = result_queue
        self.work_done = work_done

    def send(self, entry_id: bytes, entry: bytes):
        entry_dict = self.create_message(entry, entry_id)
        if not entry_dict:
            return

        try:
            self.send_message_ignoring_duplicates(entry_dict)
        except DocumentInsertError as e:
            if e.error_code == 600:  # TODO XXX handle list with NaNs
                local_logger.warning("Invalid message (NaNs?). Trying to fixing it. entry_id=%s ", entry_id)
                self.stringify_arguments(entry_dict)
                self.send_message_ignoring_duplicates(entry_dict)
            else:
                raise

    def send_message_ignoring_duplicates(self, entry_dict: Dict) -> None:
        try:
            self.dispatch_message(entry_dict)
        except DocumentInsertError as e:
            if e.error_code == 1210:
                local_logger.warning("Entry %s already inserted. Ignoring.", entry_dict["_key"])
            else:
                raise

    def stringify_arguments(self, entry_dict: Dict) -> None:
        """
        Workaround json exceptions when nans are present in arguments list.

        Only called if the message fails to serialize with non-stringified arguments.
        """
        if 'message' in entry_dict and 'args' in entry_dict:
            entry_dict['args'] = str(entry_dict['args'])

    def create_message(self, entry: bytes, entry_id: bytes) -> Optional[dict]:
        entry_id_str = entry_id.decode('ascii')

        try:
            entry_dict = json.loads(entry.decode('utf8'))
        except json.decoder.JSONDecodeError:
            local_logger.exception("Failed to decode message (id=%s): %s. Skipping it", entry_id_str, entry)
            return None
        entry_dict['_key'] = entry_id_str

        return entry_dict

    def dispatch_message(self, entry_dict: dict):
        if 'message' in entry_dict:
            self.messages.insert(entry_dict, silent=True)
        elif 'scope_path' in entry_dict:
            self.scope_starts.insert(entry_dict, silent=True)
        elif 'end_time' in entry_dict:
            self.scope_ends.insert(entry_dict, silent=True)
        elif 'qa_trace_version' in entry_dict:
            self.qa_traces.insert(entry_dict, silent=True)
        else:
            assert 'thread_id' in entry_dict
            self.threads.insert(entry_dict, silent=True)

    def handle_request_get_result(self, request: SendRequest) -> SendResult:
        try:
            self.send(request.entry_id, request.entry)
            return SendResult(request.entry_id, exception=None)
        except Exception as ex:
            local_logger.warning("Send worker failed to process request")
            return SendResult(request.entry_id, exception=ex)

    def serve_forever(self):
        while not self.work_done.is_set():
            try:
                request = self.work_queue.get(timeout=1)
            except queue.Empty:
                continue

            result = self.handle_request_get_result(request)
            self.result_queue.put(result)

        local_logger.info("Arango sender finished cleanly.")


def arango_sender_thread(work_queue: mp.Queue, result_queue: mp.Queue, work_done: mp.Event):
    ArangoParallelLogSender(work_queue, result_queue, work_done).serve_forever()
