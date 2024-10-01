import multiprocessing as mp
import queue
import threading
from typing import List

import rocksdb

from logger.rtbh_log_relay import local_logger
from logger.rtbh_log_relay.parallel_sender import SendRequest, SendResult, arango_sender_thread
from logger.rtbh_log_relay.uid import Uid


class ParallelLogForwarder:
    """
    Forwards LogSystem messages received on unix domain socket to central database (Arango DB).

    It writes messages to local RocksDB (that acts as a persistent queue) before forwarding a message.
    This way, in case of network failures, Arango DB temporary problems, etc. unhandled exception is raised
    and log relay exits. Systemd restarts it immediately and log forwarder sends queued messages.

    Forwarding is not perfect (yet) but it's quite reliable. In case of failure that occurs after receiving a message from
    LocalLogSender and writing message to persistent queue, message is lost. In very rare cases log messages can be duplicated.
    """

    def __init__(self, num_send_workers: int = 8):
        self.db = rocksdb.DB("/tmp/rtbh-log-relay.db", rocksdb.Options(create_if_missing=True))

        self.id_prefix = Uid.generate_short_uid() + b'-'
        self.seq_no = 0

        self.received_event_ids = queue.Queue()
        self.seq_lock = threading.Lock()

        self.entries_send_queue: mp.Queue = mp.Queue()
        self.entries_send_results_queue: mp.Queue = mp.Queue()
        self.work_done: mp.Event = mp.Event()

        self.num_send_workers: int = num_send_workers
        self.send_workers = [
            mp.Process(
                target=arango_sender_thread,
                name="log-relay-sender-%d" % idx,
                args=(self.entries_send_queue, self.entries_send_results_queue, self.work_done)
            )
            for idx in range(num_send_workers)
        ]
        for p in self.send_workers:
            p.start()

    def handle_worker_failures(self):
        failed_workers = []
        for worker in self.send_workers:
            if not worker.is_alive():
                failed_workers.append(worker)

        if failed_workers:
            self.work_done.set()
            for worker in self.send_workers:
                worker.join()

            raise ValueError("Some send workers were killed before the end of task: %s." % (failed_workers,))

    def read_pending_events_from_db(self):
        iterator = self.db.iteritems()
        iterator.seek_to_first()

        for entry_id, _ in iterator:
            self.received_event_ids.put(entry_id)

    def generate_id(self):
        with self.seq_lock:
            self.seq_no += 1
            seq_id = self.seq_no

        return self.id_prefix + Uid.int_base_62(seq_id, 11)

    def entry_received(self, data: bytes):
        entry_id = self.generate_id()
        self.db.put(entry_id, data)
        self.received_event_ids.put(entry_id)

    def get_n_entry_ids(self, n: int) -> List:
        result = []
        for _ in range(n):
            try:
                result.append(self.received_event_ids.get(timeout=0.1))
            except queue.Empty:
                break
        return result

    def get_send_result(self) -> SendResult:
        while True:
            self.handle_worker_failures()
            try:
                return self.entries_send_results_queue.get(timeout=1)
            except queue.Empty:
                local_logger.info("No results available yet, waiting...")

    def send_queued_entries(self):
        num_sent = 0

        for _ in range(100):
            num_sent_now = self.send_queued_entries_batch()
            if num_sent_now == 0:
                break
            num_sent += num_sent_now

        return num_sent

    def send_queued_entries_batch(self):
        entry_ids = self.get_n_entry_ids(self.num_send_workers)

        # Submit send requests
        for entry_id in entry_ids:
            self.entries_send_queue.put(SendRequest(entry_id, self.db.get(entry_id)))

        # Await send results
        errors = []
        for _ in range(len(entry_ids)):
            send_result = self.get_send_result()
            if send_result.exception is not None:
                errors.append(send_result.exception)
                self.received_event_ids.put(send_result.entry_id)
            else:
                self.db.delete(send_result.entry_id)

        if errors:
            local_logger.error("Errors while sending: %s", errors)
            raise errors[0]

        return len(entry_ids)
