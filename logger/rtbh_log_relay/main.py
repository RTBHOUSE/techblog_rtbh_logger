import datetime
import os
import threading

from logger.rtbh_log_relay import local_logger, setup_logger
from logger.rtbh_log_relay.forwarder import ParallelLogForwarder
from logger.rtbh_log_relay.server import LocalLogServer


class Sender:
    def __init__(self, server_address: str, forwarder: ParallelLogForwarder, check_socket: bool) -> None:
        self.server_address = server_address
        self.forwarder = forwarder
        self.check_socket = check_socket

    def send_forever(self) -> None:
        while True:
            start_time = datetime.datetime.utcnow()
            num_sent = self.forwarder.send_queued_entries()
            end_time = datetime.datetime.utcnow()
            duration_sec = (end_time - start_time).total_seconds()

            if num_sent > 0:
                local_logger.info("Sent %d messages in %.2f s. Bandwidth: %.1f msg/s. Num pending: %d", num_sent,
                                  duration_sec, num_sent / duration_sec, self.forwarder.received_event_ids.qsize())

            if self.check_socket and not os.path.exists(self.server_address):
                local_logger.warning("Socket file (%s) is missing", self.server_address)
                raise Exception("Socket file is missing")


def main():
    setup_logger()

    server_address = '/tmp/rtbh-log-relay.socket'

    local_logger.info("Started parallel log forwarder (%s)", server_address)
    forwarder = ParallelLogForwarder()

    try:
        forwarder.read_pending_events_from_db()

        server = LocalLogServer(server_address, forwarder)

        server_thread = threading.Thread(target=server.serve_forever)
        server_thread.daemon = True
        server_thread.start()

        Sender(server_address, forwarder, True).send_forever()
    except Exception:
        forwarder.work_done.set()
        raise


if __name__ == '__main__':
    main()
