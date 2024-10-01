import datetime
import json
import logging
import os
import socket
import struct
import sys
import threading
import time

from logger.structs import LogEntryMessage, LogSender, LogSystemMessage

logger = logging.getLogger(__name__)


class LocalLogSender(LogSender):
    """
    Sends Log System messages through unix domain socket to local Log Relay daemon.

    We assume that unix domain socket is a very reliable connection and that (in case of some failure) systemd will restart
    Log Relay daemon promptly, so this sending message to Log Relay blocks until message is delivered to LogForwarder.
    """

    def __init__(self, server_address='/tmp/rtbh-log-relay.socket'):
        self.pid = None
        self.server_address = server_address
        self.client_socket = None
        self.mutex = threading.Lock()

    def send_entry(self, log_entry: LogSystemMessage):
        self.send_entry_internal(log_entry)

    def send_entry_internal(self, log_entry: LogSystemMessage):
        num_errors = 0
        while True:
            try:
                self.send(log_entry)
                break

            except Exception as e:
                num_errors += 1
                if num_errors & (num_errors - 1) == 0:
                    sys.stderr.write("%s Failed to send log entry through %s (#errors=%s). Error: %s\n"
                                     % (datetime.datetime.utcnow(), self.server_address, num_errors, e))
                time.sleep(1)
                try:
                    self.connect()
                except:  # pylint: disable=bare-except
                    pass

    def connect(self):
        with self.mutex:
            self.pid = os.getpid()
            self.client_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            self.client_socket.connect(self.server_address)

    def send(self, log_entry: LogEntryMessage):
        if os.getpid() != self.pid:  # forked or not connected
            self.connect()

        with self.mutex:
            json_bytes = json.dumps(log_entry.to_dict()).encode('utf8')
            data_size_bytes = struct.pack('<i', -len(json_bytes))
            proto_version_bytes = struct.pack('<i', 2)
            self.client_socket.sendall(data_size_bytes)
            self.client_socket.sendall(proto_version_bytes)
            self.client_socket.sendall(json_bytes)
            ack = self.client_socket.recv(1)
            if ack != bytes([0x55]):
                raise ValueError("Unexpected ACK: %s" % (ack, ))
