import enum
import os
import socketserver
import struct
from typing import NamedTuple, Optional

from logger.rtbh_log_relay import local_logger
from logger.rtbh_log_relay.forwarder import ParallelLogForwarder

V2_ACK_BYTES = bytes([0x55])


class ProtocolVersion(enum.Enum):
    v2 = 2


class RequestHandler(socketserver.BaseRequestHandler):
    """
    Reads messages sent via unix domain socket.

    Format is:
        - message size (4 bytes int, little endian, negative),
        - protocol version (4 bytes int, little endian),
        - protocol-specific data.
        Version-specific replies might be sent.
    Positive size indicates legacy protocol whereas negative size indicates
    new version of the protocol.

    Protocol version v2 (ProtocolVersion.v2):
        - message size (4 bytes int, little endian, negative),
        - protocol version (4 bytes int, little endian, equals 2),
        - message body.
        Each message is acknowledged by sending a single byte reply with value
        0x55.
    """

    class Frame(NamedTuple):
        data: bytes
        proto_version: ProtocolVersion

    def handle(self):
        while True:
            frame = self.read_frame()
            if frame is None:
                break
            entry, proto_version = frame
            assert proto_version == ProtocolVersion.v2
            self.server.forwarder.entry_received(entry)
            self.ack_frame_v2()

    def read_frame(self) -> Optional['RequestHandler.Frame']:
        """
        Returns a tuple (message_body, protocol_version) or None if the connection
        was closed.
        """
        size_buffer = self.read_buffer(4)
        if size_buffer is None:
            return None
        data_size = struct.unpack('<i', size_buffer)[0]
        assert data_size != 0

        return self.read_body_v2(data_size_negative=data_size)

    def read_body_v2(self, data_size_negative: int) -> Optional['RequestHandler.Frame']:
        data_size = -data_size_negative

        proto_version_buffer = self.read_buffer(4)
        if proto_version_buffer is None:
            return None
        proto_version = struct.unpack('<i', proto_version_buffer)[0]

        data_buffer = self.read_buffer(data_size)
        if data_buffer is None:
            return None

        return self.Frame(data_buffer, ProtocolVersion(proto_version))

    def read_buffer(self, size: int) -> Optional[bytes]:
        all_data = b''

        while len(all_data) < size:
            data = self.request.recv(size - len(all_data))
            if len(data) == 0:
                return None
            all_data += data

        return all_data

    def ack_frame_v2(self):
        self.request.sendall(V2_ACK_BYTES)


class LocalLogServer(socketserver.ThreadingUnixStreamServer):
    """
    Unix domain server that uses RequestHandler and LogForwarder to relay messages.
    """

    def __init__(self, server_address, forwarder: ParallelLogForwarder):
        self.daemon_threads = True
        try:
            os.unlink(server_address)
        except OSError:
            if os.path.exists(server_address):
                raise

        super().__init__(server_address, RequestHandler)
        os.chmod(server_address, 0o777)
        self.forwarder = forwarder
        local_logger.info("Accepting connections")
