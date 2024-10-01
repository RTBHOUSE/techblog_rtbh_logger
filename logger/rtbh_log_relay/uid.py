import random
import socket
import time


class Uid:
    """
    Short unique identifier.
    """
    BASE_62 = [bytes(str(x), 'ascii') for x in range(10)] + \
              [bytes(chr(i), 'ascii') for i in range(ord('a'), ord('z') + 1)] + \
              [bytes(chr(i), 'ascii') for i in range(ord('A'), ord('Z') + 1)]

    @staticmethod
    def int_base_62(x: int, pad_size: int) -> bytes:
        byte_repr = b''
        while x > 0:
            m = x % 62
            byte_repr = Uid.BASE_62[m] + byte_repr
            x //= 62
        return byte_repr.rjust(pad_size, b'0')

    @staticmethod
    def generate_short_uid() -> bytes:
        timestamp = int(time.time())
        rand_id = random.randint(0, 1 << 64)  # TODO secure rand
        hostname = Uid.int_base_62(hash(socket.gethostname()) % (62 ** 4), 4)
        return b'%b-%b-%b' % (hostname, Uid.int_base_62(timestamp, 6), Uid.int_base_62(rand_id, 11))
