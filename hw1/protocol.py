import socket
import logging

from sortedcontainers import SortedList
from testable_thread import TestableThread

logging.basicConfig(encoding='utf-8', level=logging.INFO)

HEADER_SIZE = 12

class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)
        self.udp_socket.settimeout(0)

    def sendto(self, data):
        bytes = 0
        while not bytes:
            try:
                bytes = self.udp_socket.sendto(data, self.remote_addr)
                return bytes
            except PermissionError:
                pass

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg


class MyTCPHeader():
    def __init__(self, seq : int, ack : int, msg_size : int):
        self.seq_num = seq
        self.ack_num = ack
        self.size = msg_size

    @staticmethod
    def deserialize(data : bytes):
        seq = int.from_bytes(data[0:4], byteorder='little')
        ack = int.from_bytes(data[4:8], byteorder='little')
        size = int.from_bytes(data[8:12], byteorder='little')
        return MyTCPHeader(seq, ack, size)

    def serialize(self) -> bytes:
        seq = self.seq_num.to_bytes(4, byteorder='little')
        ack = self.ack_num.to_bytes(4, byteorder='little')
        size = self.size.to_bytes(4, byteorder='little')
        return seq + ack + size


class MyTCPMsg():
    def __init__(self, header, data):
        self.header = header
        self.data = data

    @staticmethod
    def deserialize(data : bytes):
        header = MyTCPHeader.deserialize(data[0:HEADER_SIZE])
        return MyTCPMsg(header, data[HEADER_SIZE:HEADER_SIZE+header.size])

    def serialize(self) -> bytes:
        header = self.header.serialize()
        return header + self.data

    def __str__(self) -> str:
        return f"seq {self.header.seq_num} | ack {self.header.ack_num} | size {len(self.data)}"


class MyTCPProtocol(UDPBasedProtocol):
    id = 0
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seq_num = 0
        self.ack_num = 1
        self.send_msgs = []
        self.recv_msgs = SortedList(key=lambda x : x.header.seq_num)
        self.max_msg_size = 32768 - HEADER_SIZE
        self.resend_rate = 2
        self.ack_rate = 2
        self.max_resend_attempt = 1024
        self.logger = logging.getLogger(f"P{MyTCPProtocol.id}")
        MyTCPProtocol.id += 1

    def _is_msg_ack_by_ack(self, msg, ack):
        return ack.header.ack_num > msg.header.seq_num + len(msg.data)

    def _recv_acks(self):
        msgs = self._recv_msgs()
        for msg in msgs:
            self.logger.debug(f"ack recv {msg}")
            if self.seq_num < msg.header.ack_num:
                self.send_msgs.clear()
                if len(msg.data) != 0:
                    self.recv_msgs.add(msg)
            while len(self.send_msgs) != 0:
                # Remove msgs from resend storage if they are acknowledged
                stored_msg = self.send_msgs[0]
                if self._is_msg_ack_by_ack(stored_msg, msg):
                    self.send_msgs.pop(0)
                else:
                    break

    def _send_msg(self, msg : MyTCPMsg, increase_seq = True):
        if increase_seq:
            self.seq_num += len(msg.data)
        data_with_header = msg.serialize()
        assert(len(data_with_header) == HEADER_SIZE + len(msg.data))
        return self.sendto(data_with_header) - HEADER_SIZE

    def _recv_msgs(self):
        msgs = []
        while True:
            try:
                buf = self.recvfrom(100 * self.max_msg_size)
                offset = 0
                while offset < len(buf):
                    msg = MyTCPMsg.deserialize(buf[offset:])
                    offset += HEADER_SIZE + msg.header.size
                    msgs.append(msg)
            except socket.timeout:
                return msgs
            except BlockingIOError:
                return msgs

    def send(self, data: bytes):
        self.logger.debug("start send")
        bytes_send = 0
        # Split data into smaller messages and send them
        while bytes_send < len(data):
            msg_data = data[bytes_send:bytes_send+self.max_msg_size]
            bytes_send += len(msg_data)
            header = MyTCPHeader(self.seq_num, self.ack_num, len(msg_data))#, hashlib.shake_256(msg_data).digest(32))
            msg = MyTCPMsg(header, msg_data)
            self._send_msg(msg)
            # Store msg for resending
            self.send_msgs.append(msg)
            self.logger.debug(f"msg send {msg} | {bytes_send}/{len(data)}")

        # Receive acks and resend if timeout
        attempt = 0
        while len(self.send_msgs) != 0 and attempt < self.max_resend_attempt:
            old_msg_len = len(self.send_msgs)
            self._recv_acks()
            if old_msg_len == len(self.send_msgs):
                attempt += 1
                if attempt % self.resend_rate == 0:
                    self._send_msg(self.send_msgs[0], False)
                    self.logger.debug(f"resend msg {self.send_msgs[0]}")
            else:
                attempt = 0

        self.logger.debug("end send")
        return bytes_send

    def _send_ack(self):
        header = MyTCPHeader(self.seq_num, self.ack_num, 0)#, bytes())
        msg = MyTCPMsg(header, bytes())
        self.logger.debug(f"ack send {msg}")
        self._send_msg(msg)

    def _remove_duplicates(self, msgs, known_ack):
        new_msgs = []
        for msg in msgs:
            ack = msg.header.seq_num + len(msg.data) + 1
            if self.ack_num - 1 <= msg.header.seq_num and ack not in known_ack:
                known_ack.add(ack)
                new_msgs.append(msg)

        return new_msgs

    # Restore data in appropriate order
    def _restore_data(self):
        data = bytes()
        while len(self.recv_msgs) != 0:
            msg_to_ack = self.recv_msgs[0]
            # If we got msg with next expected data, append it
            if self.ack_num - 1 == msg_to_ack.header.seq_num:
                self.ack_num = msg_to_ack.header.seq_num + len(msg_to_ack.data) + 1
                data += msg_to_ack.data
                self.recv_msgs.pop(0)
            # If we already received msg, drop it
            elif self.ack_num - 1 > msg_to_ack.header.seq_num:
                self.recv_msgs.pop(0)
            # Or we currently don't have data with correct seq num
            else:
                break

        return data

    # Blocking recv
    def recv(self, n: int):
        self.logger.debug("start recv")
        data = bytes()
        attempt = 0
        known_ack = set()
        while len(data) < n:
            msgs = self._remove_duplicates(self._recv_msgs(), known_ack)
            self.recv_msgs.update(msgs)
            new_data = self._restore_data()
            data += new_data
            if len(new_data) == 0:
                attempt += 1
                if attempt % self.ack_rate == 0:
                    self._send_ack()
            for msg in msgs:
                self.logger.debug(f"msg recv {msg} | {len(data)}/{n}")

        self._send_ack()
        self.logger.debug("end recv")
        return data[:n]
