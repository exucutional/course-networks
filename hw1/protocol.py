import socket
import time
import logging

from testable_thread import TestableThread

logging.basicConfig(encoding='utf-8', level=logging.INFO)

HEADER_SIZE = 12

class UDPBasedProtocol:
    def __init__(self, *, local_addr, remote_addr):
        self.udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
        self.remote_addr = remote_addr
        self.udp_socket.bind(local_addr)

    def sendto(self, data):
        return self.udp_socket.sendto(data, self.remote_addr)

    def recvfrom(self, n):
        msg, addr = self.udp_socket.recvfrom(n)
        return msg


class MyTCPHeader():
    def __init__(self, seq : int, ack : int, msg_size : int):#, checksum : bytes):
        self.seq_num = seq
        self.ack_num = ack
        self.size = msg_size
        # self.checksum = checksum

    @staticmethod
    def deserialize(data : bytes):
        seq = int.from_bytes(data[0:4], byteorder='little')
        ack = int.from_bytes(data[4:8], byteorder='little')
        size = int.from_bytes(data[8:12], byteorder='little')
        # checksum = data[8:40]
        return MyTCPHeader(seq, ack, size)#, checksum)

    def serialize(self) -> bytes:
        seq = self.seq_num.to_bytes(4, byteorder='little')
        ack = self.ack_num.to_bytes(4, byteorder='little')
        size = self.size.to_bytes(4, byteorder='little')
        #checksum = self.checksum
        return seq + ack + size# + checksum


class MyTCPMsg():
    def __init__(self, header, data):
        self.header = header
        self.data = data
        # assert(self.is_valid())

    # def is_valid(self) -> bool:
    #     hash = hashlib.shake_256(self.data)
    #     return hash.digest(32) == self.header.checksum

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
        self.recv_msgs = []
        self.max_msg_size = 32768 - HEADER_SIZE
        self.ack_sleep_time = 0.1
        self.resend_sleep_time = 0.1
        self.logger = logging.getLogger(f"P{MyTCPProtocol.id}")
        MyTCPProtocol.id += 1

    def _wait_and_resend(self):
        while len(self.send_msgs) != 0:
            for msg in self.send_msgs:
                self.logger.debug(f"resend msg {msg}")
                self._send_msg(msg, False)

            time.sleep(self.resend_sleep_time)

    def _recv_acks(self):
        while len(self.send_msgs) != 0:
            msgs = self._recv_msgs()
            for msg in msgs:
                self.logger.debug(f"ack recv {msg}")
                while len(self.send_msgs) != 0:
                    # Remove msgs from resend storage if they are acknowledged
                    stored_msg = self.send_msgs[0]
                    if msg.header.ack_num > stored_msg.header.seq_num + len(stored_msg.data):
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
        buf = self.recvfrom(100 * self.max_msg_size)
        offset = 0
        msgs = []
        while offset < len(buf):
            msg = MyTCPMsg.deserialize(buf[offset:])
            offset += HEADER_SIZE + msg.header.size
            msgs.append(msg)

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

        resend_thread = TestableThread(target=self._wait_and_resend)
        # Start thread that will resend lost messages
        resend_thread.start()

        # Wait ack for sent messages
        ack_thread = TestableThread(target=self._recv_acks)
        ack_thread.start()

        # resend_thread.join()
        self.logger.debug("end send")
        return bytes_send

    def _send_ack(self):
        header = MyTCPHeader(self.seq_num, self.ack_num, 0)#, bytes())
        msg = MyTCPMsg(header, bytes())
        self.logger.debug(f"ack send {msg}")
        self._send_msg(msg)

    def _wait_and_ack_num(self):
        while self._wait_and_ack_num_flag:
            self._send_ack()
            time.sleep(self.ack_sleep_time)

    # Restore data in appropriate order
    def _restore_data(self):
        self.recv_msgs.sort(key = lambda msg : msg.header.seq_num)
        data = bytes()
        while len(self.recv_msgs) != 0:
            msg_to_ack = self.recv_msgs[0]
            # If we already received msg, drop it
            if self.ack_num - 1 > msg_to_ack.header.seq_num:
                self.recv_msgs.pop(0)
            # If we got msg with next expected data, append it
            elif self.ack_num - 1 == msg_to_ack.header.seq_num:
                self.ack_num = msg_to_ack.header.seq_num + len(msg_to_ack.data) + 1
                data += msg_to_ack.data
                self.recv_msgs.pop(0)
            # Or we currently don't have data with correct seq num
            else:
                break

        return data

    # Blocking recv
    def recv(self, n: int):
        self.logger.debug("start recv")
        data = bytes()
        self._wait_and_ack_num_flag = True
        ack_thread = TestableThread(target=self._wait_and_ack_num)
        # Start thread that will periodically send ack for received messages
        ack_thread.start()
        while len(data) < n:
            msgs = self._recv_msgs()
            self.recv_msgs.extend(msgs)
            data += self._restore_data()
            self._send_ack()
            for msg in msgs:
                self.logger.debug(f"msg recv {msg} | {len(data)}/{n}")

        self._wait_and_ack_num_flag = False
        # ack_thread.join()
        self._send_ack()
        self.logger.debug("end recv")
        return data[:n]
