"""A loopback BitTorrent peer that serves ut_metadata for tests. Supports several failure modes."""
import hashlib
import socket
import struct
import threading
import time

from dht_scraper.bencode_codec import encode_bencode
from dht_scraper.peer_wire_messages import (
    EXTENDED_HANDSHAKE_ID,
    EXTENSION_BIT,
    EXTENSION_RESERVED_INDEX,
    FRAME_HEADER_LENGTH,
    HANDSHAKE_LENGTH,
    MESSAGE_EXTENDED,
    METADATA_PIECE_SIZE,
    MSG_TYPE_DATA,
    MSG_TYPE_REJECT,
    MSG_TYPE_REQUEST,
    UT_METADATA_LOCAL_ID,
    decode_extended_message,
    decode_frame_length,
    decode_handshake,
    decode_metadata_message,
    encode_extended_message,
    encode_frame,
    encode_handshake,
)

MODES = (
    "ok", "no_extensions", "no_ut_metadata", "too_large", "reject", "corrupt", "silent", "close_after_handshake", "wrong_hash", "huge_frame",
    "expect_pipelined", "wait_for_all_requests", "silent_before_handshake",
)


def build_info_dict(name=b"test.txt", length=1234, extra_pieces=0):
    info = {b"name": name, b"piece length": 16384, b"length": length, b"pieces": b"\x00" * 20 * (1 + extra_pieces)}
    raw = encode_bencode(info)
    return raw, hashlib.sha1(raw).digest()


def read_exact(conn, length):
    data = b""
    while len(data) < length:
        chunk = conn.recv(length - len(data))
        if not chunk:
            raise ConnectionError("closed")
        data += chunk
    return data


class FakeMetadataPeer:
    def __init__(self, info_hash, metadata, mode="ok", their_id=3, send_noise=True, host="127.0.0.1"):
        assert mode in MODES
        self.info_hash = info_hash
        self.metadata = metadata
        self.mode = mode
        self.their_id = their_id
        self.send_noise = send_noise
        self.requests = []
        self.saw_pipelined_handshake = False
        self.listener = socket.socket(socket.AF_INET6 if ":" in host else socket.AF_INET, socket.SOCK_STREAM)
        self.listener.bind((host, 0))
        self.listener.listen(5)
        self.listener.settimeout(0.2)
        self.address = (host, self.listener.getsockname()[1])
        self.running = True
        self.thread = threading.Thread(target=self.accept_loop, name="fake-peer", daemon=True)
        self.thread.start()

    def close(self):
        self.running = False
        self.listener.close()
        self.thread.join(4.0)

    def accept_loop(self):
        while self.running:
            try:
                conn, _ = self.listener.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            conn.settimeout(3.0)
            try:
                self.handle_connection(conn)
            except (OSError, ValueError, ConnectionError):
                pass
            finally:
                conn.close()

    def handle_connection(self, conn):
        info_hash, _, _ = decode_handshake(read_exact(conn, HANDSHAKE_LENGTH))
        if self.mode == "close_after_handshake":
            return
        if self.mode == "silent_before_handshake":
            time.sleep(2.5)
            return
        if self.mode == "expect_pipelined":
            conn.settimeout(1.0)
            body = read_exact(conn, decode_frame_length(read_exact(conn, FRAME_HEADER_LENGTH)))
            self.saw_pipelined_handshake = body[0] == MESSAGE_EXTENDED and body[1] == EXTENDED_HANDSHAKE_ID
            conn.settimeout(3.0)
        reply_hash = b"\x99" * 20 if self.mode == "wrong_hash" else info_hash
        handshake = bytearray(encode_handshake(reply_hash, b"-FK0001-" + b"f" * 12))
        if self.mode == "no_extensions":
            handshake[20 + EXTENSION_RESERVED_INDEX] &= ~EXTENSION_BIT
        conn.sendall(bytes(handshake))
        if self.mode == "no_extensions":
            time.sleep(0.2)
            return
        if self.send_noise:
            conn.sendall(encode_frame(5, b"\xff"))
            conn.sendall(encode_frame(4, struct.pack("!I", 0)))
            conn.sendall(struct.pack("!I", 0))
        if self.mode == "huge_frame":
            conn.sendall(struct.pack("!I", 5 * 1024 * 1024))
            time.sleep(0.2)
            return
        extensions = {} if self.mode == "no_ut_metadata" else {b"ut_metadata": self.their_id}
        size = 100 * 1024 * 1024 if self.mode == "too_large" else len(self.metadata)
        conn.sendall(encode_extended_message(EXTENDED_HANDSHAKE_ID, {b"m": extensions, b"metadata_size": size, b"v": b"fake"}))
        piece_total = (len(self.metadata) + METADATA_PIECE_SIZE - 1) // METADATA_PIECE_SIZE
        while True:
            length = decode_frame_length(read_exact(conn, FRAME_HEADER_LENGTH))
            body = read_exact(conn, length)
            if body[0] != MESSAGE_EXTENDED:
                continue
            extension_id, payload, _ = decode_extended_message(body[1:])
            if extension_id != self.their_id:
                continue
            msg_type, piece, _ = decode_metadata_message(payload)
            if msg_type != MSG_TYPE_REQUEST:
                continue
            self.requests.append(piece)
            if self.mode == "silent":
                time.sleep(2.5)
                return
            if self.mode == "reject":
                conn.sendall(encode_extended_message(UT_METADATA_LOCAL_ID, {b"msg_type": MSG_TYPE_REJECT, b"piece": piece}))
                continue
            if self.mode == "wait_for_all_requests":
                if len(self.requests) < piece_total:
                    continue
                for requested in self.requests:
                    self.send_piece(conn, requested)
                continue
            self.send_piece(conn, piece)

    def send_piece(self, conn, piece):
        data = self.metadata[piece * METADATA_PIECE_SIZE:(piece + 1) * METADATA_PIECE_SIZE]
        if self.mode == "corrupt" and piece == 0:
            data = bytes([data[0] ^ 0xff]) + data[1:]
        conn.sendall(encode_extended_message(UT_METADATA_LOCAL_ID, {b"msg_type": MSG_TYPE_DATA, b"piece": piece, b"total_size": len(self.metadata)}, data))
