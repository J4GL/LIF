"""A loopback peer that answers the MSE handshake (responder side) and then serves ut_metadata."""
import hashlib
import os
import struct

from dht_scraper.stream_encryption import (
    CRYPTO_PLAINTEXT,
    CRYPTO_RC4,
    DH_GENERATOR,
    DH_KEY_LENGTH,
    DH_PRIME,
    VERIFICATION_CONSTANT,
    mse_cipher,
)
from tests.fetch.fake_metadata_peer import FakeMetadataPeer, read_exact

PLAINTEXT_PREFIX = b"\x13BitTorrent protocol"


def sha1(data):
    return hashlib.sha1(data).digest()


class CipherConnection:
    """Socket-like wrapper: buffered plaintext first, then decrypt reads and encrypt writes."""

    def __init__(self, conn, pending, decryptor, encryptor):
        self.conn = conn
        self.pending = pending
        self.decryptor = decryptor
        self.encryptor = encryptor

    def recv(self, length):
        if self.pending:
            data, self.pending = self.pending[:length], self.pending[length:]
            return data
        data = self.conn.recv(length)
        return self.decryptor.crypt(data) if self.decryptor and data else data

    def sendall(self, data):
        self.conn.sendall(self.encryptor.crypt(data) if self.encryptor else data)

    def settimeout(self, value):
        self.conn.settimeout(value)


class FakeMsePeer(FakeMetadataPeer):
    """Requires encryption: closes plaintext handshakes, answers MSE and selects RC4 or plaintext."""

    def __init__(self, info_hash, metadata, select=CRYPTO_RC4):
        self.select = select
        self.verified_sync = False
        super().__init__(info_hash, metadata, "ok", send_noise=False)

    def handle_connection(self, conn):
        first = read_exact(conn, len(PLAINTEXT_PREFIX))
        if first == PLAINTEXT_PREFIX:
            return
        public = first + read_exact(conn, DH_KEY_LENGTH - len(first))
        private = int.from_bytes(os.urandom(20), "big")
        conn.sendall(pow(DH_GENERATOR, private, DH_PRIME).to_bytes(DH_KEY_LENGTH, "big") + os.urandom(37))
        secret = pow(int.from_bytes(public, "big"), private, DH_PRIME).to_bytes(DH_KEY_LENGTH, "big")
        sync = sha1(b"req1" + secret)
        scanned = b""
        while not scanned.endswith(sync):
            scanned += read_exact(conn, 1)
            if len(scanned) > 512 + len(sync):
                return
        obfuscated = read_exact(conn, 20)
        expected = bytes(a ^ b for a, b in zip(sha1(b"req2" + self.info_hash), sha1(b"req3" + secret)))
        if obfuscated != expected:
            return
        decryptor = mse_cipher(sha1(b"keyA" + secret + self.info_hash))
        encryptor = mse_cipher(sha1(b"keyB" + secret + self.info_hash))
        verification, provide, pad_length = struct.unpack("!8sIH", decryptor.crypt(read_exact(conn, 14)))
        if verification != VERIFICATION_CONSTANT or not provide & self.select:
            return
        decryptor.crypt(read_exact(conn, pad_length))
        initial_length = struct.unpack("!H", decryptor.crypt(read_exact(conn, 2)))[0]
        initial_payload = decryptor.crypt(read_exact(conn, initial_length))
        self.verified_sync = True
        conn.sendall(encryptor.crypt(VERIFICATION_CONSTANT + struct.pack("!IH", self.select, 3) + b"pad"))
        if self.select == CRYPTO_PLAINTEXT:
            decryptor = encryptor = None
        super().handle_connection(CipherConnection(conn, initial_payload, decryptor, encryptor))
