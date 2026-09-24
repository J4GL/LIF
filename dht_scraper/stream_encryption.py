"""Message stream encryption (MSE/PE), outgoing side: Diffie-Hellman key exchange, then RC4 or plaintext."""
import asyncio
import hashlib
import os
import random
import struct
from typing import Optional

from dht_scraper.krpc_messages import Address
from dht_scraper.metadata_fetcher import (
    DEFAULT_TIMEOUTS,
    ByteStream,
    FetchTimeouts,
    MetadataFetchError,
    open_tcp_stream,
    opening_messages,
    run_session,
    verify_metadata,
)
from dht_scraper.node_identity import NODE_ID_LENGTH

DH_PRIME = int(
    "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF"
    "9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A63A36210000000000090563",
    16,
)
DH_GENERATOR = 2
DH_KEY_LENGTH = 96
PRIVATE_KEY_BYTES = 20
VERIFICATION_CONSTANT = bytes(8)
CRYPTO_PLAINTEXT = 1
CRYPTO_RC4 = 2
MAX_PAD_LENGTH = 512
OUR_PAD_MAX = 64
RC4_DISCARD = 1024


class Rc4:
    """RC4 stream cipher (pure Python, enough for metadata-sized sessions)."""

    __slots__ = ("state", "i", "j")

    # Parents: mse_cipher, tests
    # Keywords: rc4, key schedule
    def __init__(self, key: bytes) -> None:
        assert 1 <= len(key) <= 256
        state = list(range(256))
        j = 0
        for i in range(256):
            j = (j + state[i] + key[i % len(key)]) & 0xFF
            state[i], state[j] = state[j], state[i]
        self.state = state
        self.i = 0
        self.j = 0
        assert sorted(self.state) == list(range(256))

    # Parents: EncryptedStream.read_exactly, EncryptedStream.write, negotiate_encryption, tests
    # Keywords: rc4, keystream, xor
    def crypt(self, data: bytes) -> bytes:
        assert isinstance(data, bytes)
        state, i, j = self.state, self.i, self.j
        output = bytearray(data)
        for index in range(len(output)):
            i = (i + 1) & 0xFF
            j = (j + state[i]) & 0xFF
            state[i], state[j] = state[j], state[i]
            output[index] ^= state[(state[i] + state[j]) & 0xFF]
        self.i, self.j = i, j
        result = bytes(output)
        assert len(result) == len(data)
        return result

    # Parents: negotiate_encryption
    # Keywords: rc4, copy, peek keystream
    def copy(self) -> "Rc4":
        assert len(self.state) == 256
        result = Rc4.__new__(Rc4)
        result.state, result.i, result.j = list(self.state), self.i, self.j
        assert result.state == self.state
        return result


# Parents: negotiate_encryption, tests
# Keywords: mse, rc4, discard 1024
def mse_cipher(key: bytes) -> Rc4:
    assert len(key) == 20
    result = Rc4(key)
    result.crypt(bytes(RC4_DISCARD))
    assert result.i == RC4_DISCARD % 256
    return result


# Parents: negotiate_encryption
# Keywords: sha1, concatenation
def sha1(*parts: bytes) -> bytes:
    assert parts
    result = hashlib.sha1(b"".join(parts)).digest()
    assert len(result) == 20
    return result


class EncryptedStream:
    """Byte stream that decrypts reads and encrypts writes; with no ciphers it passes bytes through."""

    # Parents: negotiate_encryption
    # Keywords: stream, cipher, wrap
    def __init__(self, inner: ByteStream, encryptor: Optional[Rc4], decryptor: Optional[Rc4]) -> None:
        assert (encryptor is None) == (decryptor is None)
        self.inner = inner
        self.encryptor = encryptor
        self.decryptor = decryptor
        assert self.inner is inner

    # Parents: run_session (through ByteStream)
    # Keywords: read, decrypt
    async def read_exactly(self, length: int, deadline: float, closed_reason: str = "closed") -> bytes:
        assert length > 0
        data = await self.inner.read_exactly(length, deadline, closed_reason)
        result = self.decryptor.crypt(data) if self.decryptor is not None else data
        assert len(result) == length
        return result

    # Parents: run_session (through ByteStream)
    # Keywords: write, encrypt
    def write(self, data: bytes) -> None:
        assert isinstance(data, bytes) and data
        self.inner.write(self.encryptor.crypt(data) if self.encryptor is not None else data)

    # Parents: fetch_metadata_encrypted
    # Keywords: close
    def close(self) -> None:
        assert self.inner is not None
        self.inner.close()


# Parents: fetch_metadata_encrypted
# Keywords: mse, diffie hellman, sync hash, verification constant, crypto select
async def negotiate_encryption(stream: ByteStream, info_hash: bytes, initial_payload: bytes, deadline: float) -> EncryptedStream:
    assert len(info_hash) == NODE_ID_LENGTH and 0 < len(initial_payload) <= 0xFFFF
    private = int.from_bytes(os.urandom(PRIVATE_KEY_BYTES), "big")
    public = pow(DH_GENERATOR, private, DH_PRIME).to_bytes(DH_KEY_LENGTH, "big")
    stream.write(public + os.urandom(random.randint(0, OUR_PAD_MAX)))
    remote = int.from_bytes(await stream.read_exactly(DH_KEY_LENGTH, deadline, "closed_on_handshake"), "big")
    secret = pow(remote, private, DH_PRIME).to_bytes(DH_KEY_LENGTH, "big")
    encryptor = mse_cipher(sha1(b"keyA", secret, info_hash))
    decryptor = mse_cipher(sha1(b"keyB", secret, info_hash))
    obfuscated = bytes(left ^ right for left, right in zip(sha1(b"req2", info_hash), sha1(b"req3", secret)))
    header = VERIFICATION_CONSTANT + struct.pack("!IHH", CRYPTO_RC4 | CRYPTO_PLAINTEXT, 0, len(initial_payload))
    stream.write(sha1(b"req1", secret) + obfuscated + encryptor.crypt(header) + encryptor.crypt(initial_payload))
    marker = decryptor.copy().crypt(VERIFICATION_CONSTANT)
    window = await stream.read_exactly(len(marker), deadline)
    scanned = 0
    while window != marker:
        if scanned >= MAX_PAD_LENGTH:
            raise MetadataFetchError("encryption_failed")
        window = window[1:] + await stream.read_exactly(1, deadline)
        scanned += 1
    decryptor.crypt(VERIFICATION_CONSTANT)
    select, pad_length = struct.unpack("!IH", decryptor.crypt(await stream.read_exactly(6, deadline)))
    if select not in (CRYPTO_PLAINTEXT, CRYPTO_RC4) or pad_length > MAX_PAD_LENGTH:
        raise MetadataFetchError("encryption_failed")
    if pad_length:
        decryptor.crypt(await stream.read_exactly(pad_length, deadline))
    result = EncryptedStream(stream, encryptor, decryptor) if select == CRYPTO_RC4 else EncryptedStream(stream, None, None)
    assert result.inner is stream
    return result


# Parents: FetchEngine.attempt (retry function), run_scraper
# Keywords: fetch, encrypted session, retry, verified metadata
async def fetch_metadata_encrypted(info_hash: bytes, address: Address, timeouts: FetchTimeouts = DEFAULT_TIMEOUTS) -> bytes:
    assert len(info_hash) == NODE_ID_LENGTH and len(address) == 2
    loop = asyncio.get_running_loop()
    session_deadline = loop.time() + timeouts.session
    stream = await open_tcp_stream(address, timeouts.connect)
    try:
        handshake_deadline = min(session_deadline, loop.time() + timeouts.handshake)
        encrypted = await negotiate_encryption(stream, info_hash, opening_messages(info_hash), handshake_deadline)
        metadata = await run_session(encrypted, info_hash, timeouts, session_deadline, send_handshakes=False)
    finally:
        stream.close()
    verify_metadata(info_hash, metadata)
    assert len(metadata) > 0
    return metadata
