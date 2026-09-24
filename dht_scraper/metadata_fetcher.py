"""Fetch a torrent's info dictionary from one peer with asyncio (BEP 10 extension protocol, BEP 9 ut_metadata)."""
import asyncio
import errno
import hashlib
import logging
import socket
import struct
from typing import Any, Awaitable, Callable, Dict, NamedTuple, Optional, Set, Tuple

try:
    from typing import Protocol
except ImportError:  # pragma: no cover (Python 3.7)
    Protocol = object  # type: ignore

from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.krpc_messages import Address
from dht_scraper.node_identity import NODE_ID_LENGTH, generate_peer_id
from dht_scraper.peer_wire_messages import (
    EXTENDED_HANDSHAKE_ID,
    FRAME_HEADER_LENGTH,
    HANDSHAKE_LENGTH,
    KEEP_ALIVE_ID,
    MAX_FRAME_LENGTH,
    MESSAGE_EXTENDED,
    MSG_TYPE_DATA,
    MSG_TYPE_REJECT,
    UT_METADATA_LOCAL_ID,
    decode_extended_message,
    decode_frame_length,
    decode_handshake,
    decode_metadata_message,
    encode_extended_handshake,
    encode_handshake,
    encode_metadata_request,
    expected_piece_length,
    piece_count,
    read_extended_handshake,
)

LOGGER = logging.getLogger(LOGGER_NAME)
PIECE_WINDOW = 8
LOCAL_ERRNOS = frozenset({errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM, errno.EADDRNOTAVAIL})
ABORTIVE_CLOSE = struct.pack("ii", 1, 0)


class FetchTimeouts(NamedTuple):
    connect: float = 1.0
    handshake: float = 4.0
    piece: float = 5.0
    session: float = 15.0
    utp_connect: float = 2.0


DEFAULT_TIMEOUTS = FetchTimeouts()
FETCH_REASONS = (
    "connect", "connect_timeout", "closed", "closed_on_handshake", "timeout", "stopped", "bad_handshake", "hash_mismatch",
    "no_extensions", "no_ut_metadata", "too_large", "reject", "bad_piece", "sha1_mismatch", "frame_too_large",
    "bad_extended_message", "encryption_failed", "local_error",
)
ConnectFunction = Callable[[socket.socket, Address], Awaitable[None]]


class MetadataFetchError(Exception):
    """A fetch attempt failed for the named reason."""

    # Parents: every fetch step
    # Keywords: error, reason, fetch failure
    def __init__(self, reason: str) -> None:
        assert reason in FETCH_REASONS, reason
        super().__init__(reason)
        self.reason = reason
        assert str(self) == reason


# Parents: within
# Keywords: task, exception, retrieved, no warning
def discard_outcome(task: "asyncio.Future[Any]") -> None:
    assert task.done()
    if not task.cancelled():
        task.exception()


# Parents: open_tcp_stream, PeerStream.read_exactly
# Keywords: timeout, asyncio.wait, cancellation, python 3.9
async def within(awaitable: Awaitable[Any], timeout: float) -> Any:
    """Await with a timeout. asyncio.wait_for in 3.9 can lose a cancellation or a finished connection."""
    assert timeout >= 0
    task = asyncio.ensure_future(awaitable)
    try:
        done, _ = await asyncio.wait({task}, timeout=timeout)
    except asyncio.CancelledError:
        task.cancel()
        task.add_done_callback(discard_outcome)
        raise
    if not done:
        task.cancel()
        task.add_done_callback(discard_outcome)
        raise asyncio.TimeoutError()
    result = task.result()
    assert task.done()
    return result


# Parents: open_tcp_stream
# Keywords: errno, local error, blame
def connect_error_reason(error: OSError) -> str:
    assert isinstance(error, OSError)
    result = "local_error" if error.errno in LOCAL_ERRNOS else "connect"
    assert result in FETCH_REASONS
    return result


class ByteStream(Protocol):
    """What a session needs from a connection: TCP (PeerStream), MSE (EncryptedStream) or uTP."""

    async def read_exactly(self, length: int, deadline: float, closed_reason: str = "closed") -> bytes:
        ...

    def write(self, data: bytes) -> None:
        ...

    def close(self) -> None:
        ...


class PeerStream:
    """Byte stream to one peer over TCP with deadline-bound reads."""

    # Parents: open_tcp_stream
    # Keywords: stream, reader, writer
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        assert reader is not None and writer is not None
        self.reader = reader
        self.writer = writer
        assert self.writer is writer

    # Parents: read_handshake, read_frame
    # Keywords: read exact, deadline, closed, timeout
    async def read_exactly(self, length: int, deadline: float, closed_reason: str = "closed") -> bytes:
        assert length > 0 and closed_reason in FETCH_REASONS
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise MetadataFetchError("timeout")
        try:
            result = await within(self.reader.readexactly(length), remaining)
        except asyncio.IncompleteReadError as error:
            raise MetadataFetchError(closed_reason if not error.partial else "closed")
        except asyncio.TimeoutError:
            raise MetadataFetchError("timeout")
        except OSError:
            raise MetadataFetchError(closed_reason)
        assert len(result) == length
        return result

    # Parents: run_session, request_pieces
    # Keywords: write, buffer, send
    def write(self, data: bytes) -> None:
        assert isinstance(data, bytes) and data
        self.writer.write(data)

    # Parents: fetch_metadata
    # Keywords: close, abort, no time wait
    def close(self) -> None:
        assert self.writer is not None
        self.writer.transport.abort()


# Parents: fetch_metadata, stream_encryption.fetch_metadata_encrypted, tests
# Keywords: tcp, connect, timeout, socket closed on failure, linger
async def open_tcp_stream(address: Address, timeout: float, connect: Optional[ConnectFunction] = None) -> PeerStream:
    assert len(address) == 2 and timeout > 0
    loop = asyncio.get_running_loop()
    family = socket.AF_INET6 if ":" in address[0] else socket.AF_INET
    try:
        sock = socket.socket(family, socket.SOCK_STREAM)
    except OSError as error:
        raise MetadataFetchError(connect_error_reason(error))
    try:
        sock.setblocking(False)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, ABORTIVE_CLOSE)
        try:
            await within((connect or loop.sock_connect)(sock, address), timeout)
        except asyncio.TimeoutError:
            raise MetadataFetchError("connect_timeout")
        except OSError as error:
            raise MetadataFetchError(connect_error_reason(error))
        reader, writer = await asyncio.open_connection(sock=sock)
    except BaseException:
        sock.close()
        raise
    result = PeerStream(reader, writer)
    assert result.writer is writer
    return result


# Parents: receive_extended
# Keywords: frame, message id, keep alive, size cap
async def read_frame(stream: ByteStream, deadline: float) -> Tuple[int, bytes]:
    assert deadline >= 0
    length = decode_frame_length(await stream.read_exactly(FRAME_HEADER_LENGTH, deadline))
    if length == 0:
        return KEEP_ALIVE_ID, b""
    if length > MAX_FRAME_LENGTH:
        raise MetadataFetchError("frame_too_large")
    body = await stream.read_exactly(length, deadline)
    result = (body[0], body[1:])
    assert 0 <= result[0] <= 255
    return result


# Parents: run_session
# Keywords: handshake, extension bit, info hash check, closed before handshake
async def read_handshake(stream: ByteStream, info_hash: bytes, deadline: float) -> None:
    assert len(info_hash) == NODE_ID_LENGTH
    data = await stream.read_exactly(HANDSHAKE_LENGTH, deadline, "closed_on_handshake")
    try:
        remote_hash, _, supports_extensions = decode_handshake(data)
    except ValueError:
        raise MetadataFetchError("bad_handshake")
    if remote_hash != info_hash:
        raise MetadataFetchError("hash_mismatch")
    if not supports_extensions:
        raise MetadataFetchError("no_extensions")
    assert supports_extensions


# Parents: receive_extended_handshake, receive_piece
# Keywords: extended message, wait for extension id, skip other frames
async def receive_extended(stream: ByteStream, extension_id: int, deadline: float) -> Tuple[Dict[bytes, Any], bytes]:
    assert 0 <= extension_id <= 255
    while True:
        message_id, body = await read_frame(stream, deadline)
        if message_id != MESSAGE_EXTENDED:
            continue
        try:
            received_id, payload, trailing = decode_extended_message(body)
        except (ValueError, RecursionError):
            raise MetadataFetchError("bad_extended_message")
        if received_id == extension_id:
            break
    assert isinstance(payload, dict)
    return payload, trailing


# Parents: run_session
# Keywords: extended handshake, ut_metadata id, metadata size
async def receive_extended_handshake(stream: ByteStream, deadline: float) -> Tuple[int, int]:
    assert deadline >= 0
    payload, _ = await receive_extended(stream, EXTENDED_HANDSHAKE_ID, deadline)
    try:
        result = read_extended_handshake(payload)
    except ValueError as error:
        raise MetadataFetchError(str(error))
    assert 1 <= result[0] <= 255 and result[1] > 0
    return result


# Parents: request_pieces
# Keywords: ut_metadata, data, reject, outstanding piece
async def receive_piece(stream: ByteStream, outstanding: Set[int], metadata_size: int, deadline: float) -> Tuple[int, bytes]:
    assert outstanding and metadata_size > 0
    while True:
        payload, data = await receive_extended(stream, UT_METADATA_LOCAL_ID, deadline)
        try:
            msg_type, piece, total_size = decode_metadata_message(payload)
        except ValueError:
            raise MetadataFetchError("bad_piece")
        if piece not in outstanding:
            continue
        if msg_type == MSG_TYPE_REJECT:
            raise MetadataFetchError("reject")
        if msg_type != MSG_TYPE_DATA:
            continue
        if len(data) != expected_piece_length(metadata_size, piece) or (total_size is not None and total_size != metadata_size):
            raise MetadataFetchError("bad_piece")
        break
    assert piece in outstanding
    return piece, data


# Parents: run_session
# Keywords: ut_metadata, request window, pipelined pieces, piece timeout
async def request_pieces(stream: ByteStream, their_id: int, metadata_size: int, timeouts: FetchTimeouts, session_deadline: float) -> bytes:
    assert 1 <= their_id <= 255 and metadata_size > 0
    loop = asyncio.get_running_loop()
    count = piece_count(metadata_size)
    pieces: Dict[int, bytes] = {}
    outstanding: Set[int] = set()
    next_piece = 0
    while len(pieces) < count:
        while next_piece < count and len(outstanding) < PIECE_WINDOW:
            stream.write(encode_metadata_request(their_id, next_piece))
            outstanding.add(next_piece)
            next_piece += 1
        piece, data = await receive_piece(stream, outstanding, metadata_size, min(session_deadline, loop.time() + timeouts.piece))
        outstanding.discard(piece)
        pieces[piece] = data
    result = b"".join(pieces[index] for index in range(count))
    assert len(result) == metadata_size
    return result


# Parents: fetch_metadata, stream_encryption.fetch_metadata_encrypted
# Keywords: session, handshakes pipelined, extended handshake, pieces
async def run_session(stream: ByteStream, info_hash: bytes, timeouts: FetchTimeouts, session_deadline: float, send_handshakes: bool = True) -> bytes:
    assert len(info_hash) == NODE_ID_LENGTH
    loop = asyncio.get_running_loop()
    if send_handshakes:
        stream.write(opening_messages(info_hash))
    handshake_deadline = min(session_deadline, loop.time() + timeouts.handshake)
    await read_handshake(stream, info_hash, handshake_deadline)
    their_id, metadata_size = await receive_extended_handshake(stream, handshake_deadline)
    result = await request_pieces(stream, their_id, metadata_size, timeouts, session_deadline)
    assert len(result) == metadata_size
    return result


# Parents: run_session, stream_encryption.fetch_metadata_encrypted
# Keywords: handshake, extended handshake, one write
def opening_messages(info_hash: bytes) -> bytes:
    assert len(info_hash) == NODE_ID_LENGTH
    result = encode_handshake(info_hash, generate_peer_id()) + encode_extended_handshake()
    assert len(result) > HANDSHAKE_LENGTH
    return result


# Parents: fetch_metadata, stream_encryption.fetch_metadata_encrypted
# Keywords: sha1, verify, info hash
def verify_metadata(info_hash: bytes, metadata: bytes) -> None:
    assert len(info_hash) == NODE_ID_LENGTH
    digest = hashlib.sha1(metadata).digest()
    if digest != info_hash:
        raise MetadataFetchError("sha1_mismatch")
    assert digest == info_hash


# Parents: FetchEngine.attempt (default fetch function), run_scraper
# Keywords: fetch, session, tcp, verified metadata
async def fetch_metadata(info_hash: bytes, address: Address, timeouts: FetchTimeouts = DEFAULT_TIMEOUTS) -> bytes:
    assert len(info_hash) == NODE_ID_LENGTH and len(address) == 2
    session_deadline = asyncio.get_running_loop().time() + timeouts.session
    stream = await open_tcp_stream(address, timeouts.connect)
    try:
        metadata = await run_session(stream, info_hash, timeouts, session_deadline)
    finally:
        stream.close()
    verify_metadata(info_hash, metadata)
    LOGGER.debug("metadata fetched for %s from %s:%d (%d bytes)", info_hash.hex(), address[0], address[1], len(metadata))
    assert len(metadata) > 0
    return metadata
