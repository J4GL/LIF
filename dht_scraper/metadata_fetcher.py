"""Fetch a torrent's info dictionary from one peer over TCP (BEP 10 extension protocol, BEP 9 ut_metadata)."""
import hashlib
import logging
import socket
import threading
import time
from typing import Any, Callable, Dict, NamedTuple, Optional, Tuple

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
STOP_POLL_SECONDS = 1.0


class FetchTimeouts(NamedTuple):
    connect: float = 5.0
    session: float = 30.0


DEFAULT_TIMEOUTS = FetchTimeouts()
FETCH_REASONS = (
    "connect", "closed", "timeout", "stopped", "bad_handshake", "hash_mismatch", "no_extensions",
    "no_ut_metadata", "too_large", "reject", "bad_piece", "sha1_mismatch", "frame_too_large", "bad_extended_message",
)


class MetadataFetchError(Exception):
    """A fetch attempt failed for the named reason."""

    # Parents: every fetch step
    # Keywords: error, reason, fetch failure
    def __init__(self, reason: str) -> None:
        assert reason in FETCH_REASONS, reason
        super().__init__(reason)
        self.reason = reason
        assert str(self) == reason


# Parents: receive_exact, fetch_metadata
# Keywords: deadline, remaining, timeout
def remaining_time(deadline: float, clock: Callable[[], float] = time.monotonic) -> float:
    assert deadline >= 0
    result = deadline - clock()
    if result <= 0:
        raise MetadataFetchError("timeout")
    assert result > 0
    return result


# Parents: fetch_metadata
# Keywords: tcp, connect, peer, timeout
def open_peer_connection(address: Address, timeouts: FetchTimeouts) -> socket.socket:
    assert len(address) == 2 and timeouts.connect > 0
    try:
        sock = socket.create_connection(address, timeout=timeouts.connect)
    except socket.timeout:
        raise MetadataFetchError("timeout")
    except OSError:
        raise MetadataFetchError("connect")
    assert sock is not None
    return sock


# Parents: exchange_handshake, exchange_extended_handshake, request_piece
# Keywords: send, tcp, sendall, closed
def send_all(sock: socket.socket, data: bytes) -> None:
    assert isinstance(data, bytes) and len(data) > 0
    try:
        sock.sendall(data)
    except socket.timeout:
        raise MetadataFetchError("timeout")
    except OSError:
        raise MetadataFetchError("closed")
    assert True


# Parents: exchange_handshake, receive_frame
# Keywords: recv, exact length, deadline, stop event
def receive_exact(sock: socket.socket, length: int, deadline: float, stop_event: Optional[threading.Event]) -> bytes:
    assert length >= 0
    chunks = bytearray()
    while len(chunks) < length:
        if stop_event is not None and stop_event.is_set():
            raise MetadataFetchError("stopped")
        sock.settimeout(min(STOP_POLL_SECONDS, remaining_time(deadline)))
        try:
            chunk = sock.recv(length - len(chunks))
        except socket.timeout:
            continue
        except OSError:
            raise MetadataFetchError("closed")
        if not chunk:
            raise MetadataFetchError("closed")
        chunks.extend(chunk)
    result = bytes(chunks)
    assert len(result) == length
    return result


# Parents: exchange_extended_handshake, request_piece
# Keywords: frame, message id, keep alive, size cap
def receive_frame(sock: socket.socket, deadline: float, stop_event: Optional[threading.Event]) -> Tuple[int, bytes]:
    assert deadline >= 0
    length = decode_frame_length(receive_exact(sock, FRAME_HEADER_LENGTH, deadline, stop_event))
    if length == 0:
        return KEEP_ALIVE_ID, b""
    if length > MAX_FRAME_LENGTH:
        raise MetadataFetchError("frame_too_large")
    body = receive_exact(sock, length, deadline, stop_event)
    result = (body[0], body[1:])
    assert 0 <= result[0] <= 255
    return result


# Parents: fetch_metadata
# Keywords: handshake, extension bit, info hash check
def exchange_handshake(sock: socket.socket, info_hash: bytes, peer_id: bytes, deadline: float, stop_event: Optional[threading.Event]) -> None:
    assert len(info_hash) == NODE_ID_LENGTH and len(peer_id) == NODE_ID_LENGTH
    send_all(sock, encode_handshake(info_hash, peer_id))
    try:
        remote_hash, _, supports_extensions = decode_handshake(receive_exact(sock, HANDSHAKE_LENGTH, deadline, stop_event))
    except ValueError:
        raise MetadataFetchError("bad_handshake")
    if remote_hash != info_hash:
        raise MetadataFetchError("hash_mismatch")
    if not supports_extensions:
        raise MetadataFetchError("no_extensions")
    assert supports_extensions


# Parents: exchange_extended_handshake, request_piece
# Keywords: extended message, wait for extension id, skip other frames
def receive_extended(sock: socket.socket, extension_id: int, deadline: float, stop_event: Optional[threading.Event]) -> Tuple[Dict[bytes, Any], bytes]:
    assert 0 <= extension_id <= 255
    while True:
        message_id, body = receive_frame(sock, deadline, stop_event)
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


# Parents: fetch_metadata
# Keywords: extended handshake, ut_metadata id, metadata size
def exchange_extended_handshake(sock: socket.socket, deadline: float, stop_event: Optional[threading.Event]) -> Tuple[int, int]:
    assert deadline >= 0
    send_all(sock, encode_extended_handshake())
    payload, _ = receive_extended(sock, EXTENDED_HANDSHAKE_ID, deadline, stop_event)
    try:
        result = read_extended_handshake(payload)
    except ValueError as error:
        raise MetadataFetchError(str(error))
    assert 1 <= result[0] <= 255 and result[1] > 0
    return result


# Parents: fetch_metadata
# Keywords: ut_metadata, request, piece, data, reject
def request_piece(sock: socket.socket, their_id: int, piece: int, expected_length: int, metadata_size: int, deadline: float, stop_event: Optional[threading.Event]) -> bytes:
    assert 1 <= their_id <= 255 and piece >= 0 and expected_length > 0
    send_all(sock, encode_metadata_request(their_id, piece))
    while True:
        payload, data = receive_extended(sock, UT_METADATA_LOCAL_ID, deadline, stop_event)
        try:
            msg_type, received_piece, total_size = decode_metadata_message(payload)
        except ValueError:
            raise MetadataFetchError("bad_piece")
        if received_piece != piece:
            continue
        if msg_type == MSG_TYPE_REJECT:
            raise MetadataFetchError("reject")
        if msg_type != MSG_TYPE_DATA:
            continue
        if len(data) != expected_length or (total_size is not None and total_size != metadata_size):
            raise MetadataFetchError("bad_piece")
        break
    assert len(data) == expected_length
    return data


# Parents: fetch_metadata
# Keywords: sha1, verify, info hash
def verify_metadata(info_hash: bytes, metadata: bytes) -> None:
    assert len(info_hash) == NODE_ID_LENGTH
    digest = hashlib.sha1(metadata).digest()
    if digest != info_hash:
        raise MetadataFetchError("sha1_mismatch")
    assert digest == info_hash


# Parents: FetchWorkerPool.process_candidate
# Keywords: fetch, session, pieces, verified metadata
def fetch_metadata(info_hash: bytes, address: Address, stop_event: Optional[threading.Event] = None, timeouts: FetchTimeouts = DEFAULT_TIMEOUTS) -> bytes:
    assert len(info_hash) == NODE_ID_LENGTH and len(address) == 2
    deadline = time.monotonic() + timeouts.session
    sock = open_peer_connection(address, timeouts)
    try:
        exchange_handshake(sock, info_hash, generate_peer_id(), deadline, stop_event)
        their_id, metadata_size = exchange_extended_handshake(sock, deadline, stop_event)
        pieces = []
        for piece in range(piece_count(metadata_size)):
            pieces.append(request_piece(sock, their_id, piece, expected_piece_length(metadata_size, piece), metadata_size, deadline, stop_event))
        metadata = b"".join(pieces)
    finally:
        sock.close()
    verify_metadata(info_hash, metadata)
    LOGGER.debug("metadata fetched for %s from %s:%d (%d bytes)", info_hash.hex(), address[0], address[1], len(metadata))
    assert len(metadata) == metadata_size
    return metadata
