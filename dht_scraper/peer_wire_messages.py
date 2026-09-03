"""Peer wire protocol encoding: handshake (BEP 3), extension protocol (BEP 10), ut_metadata (BEP 9)."""
import struct
from typing import Any, Dict, Optional, Tuple

from dht_scraper.bencode_codec import decode_value_at, encode_bencode, is_plain_int
from dht_scraper.node_identity import NODE_ID_LENGTH

HANDSHAKE_LENGTH = 68
PROTOCOL_NAME = b"BitTorrent protocol"
RESERVED_LENGTH = 8
EXTENSION_RESERVED_INDEX = 5
EXTENSION_BIT = 0x10
FRAME_HEADER_LENGTH = 4
MESSAGE_EXTENDED = 20
MESSAGE_PIECE = 7
EXTENDED_HANDSHAKE_ID = 0
UT_METADATA_LOCAL_ID = 1
METADATA_PIECE_SIZE = 16384
MAX_METADATA_SIZE = 4 * 1024 * 1024
MAX_FRAME_LENGTH = 1024 * 1024
MSG_TYPE_REQUEST = 0
MSG_TYPE_DATA = 1
MSG_TYPE_REJECT = 2
CLIENT_VERSION = b"dht_scraper 2.0"
KEEP_ALIVE_ID = -1


# Parents: exchange_handshake
# Keywords: handshake, reserved bits, extension bit, peer wire
def encode_handshake(info_hash: bytes, peer_id: bytes) -> bytes:
    assert len(info_hash) == NODE_ID_LENGTH and len(peer_id) == NODE_ID_LENGTH
    reserved = bytearray(RESERVED_LENGTH)
    reserved[EXTENSION_RESERVED_INDEX] |= EXTENSION_BIT
    result = bytes([len(PROTOCOL_NAME)]) + PROTOCOL_NAME + bytes(reserved) + info_hash + peer_id
    assert len(result) == HANDSHAKE_LENGTH and result[28:48] == info_hash
    return result


# Parents: exchange_handshake, FakeMetadataPeer (tests)
# Keywords: handshake, decode, extension bit, validation
def decode_handshake(data: bytes) -> Tuple[bytes, bytes, bool]:
    assert isinstance(data, bytes)
    if len(data) != HANDSHAKE_LENGTH or data[0] != len(PROTOCOL_NAME) or data[1:20] != PROTOCOL_NAME:
        raise ValueError("bad_handshake")
    supports_extensions = bool(data[20 + EXTENSION_RESERVED_INDEX] & EXTENSION_BIT)
    info_hash = data[28:48]
    peer_id = data[48:68]
    assert len(info_hash) == NODE_ID_LENGTH and len(peer_id) == NODE_ID_LENGTH
    return info_hash, peer_id, supports_extensions


# Parents: encode_extended_message, FakeMetadataPeer (tests)
# Keywords: frame, length prefix, message id, peer wire
def encode_frame(message_id: int, payload: bytes) -> bytes:
    assert 0 <= message_id <= 255 and isinstance(payload, bytes)
    result = struct.pack("!I", 1 + len(payload)) + bytes([message_id]) + payload
    assert len(result) == FRAME_HEADER_LENGTH + 1 + len(payload)
    return result


# Parents: receive_frame
# Keywords: frame, length, header, decode
def decode_frame_length(header: bytes) -> int:
    assert len(header) == FRAME_HEADER_LENGTH
    result = struct.unpack("!I", header)[0]
    assert result >= 0
    return result


# Parents: encode_extended_handshake, encode_metadata_request, FakeMetadataPeer (tests)
# Keywords: extended message, bep10, bencode, frame
def encode_extended_message(extension_id: int, payload: Dict[bytes, Any], trailing: bytes = b"") -> bytes:
    assert 0 <= extension_id <= 255 and isinstance(payload, dict)
    result = encode_frame(MESSAGE_EXTENDED, bytes([extension_id]) + encode_bencode(payload) + trailing)
    assert result[4] == MESSAGE_EXTENDED and result[5] == extension_id
    return result


# Parents: exchange_extended_handshake, request_piece, FakeMetadataPeer (tests)
# Keywords: extended message, decode, trailing bytes, bep10
def decode_extended_message(body: bytes) -> Tuple[int, Dict[bytes, Any], bytes]:
    assert isinstance(body, bytes)
    if len(body) < 2:
        raise ValueError("bad_extended_message")
    extension_id = body[0]
    payload, next_index = decode_value_at(body, 1)
    if not isinstance(payload, dict):
        raise ValueError("bad_extended_message")
    trailing = body[next_index:]
    assert 0 <= extension_id <= 255
    return extension_id, payload, trailing


# Parents: exchange_extended_handshake
# Keywords: extended handshake, ut_metadata, advertise, bep10
def encode_extended_handshake() -> bytes:
    assert UT_METADATA_LOCAL_ID > 0
    result = encode_extended_message(EXTENDED_HANDSHAKE_ID, {b"m": {b"ut_metadata": UT_METADATA_LOCAL_ID}, b"v": CLIENT_VERSION})
    assert result[5] == EXTENDED_HANDSHAKE_ID
    return result


# Parents: exchange_extended_handshake
# Keywords: extended handshake, metadata_size, ut_metadata id, validation
def read_extended_handshake(payload: Dict[bytes, Any]) -> Tuple[int, int]:
    assert isinstance(payload, dict)
    extensions = payload.get(b"m")
    their_id = extensions.get(b"ut_metadata") if isinstance(extensions, dict) else None
    if not is_plain_int(their_id) or not 1 <= their_id <= 255:
        raise ValueError("no_ut_metadata")
    metadata_size = payload.get(b"metadata_size")
    if not is_plain_int(metadata_size) or metadata_size <= 0:
        raise ValueError("no_ut_metadata")
    if metadata_size > MAX_METADATA_SIZE:
        raise ValueError("too_large")
    assert 1 <= their_id <= 255 and 0 < metadata_size <= MAX_METADATA_SIZE
    return their_id, metadata_size


# Parents: request_piece
# Keywords: ut_metadata, request, piece, bep9
def encode_metadata_request(their_id: int, piece: int) -> bytes:
    assert 1 <= their_id <= 255 and piece >= 0
    result = encode_extended_message(their_id, {b"msg_type": MSG_TYPE_REQUEST, b"piece": piece})
    assert result[5] == their_id
    return result


# Parents: request_piece, FakeMetadataPeer (tests)
# Keywords: ut_metadata, data, reject, decode, bep9
def decode_metadata_message(payload: Dict[bytes, Any]) -> Tuple[int, int, Optional[int]]:
    assert isinstance(payload, dict)
    msg_type = payload.get(b"msg_type")
    piece = payload.get(b"piece")
    total_size = payload.get(b"total_size")
    if not is_plain_int(msg_type) or not is_plain_int(piece) or msg_type < 0 or piece < 0:
        raise ValueError("bad_piece")
    if total_size is not None and (not is_plain_int(total_size) or total_size < 0):
        raise ValueError("bad_piece")
    assert msg_type >= 0 and piece >= 0
    return msg_type, piece, total_size


# Parents: fetch_metadata, expected_piece_length
# Keywords: piece count, metadata size, ceiling
def piece_count(metadata_size: int) -> int:
    assert metadata_size > 0
    result = (metadata_size + METADATA_PIECE_SIZE - 1) // METADATA_PIECE_SIZE
    assert result >= 1 and (result - 1) * METADATA_PIECE_SIZE < metadata_size <= result * METADATA_PIECE_SIZE
    return result


# Parents: fetch_metadata
# Keywords: piece length, last piece, metadata size
def expected_piece_length(metadata_size: int, piece: int) -> int:
    assert metadata_size > 0 and 0 <= piece < piece_count(metadata_size)
    result = min(METADATA_PIECE_SIZE, metadata_size - piece * METADATA_PIECE_SIZE)
    assert 0 < result <= METADATA_PIECE_SIZE
    return result
