"""Bencode encoder and decoder (BEP 3) for KRPC messages."""
from typing import Any, Dict, List, Tuple

BencodeValue = Any


# Parents: krpc_messages.read_error_code, krpc_messages.read_announced_port, peer_wire_messages.read_extended_handshake,
#          peer_wire_messages.decode_metadata_message, torrent_info_summary.read_file_entries,
#          torrent_info_summary.summarize_info_dict, DhtCrawler.handle_samples
# Keywords: bencode int, bool excluded, type check
def is_plain_int(value: Any) -> bool:
    assert value is not Ellipsis
    result = type(value) is int
    assert isinstance(result, bool)
    return result


# Parents: encode_bencode
# Keywords: bencode, encode, integer, serialize
def encode_int(value: int) -> bytes:
    assert type(value) is int, "value must be int"
    result = b"i" + str(value).encode("ascii") + b"e"
    assert result[:1] == b"i" and result[-1:] == b"e"
    return result


# Parents: encode_bencode, encode_dict
# Keywords: bencode, encode, bytes, string, serialize
def encode_bytes(value: bytes) -> bytes:
    assert isinstance(value, bytes), "value must be bytes"
    result = str(len(value)).encode("ascii") + b":" + value
    assert result.endswith(value)
    return result


# Parents: encode_bencode
# Keywords: bencode, encode, list, serialize
def encode_list(values: List[BencodeValue]) -> bytes:
    assert isinstance(values, list), "values must be list"
    result = b"l" + b"".join(encode_bencode(item) for item in values) + b"e"
    assert result[:1] == b"l" and result[-1:] == b"e"
    return result


# Parents: encode_bencode
# Keywords: bencode, encode, dict, sorted keys, serialize
def encode_dict(values: Dict[Any, BencodeValue]) -> bytes:
    assert isinstance(values, dict), "values must be dict"
    assert all(isinstance(key, (bytes, str)) for key in values), "keys must be bytes or str"
    normalized = {key.encode("utf-8") if isinstance(key, str) else key: value for key, value in values.items()}
    parts = [encode_bytes(key) + encode_bencode(normalized[key]) for key in sorted(normalized)]
    result = b"d" + b"".join(parts) + b"e"
    assert result[:1] == b"d" and result[-1:] == b"e"
    return result


# Parents: encode_list, encode_dict, DhtCrawler.send_message
# Keywords: bencode, encode, dispatch, serialize
def encode_bencode(value: BencodeValue) -> bytes:
    assert value is not None, "value must not be None"
    if isinstance(value, bool):
        raise TypeError("bool is not a bencode type")
    if isinstance(value, int):
        result = encode_int(value)
    elif isinstance(value, bytes):
        result = encode_bytes(value)
    elif isinstance(value, str):
        result = encode_bytes(value.encode("utf-8"))
    elif isinstance(value, list):
        result = encode_list(value)
    elif isinstance(value, dict):
        result = encode_dict(value)
    else:
        raise TypeError("unsupported bencode type: %s" % type(value).__name__)
    assert isinstance(result, bytes) and len(result) >= 2
    return result


# Parents: decode_value_at
# Keywords: bencode, decode, integer, parse
def decode_int_at(data: bytes, index: int) -> Tuple[int, int]:
    assert 0 <= index < len(data) and data[index] == ord("i"), "expected 'i'"
    end = data.find(b"e", index + 1)
    if end < 0:
        raise ValueError("unterminated integer at %d" % index)
    text = data[index + 1:end]
    digits = text[1:] if text[:1] == b"-" else text
    if not digits.isdigit() or (len(digits) > 1 and digits[:1] == b"0") or text == b"-0":
        raise ValueError("invalid integer at %d" % index)
    value = int(text)
    next_index = end + 1
    assert next_index > index
    return value, next_index


# Parents: decode_value_at, decode_dict_at
# Keywords: bencode, decode, bytes, string, parse
def decode_bytes_at(data: bytes, index: int) -> Tuple[bytes, int]:
    assert 0 <= index < len(data) and data[index:index + 1].isdigit(), "expected digit"
    colon = data.find(b":", index)
    if colon < 0:
        raise ValueError("missing ':' at %d" % index)
    length_text = data[index:colon]
    if not length_text.isdigit():
        raise ValueError("invalid string length at %d" % index)
    length = int(length_text)
    start = colon + 1
    next_index = start + length
    if next_index > len(data):
        raise ValueError("truncated string at %d" % index)
    value = data[start:next_index]
    assert len(value) == length
    return value, next_index


# Parents: decode_value_at
# Keywords: bencode, decode, list, parse
def decode_list_at(data: bytes, index: int) -> Tuple[List[BencodeValue], int]:
    assert 0 <= index < len(data) and data[index] == ord("l"), "expected 'l'"
    items: List[BencodeValue] = []
    position = index + 1
    while True:
        if position >= len(data):
            raise ValueError("unterminated list at %d" % index)
        if data[position] == ord("e"):
            break
        item, position = decode_value_at(data, position)
        items.append(item)
    next_index = position + 1
    assert next_index > index
    return items, next_index


# Parents: decode_value_at
# Keywords: bencode, decode, dict, parse
def decode_dict_at(data: bytes, index: int) -> Tuple[Dict[bytes, BencodeValue], int]:
    assert 0 <= index < len(data) and data[index] == ord("d"), "expected 'd'"
    items: Dict[bytes, BencodeValue] = {}
    position = index + 1
    while True:
        if position >= len(data):
            raise ValueError("unterminated dict at %d" % index)
        if data[position] == ord("e"):
            break
        if not data[position:position + 1].isdigit():
            raise ValueError("dict key is not a string at %d" % position)
        key, position = decode_bytes_at(data, position)
        if position >= len(data):
            raise ValueError("dict value missing at %d" % position)
        value, position = decode_value_at(data, position)
        items[key] = value
    next_index = position + 1
    assert all(isinstance(key, bytes) for key in items)
    return items, next_index


# Parents: decode_bencode, decode_list_at, decode_dict_at
# Keywords: bencode, decode, dispatch, parse
def decode_value_at(data: bytes, index: int) -> Tuple[BencodeValue, int]:
    assert 0 <= index < len(data), "index out of range"
    prefix = data[index:index + 1]
    if prefix == b"i":
        value, next_index = decode_int_at(data, index)
    elif prefix == b"l":
        value, next_index = decode_list_at(data, index)
    elif prefix == b"d":
        value, next_index = decode_dict_at(data, index)
    elif prefix.isdigit():
        value, next_index = decode_bytes_at(data, index)
    else:
        raise ValueError("unknown bencode prefix %r at %d" % (prefix, index))
    assert index < next_index <= len(data)
    return value, next_index


# Parents: DhtCrawler.handle_datagram
# Keywords: bencode, decode, entry point, parse
def decode_bencode(data: bytes) -> BencodeValue:
    assert isinstance(data, bytes), "data must be bytes"
    if len(data) == 0:
        raise ValueError("empty input")
    value, next_index = decode_value_at(data, 0)
    if next_index != len(data):
        raise ValueError("trailing data after position %d" % next_index)
    assert next_index == len(data)
    return value
