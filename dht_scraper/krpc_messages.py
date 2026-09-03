"""KRPC message builders and compact node encoding (BEP 5)."""
import hmac
import ipaddress
import socket
import struct
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dht_scraper.bencode_codec import is_plain_int
from dht_scraper.node_identity import NODE_ID_LENGTH

COMPACT_ADDRESS_LENGTH = 6
COMPACT_NODE_LENGTH = NODE_ID_LENGTH + COMPACT_ADDRESS_LENGTH
TOKEN_LENGTH = 8
ERROR_GENERIC = 201
ERROR_SERVER = 202
ERROR_PROTOCOL = 203
ERROR_METHOD_UNKNOWN = 204
KNOWN_ERROR_CODES = (ERROR_GENERIC, ERROR_SERVER, ERROR_PROTOCOL, ERROR_METHOD_UNKNOWN)

QUERY_SAMPLE_INFOHASHES = b"sample_infohashes"
MAX_SAMPLE_INTERVAL = 21600

Node = Tuple[bytes, str, int]
Address = Tuple[str, int]
Message = Dict[bytes, Any]


# Parents: encode_compact_nodes
# Keywords: compact address, ipv4, port, pack
def encode_compact_address(ip: str, port: int) -> bytes:
    assert 0 <= port <= 65535, "port out of range"
    result = socket.inet_aton(ip) + struct.pack("!H", port)
    assert len(result) == COMPACT_ADDRESS_LENGTH
    return result


# Parents: decode_compact_nodes
# Keywords: compact address, ipv4, port, unpack
def decode_compact_address(data: bytes) -> Tuple[str, int]:
    assert len(data) == COMPACT_ADDRESS_LENGTH, "compact address must be 6 bytes"
    ip = socket.inet_ntoa(data[:4])
    port = struct.unpack("!H", data[4:6])[0]
    assert 0 <= port <= 65535
    return ip, port


# Parents: DhtCrawler.handle_find_node_query, DhtCrawler.handle_get_peers_query
# Keywords: compact nodes, encode, routing, find_node
def encode_compact_nodes(nodes: Sequence[Node]) -> bytes:
    assert all(len(node[0]) == NODE_ID_LENGTH for node in nodes), "node id must be 20 bytes"
    result = b"".join(node_id + encode_compact_address(ip, port) for node_id, ip, port in nodes)
    assert len(result) == COMPACT_NODE_LENGTH * len(nodes)
    return result


# Parents: DhtCrawler.add_node, DhtCrawler.handle_response
# Keywords: routable, address filter, loopback, multicast, port
def is_routable_address(ip: str, port: int) -> bool:
    assert isinstance(ip, str) and isinstance(port, int)
    try:
        parsed = ipaddress.ip_address(ip)
    except ValueError:
        return False
    result = (
        parsed.version == 4
        and 0 < port <= 65535
        and not (parsed.is_unspecified or parsed.is_loopback or parsed.is_multicast or parsed.is_link_local)
    )
    assert isinstance(result, bool)
    return result


# Parents: DhtCrawler.handle_response
# Keywords: compact nodes, decode, routing, find_node
def decode_compact_nodes(data: bytes) -> List[Node]:
    assert isinstance(data, bytes), "data must be bytes"
    nodes: List[Node] = []
    last_start = len(data) - COMPACT_NODE_LENGTH
    for offset in range(0, last_start + 1, COMPACT_NODE_LENGTH):
        node_id = data[offset:offset + NODE_ID_LENGTH]
        ip, port = decode_compact_address(data[offset + NODE_ID_LENGTH:offset + COMPACT_NODE_LENGTH])
        nodes.append((node_id, ip, port))
    assert len(nodes) == len(data) // COMPACT_NODE_LENGTH
    return nodes


# Parents: DhtCrawler.send_find_node, DhtCrawler.handle_error
# Keywords: krpc, query, find_node, build
def build_find_node_query(transaction_id: bytes, node_id: bytes, target_id: bytes) -> Message:
    assert len(node_id) == NODE_ID_LENGTH and len(target_id) == NODE_ID_LENGTH
    result: Message = {
        b"t": transaction_id,
        b"y": b"q",
        b"q": b"find_node",
        b"a": {b"id": node_id, b"target": target_id},
    }
    assert result[b"y"] == b"q" and result[b"q"] == b"find_node"
    return result


# Parents: DhtCrawler.handle_ping_query, DhtCrawler.handle_announce_peer_query
# Keywords: krpc, response, ping, build
def build_ping_response(transaction_id: bytes, node_id: bytes) -> Message:
    assert len(node_id) == NODE_ID_LENGTH
    result: Message = {b"t": transaction_id, b"y": b"r", b"r": {b"id": node_id}}
    assert result[b"y"] == b"r" and result[b"r"][b"id"] == node_id
    return result


# Parents: DhtCrawler.handle_find_node_query
# Keywords: krpc, response, find_node, nodes, build
def build_find_node_response(transaction_id: bytes, node_id: bytes, compact_nodes: bytes) -> Message:
    assert len(node_id) == NODE_ID_LENGTH and len(compact_nodes) % COMPACT_NODE_LENGTH == 0
    result: Message = {b"t": transaction_id, b"y": b"r", b"r": {b"id": node_id, b"nodes": compact_nodes}}
    assert result[b"r"][b"nodes"] == compact_nodes
    return result


# Parents: DhtCrawler.handle_get_peers_query
# Keywords: krpc, response, get_peers, token, build
def build_get_peers_response(transaction_id: bytes, node_id: bytes, token: bytes, compact_nodes: bytes) -> Message:
    assert len(node_id) == NODE_ID_LENGTH and len(token) > 0
    result: Message = {
        b"t": transaction_id,
        b"y": b"r",
        b"r": {b"id": node_id, b"token": token, b"nodes": compact_nodes},
    }
    assert all(key in result[b"r"] for key in (b"id", b"token", b"nodes"))
    return result


# Parents: DhtCrawler.handle_query, DhtCrawler.handle_get_peers_query, DhtCrawler.handle_announce_peer_query
# Keywords: krpc, error, response, build
def build_error_response(transaction_id: bytes, code: int, text: bytes) -> Message:
    assert code in KNOWN_ERROR_CODES, "unknown error code"
    result: Message = {b"t": transaction_id, b"y": b"e", b"e": [code, text]}
    assert result[b"y"] == b"e" and result[b"e"] == [code, text]
    return result


# Parents: DhtCrawler.handle_get_peers_query, DhtCrawler.handle_announce_peer_query
# Keywords: token, hmac, announce, secret
def make_token(secret: bytes, ip: str) -> bytes:
    assert len(secret) > 0, "secret must not be empty"
    result = hmac.digest(secret, ip.encode("ascii"), "sha1")[:TOKEN_LENGTH]
    assert len(result) == TOKEN_LENGTH
    return result


# Parents: DhtCrawler.send_lookup_query
# Keywords: krpc, query, get_peers, lookup, build
def build_get_peers_query(transaction_id: bytes, node_id: bytes, info_hash: bytes) -> Message:
    assert len(node_id) == NODE_ID_LENGTH and len(info_hash) == NODE_ID_LENGTH
    result: Message = {
        b"t": transaction_id,
        b"y": b"q",
        b"q": b"get_peers",
        b"a": {b"id": node_id, b"info_hash": info_hash},
    }
    assert result[b"q"] == b"get_peers"
    return result


# Parents: DhtCrawler.send_sample_infohashes
# Keywords: krpc, query, sample_infohashes, bep51, build
def build_sample_infohashes_query(transaction_id: bytes, node_id: bytes, target_id: bytes) -> Message:
    assert len(node_id) == NODE_ID_LENGTH and len(target_id) == NODE_ID_LENGTH
    result: Message = {
        b"t": transaction_id,
        b"y": b"q",
        b"q": QUERY_SAMPLE_INFOHASHES,
        b"a": {b"id": node_id, b"target": target_id},
    }
    assert result[b"q"] == QUERY_SAMPLE_INFOHASHES
    return result


# Parents: DhtCrawler.handle_sample_infohashes_query
# Keywords: krpc, response, sample_infohashes, bep51, build
def build_sample_infohashes_response(
    transaction_id: bytes, node_id: bytes, interval: int, compact_nodes: bytes, num: int, samples: bytes
) -> Message:
    assert len(node_id) == NODE_ID_LENGTH and 0 <= interval <= MAX_SAMPLE_INTERVAL
    assert len(samples) % NODE_ID_LENGTH == 0 and num >= 0 and len(compact_nodes) % COMPACT_NODE_LENGTH == 0
    result: Message = {
        b"t": transaction_id,
        b"y": b"r",
        b"r": {b"id": node_id, b"interval": interval, b"nodes": compact_nodes, b"num": num, b"samples": samples},
    }
    assert all(key in result[b"r"] for key in (b"interval", b"nodes", b"num", b"samples"))
    return result


# Parents: tests (lookup and crawler tests)
# Keywords: compact peers, encode, values, get_peers
def encode_compact_peers(peers: Sequence[Address]) -> List[bytes]:
    assert all(len(peer) == 2 for peer in peers)
    result = [encode_compact_address(ip, port) for ip, port in peers]
    assert len(result) == len(peers)
    return result


# Parents: LookupManager.handle_response
# Keywords: compact peers, decode, values, get_peers
def decode_compact_peers(values: Any) -> List[Address]:
    assert COMPACT_ADDRESS_LENGTH == 6
    if not isinstance(values, list):
        return []
    result = [decode_compact_address(item) for item in values if isinstance(item, bytes) and len(item) == COMPACT_ADDRESS_LENGTH]
    assert all(0 <= port <= 65535 for _, port in result)
    return result


# Parents: DhtCrawler.handle_samples
# Keywords: samples, decode, bep51, info hashes
def decode_samples(data: Any) -> List[bytes]:
    assert NODE_ID_LENGTH == 20
    if not isinstance(data, bytes):
        return []
    result = [data[offset:offset + NODE_ID_LENGTH] for offset in range(0, len(data) - NODE_ID_LENGTH + 1, NODE_ID_LENGTH)]
    assert len(result) == len(data) // NODE_ID_LENGTH
    return result


# Parents: DhtCrawler.handle_error
# Keywords: krpc, error, code, parse
def read_error_code(message: Message) -> Optional[int]:
    assert isinstance(message, dict)
    error = message.get(b"e")
    result: Optional[int] = None
    if isinstance(error, list) and error and is_plain_int(error[0]):
        result = error[0]
    assert result is None or isinstance(result, int)
    return result


# Parents: DhtCrawler.handle_announce_peer_query
# Keywords: announce_peer, port, implied_port, peer address
def read_announced_port(args: Dict[bytes, Any], source_port: int) -> Optional[int]:
    assert isinstance(args, dict) and 0 <= source_port <= 65535
    implied = args.get(b"implied_port")
    if is_plain_int(implied) and implied != 0:
        port: Any = source_port
    else:
        port = args.get(b"port")
    result: Optional[int] = None
    if is_plain_int(port) and 0 < port <= 65535:
        result = port
    assert result is None or 0 < result <= 65535
    return result
