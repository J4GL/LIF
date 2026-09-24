"""Node ID and transaction ID generation for the DHT crawler."""
import os
from typing import Any, Iterable, List, Tuple

NODE_ID_LENGTH = 20
NEIGHBOR_PREFIX_LENGTH = 15
TRANSACTION_ID_LENGTH = 2


# Parents: DhtCrawler.__init__, DhtCrawler.send_find_node
# Keywords: node id, random, identity, dht
def generate_node_id() -> bytes:
    assert NODE_ID_LENGTH == 20
    result = os.urandom(NODE_ID_LENGTH)
    assert len(result) == NODE_ID_LENGTH
    return result


# Parents: DhtCrawler.send_find_node, DhtCrawler.handle_query
# Keywords: neighbor id, closeness, routing table, sybil
def neighbor_node_id(remote_id: bytes, own_id: bytes) -> bytes:
    assert len(remote_id) == NODE_ID_LENGTH and len(own_id) == NODE_ID_LENGTH
    result = remote_id[:NEIGHBOR_PREFIX_LENGTH] + own_id[NEIGHBOR_PREFIX_LENGTH:]
    assert len(result) == NODE_ID_LENGTH
    assert result[:NEIGHBOR_PREFIX_LENGTH] == remote_id[:NEIGHBOR_PREFIX_LENGTH]
    return result


# Parents: DhtCrawler.send_find_node
# Keywords: transaction id, krpc, random
def generate_transaction_id() -> bytes:
    assert TRANSACTION_ID_LENGTH == 2
    result = os.urandom(TRANSACTION_ID_LENGTH)
    assert len(result) == TRANSACTION_ID_LENGTH
    return result


# Parents: DhtCrawler.send_find_node, DhtCrawler.is_foreign_node, DhtCrawler.handle_query,
#          DhtCrawler.read_info_hash
# Keywords: validation, node id, info hash, length
def is_valid_node_id(value: Any) -> bool:
    assert NODE_ID_LENGTH > 0
    result = isinstance(value, bytes) and len(value) == NODE_ID_LENGTH
    assert isinstance(result, bool)
    return result

LOOKUP_TRANSACTION_PREFIX = b"L"
LOOKUP_TRANSACTION_ID_LENGTH = 4
PEER_ID_PREFIX = b"-DS0002-"
NodeTuple = Tuple[bytes, str, int]


# Parents: select_closest_nodes, LookupManager.add_to_shortlist
# Keywords: xor, distance, kademlia, closeness
def xor_distance(left_id: bytes, right_id: bytes) -> int:
    assert len(left_id) == NODE_ID_LENGTH and len(right_id) == NODE_ID_LENGTH
    result = int.from_bytes(left_id, "big") ^ int.from_bytes(right_id, "big")
    assert result >= 0
    return result


# Parents: DhtCrawler.start_pending_lookups, LookupManager.add_to_shortlist, LookupManager.send_round
# Keywords: closest, sort, kademlia, shortlist
def select_closest_nodes(nodes: Iterable[NodeTuple], target: bytes, count: int) -> List[NodeTuple]:
    assert len(target) == NODE_ID_LENGTH and count >= 0
    result = sorted(nodes, key=lambda node: xor_distance(node[0], target))[:count]
    assert len(result) <= count
    return result


# Parents: LookupManager.send_to
# Keywords: transaction id, lookup, prefix, counter, unique in flight
def lookup_transaction_id(sequence: int) -> bytes:
    assert sequence >= 0 and LOOKUP_TRANSACTION_ID_LENGTH > len(LOOKUP_TRANSACTION_PREFIX)
    counter_length = LOOKUP_TRANSACTION_ID_LENGTH - len(LOOKUP_TRANSACTION_PREFIX)
    result = LOOKUP_TRANSACTION_PREFIX + (sequence % (1 << (8 * counter_length))).to_bytes(counter_length, "big")
    assert len(result) == LOOKUP_TRANSACTION_ID_LENGTH
    return result


# Parents: opening_messages
# Keywords: peer id, client prefix, random, handshake
def generate_peer_id() -> bytes:
    assert len(PEER_ID_PREFIX) < NODE_ID_LENGTH
    result = PEER_ID_PREFIX + os.urandom(NODE_ID_LENGTH - len(PEER_ID_PREFIX))
    assert len(result) == NODE_ID_LENGTH
    return result
