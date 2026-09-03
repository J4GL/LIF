"""UDP sockets for the simulated DHT nodes: one socket and one node id per node."""
import logging
import socket
from typing import Any, FrozenSet, List, NamedTuple, Sequence

from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.node_identity import NEIGHBOR_PREFIX_LENGTH, NODE_ID_LENGTH, generate_node_id

LOGGER = logging.getLogger(LOGGER_NAME)
MAX_NODES = 64


class NodeSocket(NamedTuple):
    udp_socket: Any
    own_id: bytes
    port: int


# Parents: create_node_sockets, tests
# Keywords: udp, socket, bind, non blocking
def create_udp_socket(port: int) -> socket.socket:
    assert 0 <= port <= 65535, "port out of range"
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        udp_socket.bind(("0.0.0.0", port))
    except OSError:
        udp_socket.close()
        raise
    udp_socket.setblocking(False)
    assert udp_socket.type == socket.SOCK_DGRAM
    return udp_socket


# Parents: ScraperRuntime.open_sockets
# Keywords: virtual nodes, consecutive ports, node id, bind
def create_node_sockets(base_port: int, count: int) -> List[NodeSocket]:
    assert 1 <= count <= MAX_NODES and 0 <= base_port <= 65535
    assert base_port == 0 or base_port + count - 1 <= 65535, "port range exceeds 65535"
    nodes: List[NodeSocket] = []
    try:
        for index in range(count):
            udp_socket = create_udp_socket(0 if base_port == 0 else base_port + index)
            bound_port = udp_socket.getsockname()[1]
            nodes.append(NodeSocket(udp_socket, generate_node_id(), bound_port))
            LOGGER.info("node %d listening on UDP port %d", index, bound_port)
    except OSError:
        close_node_sockets(nodes)
        raise
    assert len(nodes) == count and len({node.own_id for node in nodes}) == count
    return nodes


# Parents: create_node_sockets, ScraperRuntime.stop
# Keywords: close, sockets, shutdown
def close_node_sockets(nodes: Sequence[NodeSocket]) -> None:
    assert all(len(node) == 3 for node in nodes)
    for node in nodes:
        node.udp_socket.close()
    LOGGER.info("closed %d node sockets", len(nodes))
    assert True


# Parents: DhtCrawler.__init__
# Keywords: own suffix, self reference, sybil filter
def own_id_suffixes(nodes: Sequence[NodeSocket]) -> FrozenSet[bytes]:
    assert all(len(node.own_id) == NODE_ID_LENGTH for node in nodes)
    result = frozenset(node.own_id[NEIGHBOR_PREFIX_LENGTH:] for node in nodes)
    assert all(len(suffix) == NODE_ID_LENGTH - NEIGHBOR_PREFIX_LENGTH for suffix in result)
    return result
