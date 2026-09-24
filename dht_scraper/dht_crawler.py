"""DHT crawler thread: N simulated nodes per address family share one selector loop, crawl with BEP 51, answer queries."""
import collections
import itertools
import logging
import os
import selectors
import socket
import threading
import time
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Tuple

from dht_scraper.bencode_codec import decode_bencode, encode_bencode, is_plain_int
from dht_scraper.bounded_recent_map import remember
from dht_scraper.dht_lookup import DEFAULT_LOOKUP_LIMITS, LookupLimits, LookupManager
from dht_scraper.dht_node_sockets import NodeSocket, own_id_suffixes
from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.krpc_messages import (
    ERROR_METHOD_UNKNOWN,
    ERROR_PROTOCOL,
    MAX_SAMPLE_INTERVAL,
    NODES6_KEY,
    NODES_KEY,
    QUERY_SAMPLE_INFOHASHES,
    Address,
    Message,
    Node,
    build_error_response,
    build_find_node_query,
    build_find_node_response,
    build_get_peers_query,
    build_get_peers_response,
    build_ping_response,
    build_sample_infohashes_query,
    build_sample_infohashes_response,
    decode_compact_nodes,
    decode_compact_nodes6,
    decode_samples,
    encode_compact_nodes,
    is_routable_address,
    make_token,
    read_announced_port,
    read_error_code,
)
from dht_scraper.node_identity import (
    LOOKUP_TRANSACTION_ID_LENGTH,
    NEIGHBOR_PREFIX_LENGTH,
    NODE_ID_LENGTH,
    generate_node_id,
    generate_transaction_id,
    is_valid_node_id,
    neighbor_node_id,
    select_closest_nodes,
)
from dht_scraper.torrent_catalog import SOURCE_ANNOUNCE_PEER, SOURCE_GET_PEERS, SOURCE_SAMPLE, TorrentCatalog

LOGGER = logging.getLogger(LOGGER_NAME)

DEFAULT_BOOTSTRAP_NODES: List[Tuple[str, int]] = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.aelitis.com", 6881),
    ("dht.libtorrent.org", 25401),
]
MAX_DATAGRAM_SIZE = 65535
RECENT_NODES_SIZE = 8
RECENT_HASHES_SIZE = 64
SAMPLES_PER_RESPONSE = 20
OWN_SAMPLE_INTERVAL = 300
BOOTSTRAP_RETRY_SECONDS = 2.0
MAX_RECEIVE_PER_SOCKET = 1000
MAX_BACKOFF_ENTRIES = 50000
TOKEN_SECRET_LENGTH = 16
LOOKUP_SEED_SCAN = 2000
LOOKUP_SEED_COUNT = 16
LOOKUP_FETCHABLE_TARGET = 512
HINT_NODES_SIZE = 65536
SOCKET_AFFINITY_SIZE = 65536
PER_ADDRESS_WINDOW_SECONDS = 10.0
PER_ADDRESS_MAX_PACKETS = 40
SEND_WINDOW_ENTRIES = 65536
COUNTER_KEYS = (
    "packets_sent", "packets_received", "queries_received", "responses_received", "errors_received",
    "samples_received", "sample_responses", "announced_peers", "lookups_started", "sample_queries_sent", "find_node_sent",
    "packets_throttled",
)
LOOKUP_KEYS = (
    "lookups_finished", "lookups_with_peers", "lookup_peers_found", "lookup_queries_sent",
    "lookup_hint_queries", "lookup_hint_answers", "lookup_hint_values", "active_lookups",
)
STATS_KEYS = COUNTER_KEYS + LOOKUP_KEYS + ("queue_size", "queue_size6")


# Parents: DhtCrawler.run
# Keywords: pacing, interval, drift, catch up
def next_batch_time(due: float, now: float, interval: float) -> float:
    assert interval > 0 and now >= due
    result = due + interval
    if now - due > interval:
        result = now + interval
    assert result > now - interval
    return result


# Parents: DhtCrawler.bootstrap (default resolver)
# Keywords: dns, every address, ipv4, ipv6, getaddrinfo
def resolve_addresses(host: str, port: int, family: int) -> List[str]:
    assert host and 0 < port <= 65535 and family in (socket.AF_INET, socket.AF_INET6)
    infos = socket.getaddrinfo(host, port, family, socket.SOCK_DGRAM)
    result = sorted({info[4][0] for info in infos})
    assert all(isinstance(ip, str) for ip in result)
    return result


# Parents: DhtCrawler (every family dispatch)
# Keywords: address family, ipv6, colon
def address_family(ip: str) -> int:
    assert isinstance(ip, str)
    result = socket.AF_INET6 if ":" in ip else socket.AF_INET
    assert result in (socket.AF_INET, socket.AF_INET6)
    return result


class FamilyState:
    """Crawl queue, recent nodes and socket rotation of one address family."""

    __slots__ = ("family", "node_sockets", "socket_cycle", "node_queue", "recent_nodes", "nodes_key", "last_bootstrap_time")

    # Parents: DhtCrawler.__init__
    # Keywords: family, queue, recent nodes, rotation
    def __init__(self, family: int, node_sockets: List[NodeSocket], max_queue: int) -> None:
        assert node_sockets and all(node.family == family for node in node_sockets)
        self.family = family
        self.node_sockets = node_sockets
        self.socket_cycle = itertools.cycle(node_sockets)
        self.node_queue: Deque[Node] = collections.deque(maxlen=max_queue)
        self.recent_nodes: Deque[Node] = collections.deque(maxlen=RECENT_NODES_SIZE)
        self.nodes_key = NODES6_KEY if family == socket.AF_INET6 else NODES_KEY
        self.last_bootstrap_time = float("-inf")
        assert not self.node_queue


class DhtCrawler:
    """Owns the node sockets, the crawl queue and the lookups. Runs in one thread.

    The counters in `stats` have a single writer (the crawler thread); other threads read a
    dict copy through `snapshot_stats`, which is atomic under the interpreter lock.
    """

    # Parents: ScraperRuntime.create_crawler, tests
    # Keywords: crawler, init, sockets, queue, stats
    def __init__(
        self,
        node_sockets: Sequence[NodeSocket],
        catalog: TorrentCatalog,
        bootstrap_nodes: Sequence[Tuple[str, int]],
        batch_size: int = 5,
        interval: float = 0.1,
        max_queue: int = 20000,
        clock: Callable[[], float] = time.monotonic,
        lookup_fetchable_target: int = LOOKUP_FETCHABLE_TARGET,
        lookup_limits: LookupLimits = DEFAULT_LOOKUP_LIMITS,
        resolver: Callable[[str, int, int], List[str]] = resolve_addresses,
    ) -> None:
        assert len(node_sockets) >= 1 and batch_size >= 1 and interval > 0 and max_queue >= 1 and lookup_fetchable_target >= 0
        assert len(bootstrap_nodes) > 0, "bootstrap list must not be empty"
        self.node_sockets = list(node_sockets)
        self.selector: Optional[selectors.BaseSelector] = None
        self.catalog = catalog
        self.bootstrap_nodes = list(bootstrap_nodes)
        self.resolver = resolver
        self.resolved_bootstrap_hosts: Dict[int, Dict[str, List[str]]] = {}
        self.batch_size = batch_size
        self.interval = interval
        self.clock = clock
        self.own_suffixes = own_id_suffixes(self.node_sockets)
        self.families: Dict[int, FamilyState] = {}
        for family in (socket.AF_INET, socket.AF_INET6):
            members = [node for node in self.node_sockets if node.family == family]
            if members:
                self.families[family] = FamilyState(family, members, max_queue)
                self.resolved_bootstrap_hosts[family] = {}
        primary = next(iter(self.families.values()))
        self.node_queue = primary.node_queue
        self.recent_nodes = primary.recent_nodes
        self.token_secret = os.urandom(TOKEN_SECRET_LENGTH)
        self.recent_hashes: Deque[bytes] = collections.deque(maxlen=RECENT_HASHES_SIZE)
        self.sample_backoff: "collections.OrderedDict[Address, float]" = collections.OrderedDict()
        self.fallback_sent: "collections.OrderedDict[Address, None]" = collections.OrderedDict()
        self.recently_queued: "collections.OrderedDict[Address, None]" = collections.OrderedDict()
        self.hint_nodes: "collections.OrderedDict[bytes, Node]" = collections.OrderedDict()
        self.socket_affinity: "collections.OrderedDict[Address, NodeSocket]" = collections.OrderedDict()
        self.send_windows: "collections.OrderedDict[str, Tuple[float, int]]" = collections.OrderedDict()
        self.lookup_fetchable_target = lookup_fetchable_target
        self.stats: Dict[str, int] = {key: 0 for key in COUNTER_KEYS}
        self.lookup_manager = LookupManager(self.send_lookup_query, catalog.add_lookup_result, clock, self.is_foreign_node, catalog.add_peers, lookup_limits)
        assert len(self.node_queue) == 0 and self.families

    # Parents: send_crawl_query, send_lookup_query
    # Keywords: round robin, socket, virtual node, family
    def pick_node_socket(self, family: int = socket.AF_INET) -> Optional[NodeSocket]:
        assert family in (socket.AF_INET, socket.AF_INET6)
        state = self.families.get(family)
        result = next(state.socket_cycle) if state is not None else None
        assert result is None or result.family == family
        return result

    # Parents: send_find_node, send_sample_infohashes, send_lookup_query, handle_query, handle_ping_query,
    #          handle_find_node_query, handle_get_peers_query, handle_announce_peer_query,
    #          handle_sample_infohashes_query, read_info_hash, handle_error
    # Keywords: udp, send, bencode, datagram, per address cap
    def send_message(self, node_socket: NodeSocket, message: Message, address: Address) -> bool:
        assert isinstance(message, dict) and node_socket in self.node_sockets
        if not self.allow_send(address[0]):
            self.stats["packets_throttled"] += 1
            return False
        payload = encode_bencode(message)
        try:
            node_socket.udp_socket.sendto(payload, address)
        except OSError as error:
            LOGGER.debug("send to %s failed: %s", address, error)
            return False
        self.stats["packets_sent"] += 1
        assert self.stats["packets_sent"] >= 1
        return True

    # Parents: send_message
    # Keywords: rate limit, per address, ban avoidance, window
    def allow_send(self, ip: str) -> bool:
        assert isinstance(ip, str)
        now = self.clock()
        window = self.send_windows.get(ip)
        if window is None or now - window[0] >= PER_ADDRESS_WINDOW_SECONDS:
            remember(self.send_windows, ip, (now, 1), SEND_WINDOW_ENTRIES)
            return True
        if window[1] >= PER_ADDRESS_MAX_PACKETS:
            return False
        self.send_windows[ip] = (window[0], window[1] + 1)
        assert self.send_windows[ip][1] <= PER_ADDRESS_MAX_PACKETS
        return True

    # Parents: bootstrap, send_crawl_query, handle_error
    # Keywords: find_node, query, neighbor id, crawl
    def send_find_node(self, node_socket: NodeSocket, node: Node) -> None:
        node_id, ip, port = node
        assert is_valid_node_id(node_id) and isinstance(ip, str) and 0 <= port <= 65535
        query = build_find_node_query(generate_transaction_id(), neighbor_node_id(node_id, node_socket.own_id), generate_node_id())
        self.send_message(node_socket, query, (ip, port))
        self.stats["find_node_sent"] += 1
        assert query[b"q"] == b"find_node"

    # Parents: send_crawl_query
    # Keywords: sample_infohashes, bep51, query, crawl
    def send_sample_infohashes(self, node_socket: NodeSocket, node: Node) -> None:
        node_id, ip, port = node
        assert is_valid_node_id(node_id) and 0 <= port <= 65535
        query = build_sample_infohashes_query(generate_transaction_id(), neighbor_node_id(node_id, node_socket.own_id), generate_node_id())
        self.send_message(node_socket, query, (ip, port))
        self.stats["sample_queries_sent"] += 1
        assert query[b"q"] == QUERY_SAMPLE_INFOHASHES

    # Parents: crawl_step
    # Keywords: crawl policy, backoff, bep51 primary, find_node fallback
    def send_crawl_query(self, node: Node) -> None:
        assert len(node) == 3
        node_socket = self.pick_node_socket(address_family(node[1]))
        if node_socket is None:
            return
        address = (node[1], node[2])
        backoff_until = self.sample_backoff.get(address)
        if backoff_until is not None and backoff_until > self.clock():
            self.send_find_node(node_socket, node)
        else:
            self.send_sample_infohashes(node_socket, node)
        assert self.stats["packets_sent"] >= 0

    # Parents: LookupManager.send_to
    # Keywords: get_peers, lookup, query, neighbor id, socket affinity
    def send_lookup_query(self, transaction_id: bytes, node: Node, info_hash: bytes) -> None:
        assert len(transaction_id) == LOOKUP_TRANSACTION_ID_LENGTH and len(info_hash) == NODE_ID_LENGTH
        node_socket = self.socket_affinity.get((node[1], node[2])) or self.pick_node_socket(address_family(node[1]))
        if node_socket is None:
            return
        query = build_get_peers_query(transaction_id, neighbor_node_id(node[0], node_socket.own_id), info_hash)
        self.send_message(node_socket, query, (node[1], node[2]))
        assert query[b"q"] == b"get_peers"

    # Parents: run, crawl_step
    # Keywords: bootstrap, dns, router, every address, join, every family
    def bootstrap(self, families: Optional[Sequence[int]] = None) -> None:
        assert len(self.bootstrap_nodes) > 0
        for family in families if families is not None else list(self.families):
            state = self.families[family]
            state.last_bootstrap_time = self.clock()
            resolved = self.resolved_bootstrap_hosts[family]
            retry_failed = not any(resolved.values())
            contacted = 0
            for host, port in self.bootstrap_nodes:
                if host not in resolved or (retry_failed and not resolved[host]):
                    try:
                        resolved[host] = self.resolver(host, port, family)
                    except OSError as error:
                        LOGGER.debug("bootstrap: cannot resolve %s for family %d: %s", host, family, error)
                        resolved[host] = []
                for ip in resolved[host]:
                    contacted += 1
                    for node_socket in state.node_sockets:
                        self.send_find_node(node_socket, (node_socket.own_id, ip, port))
            LOGGER.info("bootstrap: contacted %d %s router addresses of %d hosts from %d nodes", contacted, "IPv6" if family == socket.AF_INET6 else "IPv4", len(self.bootstrap_nodes), len(state.node_sockets))
        assert all(state.last_bootstrap_time > float("-inf") for state in self.families.values() if families is None)

    # Parents: add_node, handle_response, LookupManager.add_to_shortlist
    # Keywords: node filter, self reference, routable, admission, family with sockets
    def is_foreign_node(self, node: Node) -> bool:
        node_id, ip, port = node
        assert isinstance(ip, str)
        result = (
            is_valid_node_id(node_id)
            and node_id[NEIGHBOR_PREFIX_LENGTH:] not in self.own_suffixes
            and address_family(ip) in self.families
            and is_routable_address(ip, port)
        )
        assert isinstance(result, bool)
        return result

    # Parents: handle_response, handle_query
    # Keywords: queue, node, filter, dedup
    def add_node(self, node: Node) -> None:
        assert len(node) == 3
        state = self.families.get(address_family(node[1])) if isinstance(node[1], str) else None
        if state is None or not self.is_foreign_node(node) or not remember(self.recently_queued, (node[1], node[2]), None, MAX_BACKOFF_ENTRIES):
            return
        state.node_queue.append(node)
        state.recent_nodes.append(node)
        assert len(state.node_queue) <= (state.node_queue.maxlen or 0)

    # Parents: receive_pending, tests
    # Keywords: datagram, dispatch, decode, krpc
    def handle_datagram(self, data: bytes, address: Address, node_socket: NodeSocket) -> None:
        assert isinstance(data, bytes) and node_socket in self.node_sockets
        self.stats["packets_received"] += 1
        remember(self.socket_affinity, address, node_socket, SOCKET_AFFINITY_SIZE)
        try:
            message = decode_bencode(data)
        except (ValueError, RecursionError) as error:
            LOGGER.debug("malformed datagram from %s: %s", address, error)
            return
        if not isinstance(message, dict):
            return
        message_type = message.get(b"y")
        if message_type == b"r":
            self.handle_response(message, address)
        elif message_type == b"q":
            self.handle_query(message, address, node_socket)
        elif message_type == b"e":
            self.handle_error(message, address, node_socket)
        assert self.stats["packets_received"] >= 1

    # Parents: handle_datagram
    # Keywords: response, nodes, samples, lookup routing
    def handle_response(self, message: Message, address: Address) -> None:
        assert message.get(b"y") == b"r"
        self.stats["responses_received"] += 1
        response = message.get(b"r")
        transaction_id = message.get(b"t")
        if self.lookup_manager.owns_transaction(transaction_id):
            self.lookup_manager.handle_response(transaction_id, response, address)
            return
        if not isinstance(response, dict):
            return
        sender: Optional[Node] = (response.get(b"id"), address[0], address[1])
        if self.is_foreign_node(sender):
            self.families[address_family(address[0])].recent_nodes.append(sender)
        else:
            sender = None
        for key, decoder in ((NODES_KEY, decode_compact_nodes), (NODES6_KEY, decode_compact_nodes6)):
            compact_nodes = response.get(key)
            if isinstance(compact_nodes, bytes):
                for node in decoder(compact_nodes):
                    self.add_node(node)
        if b"samples" in response:
            self.handle_samples(response, address, sender)
        assert self.stats["responses_received"] >= 1

    # Parents: handle_response
    # Keywords: samples, bep51, record, interval backoff, hint nodes
    def handle_samples(self, response: Dict[bytes, Any], address: Address, sender: Optional[Node]) -> None:
        assert isinstance(response, dict)
        samples = decode_samples(response.get(b"samples"))
        self.catalog.record_hashes(samples, SOURCE_SAMPLE)
        self.recent_hashes.extend(samples)
        if sender is not None:
            for info_hash in samples:
                remember(self.hint_nodes, info_hash, sender, HINT_NODES_SIZE)
        self.stats["sample_responses"] += 1
        interval = response.get(b"interval")
        seconds = interval if is_plain_int(interval) else 0
        remember(self.sample_backoff, address, self.clock() + max(0, min(seconds, MAX_SAMPLE_INTERVAL)), MAX_BACKOFF_ENTRIES)
        self.stats["samples_received"] += len(samples)
        assert address in self.sample_backoff

    # Parents: handle_datagram
    # Keywords: error, 204, fallback, lookup routing
    def handle_error(self, message: Message, address: Address, node_socket: NodeSocket) -> None:
        assert message.get(b"y") == b"e"
        self.stats["errors_received"] += 1
        transaction_id = message.get(b"t")
        if self.lookup_manager.owns_transaction(transaction_id):
            self.lookup_manager.handle_error(transaction_id)
            return
        if read_error_code(message) == ERROR_METHOD_UNKNOWN:
            remember(self.sample_backoff, address, self.clock() + MAX_SAMPLE_INTERVAL, MAX_BACKOFF_ENTRIES)
            queue = self.families[node_socket.family].node_queue
            needs_nodes = len(queue) * 2 < (queue.maxlen or 0)
            if needs_nodes and remember(self.fallback_sent, address, None, MAX_BACKOFF_ENTRIES):
                self.send_find_node(node_socket, (node_socket.own_id, address[0], address[1]))
        assert self.stats["errors_received"] >= 1

    # Parents: handle_datagram
    # Keywords: query, dispatch, ping, find_node, get_peers, announce_peer, sample_infohashes
    def handle_query(self, message: Message, address: Address, node_socket: NodeSocket) -> None:
        assert message.get(b"y") == b"q"
        self.stats["queries_received"] += 1
        transaction_id = message.get(b"t")
        args = message.get(b"a")
        query_name = message.get(b"q")
        if not isinstance(transaction_id, bytes) or not isinstance(args, dict):
            return
        sender_id = args.get(b"id")
        reply_id = neighbor_node_id(sender_id, node_socket.own_id) if is_valid_node_id(sender_id) else node_socket.own_id
        if query_name in (b"get_peers", b"announce_peer") and is_valid_node_id(args.get(b"info_hash")):
            reply_id = neighbor_node_id(args[b"info_hash"], node_socket.own_id)
        if query_name == b"ping":
            self.handle_ping_query(transaction_id, reply_id, address, node_socket)
        elif query_name == b"find_node":
            self.handle_find_node_query(transaction_id, reply_id, address, node_socket)
        elif query_name == b"get_peers":
            self.handle_get_peers_query(transaction_id, reply_id, args, address, node_socket)
        elif query_name == b"announce_peer":
            self.handle_announce_peer_query(transaction_id, reply_id, args, address, node_socket)
        elif query_name == QUERY_SAMPLE_INFOHASHES:
            self.handle_sample_infohashes_query(transaction_id, reply_id, address, node_socket)
        else:
            self.send_message(node_socket, build_error_response(transaction_id, ERROR_METHOD_UNKNOWN, b"Method Unknown"), address)
        self.add_node((sender_id, address[0], address[1]))
        assert self.stats["queries_received"] >= 1

    # Parents: handle_query
    # Keywords: ping, response, keepalive
    def handle_ping_query(self, transaction_id: bytes, reply_id: bytes, address: Address, node_socket: NodeSocket) -> None:
        assert len(reply_id) == NODE_ID_LENGTH
        sent = self.send_message(node_socket, build_ping_response(transaction_id, reply_id), address)
        assert isinstance(sent, bool)

    # Parents: handle_query
    # Keywords: find_node, response, recent nodes
    def handle_find_node_query(self, transaction_id: bytes, reply_id: bytes, address: Address, node_socket: NodeSocket) -> None:
        assert len(reply_id) == NODE_ID_LENGTH
        state = self.families[node_socket.family]
        sent = self.send_message(node_socket, build_find_node_response(transaction_id, reply_id, encode_compact_nodes(state.recent_nodes), state.nodes_key), address)
        assert isinstance(sent, bool)

    # Parents: handle_get_peers_query, handle_announce_peer_query
    # Keywords: info hash, validation, protocol error, extract
    def read_info_hash(self, transaction_id: bytes, args: Dict[bytes, Any], address: Address, node_socket: NodeSocket) -> Optional[bytes]:
        assert isinstance(args, dict)
        info_hash = args.get(b"info_hash")
        if not is_valid_node_id(info_hash):
            self.send_message(node_socket, build_error_response(transaction_id, ERROR_PROTOCOL, b"invalid info_hash"), address)
            info_hash = None
        assert info_hash is None or len(info_hash) == NODE_ID_LENGTH
        return info_hash

    # Parents: handle_query
    # Keywords: get_peers, info hash, token, record
    def handle_get_peers_query(self, transaction_id: bytes, reply_id: bytes, args: Dict[bytes, Any], address: Address, node_socket: NodeSocket) -> None:
        assert len(reply_id) == NODE_ID_LENGTH and isinstance(args, dict)
        info_hash = self.read_info_hash(transaction_id, args, address, node_socket)
        if info_hash is None:
            return
        self.record_hash(info_hash, SOURCE_GET_PEERS)
        token = make_token(self.token_secret, address[0])
        state = self.families[node_socket.family]
        sent = self.send_message(node_socket, build_get_peers_response(transaction_id, reply_id, token, encode_compact_nodes(state.recent_nodes), state.nodes_key), address)
        assert isinstance(sent, bool)

    # Parents: handle_query
    # Keywords: announce_peer, token check, peer address, record
    def handle_announce_peer_query(self, transaction_id: bytes, reply_id: bytes, args: Dict[bytes, Any], address: Address, node_socket: NodeSocket) -> None:
        assert len(reply_id) == NODE_ID_LENGTH and isinstance(args, dict)
        info_hash = self.read_info_hash(transaction_id, args, address, node_socket)
        if info_hash is None:
            return
        token_valid = args.get(b"token") == make_token(self.token_secret, address[0])
        peer: Optional[Address] = None
        port = read_announced_port(args, address[1]) if token_valid else None
        if port is not None and is_routable_address(address[0], port):
            peer = (address[0], port)
            self.stats["announced_peers"] += 1
        self.record_hash(info_hash, SOURCE_ANNOUNCE_PEER, peer)
        if not token_valid:
            self.send_message(node_socket, build_error_response(transaction_id, ERROR_PROTOCOL, b"bad token"), address)
            return
        sent = self.send_message(node_socket, build_ping_response(transaction_id, reply_id), address)
        assert isinstance(sent, bool)

    # Parents: handle_query
    # Keywords: sample_infohashes, bep51, answer, recent hashes
    def handle_sample_infohashes_query(self, transaction_id: bytes, reply_id: bytes, address: Address, node_socket: NodeSocket) -> None:
        assert len(reply_id) == NODE_ID_LENGTH
        recent = list(self.recent_hashes)[-SAMPLES_PER_RESPONSE:]
        state = self.families[node_socket.family]
        response = build_sample_infohashes_response(transaction_id, reply_id, OWN_SAMPLE_INTERVAL, encode_compact_nodes(state.recent_nodes), len(self.recent_hashes), b"".join(recent), state.nodes_key)
        sent = self.send_message(node_socket, response, address)
        assert isinstance(sent, bool)

    # Parents: handle_get_peers_query, handle_announce_peer_query
    # Keywords: record, info hash, catalog, recent hashes
    def record_hash(self, info_hash: bytes, source: str, peer: Optional[Address] = None) -> None:
        assert len(info_hash) == NODE_ID_LENGTH
        self.catalog.record_hash(info_hash, source, peer)
        self.recent_hashes.append(info_hash)
        LOGGER.debug("info hash %s from %s", info_hash.hex(), source)
        assert self.recent_hashes[-1] == info_hash

    # Parents: run
    # Keywords: crawl, batch, queue, bep51
    def crawl_step(self) -> int:
        assert self.batch_size >= 1
        now = self.clock()
        starving = [family for family, state in self.families.items() if not state.node_queue and now - state.last_bootstrap_time >= BOOTSTRAP_RETRY_SECONDS]
        if starving:
            self.bootstrap(starving)
        sent = 0
        for state in self.families.values():
            limit = self.batch_size * len(state.node_sockets)
            count = 0
            while state.node_queue and count < limit:
                self.send_crawl_query(state.node_queue.popleft())
                count += 1
            sent += count
        assert 0 <= sent <= self.batch_size * len(self.node_sockets)
        return sent

    # Parents: start_pending_lookups
    # Keywords: seed nodes, closest, queue scan, recent nodes
    def seed_nodes(self, info_hash: bytes) -> List[Node]:
        assert len(info_hash) == NODE_ID_LENGTH
        scan_each = LOOKUP_SEED_SCAN // len(self.families)
        queued = itertools.chain.from_iterable(itertools.islice(state.node_queue, 0, scan_each) for state in self.families.values())
        recent = [node for state in self.families.values() for node in state.recent_nodes]
        result = select_closest_nodes(queued, info_hash, LOOKUP_SEED_COUNT) + recent
        assert len(result) <= LOOKUP_SEED_COUNT + RECENT_NODES_SIZE * len(self.families)
        return result

    # Parents: run
    # Keywords: lookups, hint node, backpressure, fetchable backlog, token budget
    def start_pending_lookups(self, now: float) -> int:
        assert now >= 0
        room = self.lookup_fetchable_target - self.catalog.fetchable_count()
        slots = min(self.lookup_manager.free_slots(), room, int(self.lookup_manager.refill(now)))
        started = 0
        if slots <= 0:
            return 0
        for info_hash in self.catalog.hashes_needing_peers(slots):
            hint = self.hint_nodes.get(info_hash)
            seeds = list(self.families[address_family(hint[1])].recent_nodes) if hint is not None else self.seed_nodes(info_hash)
            if self.lookup_manager.start_lookup(info_hash, seeds, now, hint):
                started += 1
            else:
                self.catalog.release_lookup_claim(info_hash)
        self.stats["lookups_started"] += started
        assert started <= slots
        return started

    # Parents: run, tests
    # Keywords: receive, selectors, multi socket, drain, high descriptors
    def receive_pending(self, timeout: float) -> int:
        assert timeout >= 0, "timeout must not be negative"
        if self.selector is None:
            self.selector = selectors.DefaultSelector()
            for node in self.node_sockets:
                self.selector.register(node.udp_socket, selectors.EVENT_READ, node)
        handled = 0
        for key, _ in self.selector.select(timeout):
            node_socket = key.data
            udp_socket = node_socket.udp_socket
            for _ in range(MAX_RECEIVE_PER_SOCKET):
                try:
                    data, raw_address = udp_socket.recvfrom(MAX_DATAGRAM_SIZE)
                except BlockingIOError:
                    break
                except ConnectionResetError:
                    continue
                except OSError as error:
                    LOGGER.debug("receive failed: %s", error)
                    break
                self.handle_datagram(data, (raw_address[0], raw_address[1]), node_socket)
                handled += 1
        assert handled >= 0
        return handled

    # Parents: run, tests
    # Keywords: selector, close, cleanup
    def close(self) -> None:
        assert self.node_sockets
        if self.selector is not None:
            self.selector.close()
            self.selector = None
        assert self.selector is None

    # Parents: ScraperRuntime.snapshot_stats, run
    # Keywords: statistics, snapshot, single writer
    def snapshot_stats(self) -> Dict[str, int]:
        assert all(key in self.stats for key in COUNTER_KEYS)
        stats = dict(self.stats)
        stats["queue_size"] = len(self.node_queue)
        stats["queue_size6"] = len(self.families[socket.AF_INET6].node_queue) if socket.AF_INET6 in self.families else 0
        stats.update(self.lookup_manager.snapshot_counts())
        assert all(key in stats for key in STATS_KEYS)
        return stats

    # Parents: ScraperRuntime.run_crawler_thread
    # Keywords: main loop, pacing, stop event, select
    def run(self, stop_event: threading.Event) -> Dict[str, int]:
        assert isinstance(stop_event, threading.Event)
        LOGGER.info("crawler start: %d nodes, batch %d per node every %.3fs", len(self.node_sockets), self.batch_size, self.interval)
        self.bootstrap()
        next_send = self.clock()
        while not stop_event.is_set():
            now = self.clock()
            if now >= next_send:
                self.crawl_step()
                self.lookup_manager.tick(now)
                self.start_pending_lookups(now)
                next_send = next_batch_time(next_send, now, self.interval)
            self.receive_pending(max(0.0, min(self.interval, next_send - self.clock())))
        self.lookup_manager.abort_all()
        self.close()
        stats = self.snapshot_stats()
        LOGGER.info("crawler stopped")
        assert all(key in stats for key in STATS_KEYS)
        return stats
