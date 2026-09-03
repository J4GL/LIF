"""DHT crawler thread: N simulated nodes share one select loop, crawl with BEP 51 and answer queries."""
import collections
import itertools
import logging
import os
import select
import socket
import threading
import time
from typing import Any, Callable, Deque, Dict, List, Optional, Sequence, Tuple

from dht_scraper.bencode_codec import decode_bencode, encode_bencode, is_plain_int
from dht_scraper.bounded_recent_map import remember
from dht_scraper.dht_lookup import LookupManager
from dht_scraper.dht_node_sockets import NodeSocket, own_id_suffixes
from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.krpc_messages import (
    ERROR_METHOD_UNKNOWN,
    ERROR_PROTOCOL,
    MAX_SAMPLE_INTERVAL,
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
]
MAX_DATAGRAM_SIZE = 65535
RECENT_NODES_SIZE = 8
RECENT_HASHES_SIZE = 64
SAMPLES_PER_RESPONSE = 20
OWN_SAMPLE_INTERVAL = 300
BOOTSTRAP_RETRY_SECONDS = 5.0
LOOKUP_SCAN_INTERVAL_SECONDS = 1.0
MAX_RECEIVE_PER_SOCKET = 1000
MAX_BACKOFF_ENTRIES = 50000
TOKEN_SECRET_LENGTH = 16
LOOKUP_SEED_SCAN = 2000
LOOKUP_SEED_COUNT = 16
COUNTER_KEYS = (
    "packets_sent", "packets_received", "queries_received", "responses_received", "errors_received",
    "samples_received", "announced_peers", "lookups_started", "sample_queries_sent", "find_node_sent",
)
STATS_KEYS = COUNTER_KEYS + ("queue_size", "active_lookups")


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
    ) -> None:
        assert len(node_sockets) >= 1 and batch_size >= 1 and interval > 0 and max_queue >= 1
        assert len(bootstrap_nodes) > 0, "bootstrap list must not be empty"
        self.node_sockets = list(node_sockets)
        self.socket_index = {id(node.udp_socket): node for node in self.node_sockets}
        self.catalog = catalog
        self.bootstrap_nodes = list(bootstrap_nodes)
        self.resolved_bootstrap_hosts: Dict[str, str] = {}
        self.batch_size = batch_size
        self.interval = interval
        self.clock = clock
        self.own_suffixes = own_id_suffixes(self.node_sockets)
        self.socket_cycle = itertools.cycle(self.node_sockets)
        self.token_secret = os.urandom(TOKEN_SECRET_LENGTH)
        self.node_queue: Deque[Node] = collections.deque(maxlen=max_queue)
        self.recent_nodes: Deque[Node] = collections.deque(maxlen=RECENT_NODES_SIZE)
        self.recent_hashes: Deque[bytes] = collections.deque(maxlen=RECENT_HASHES_SIZE)
        self.sample_backoff: "collections.OrderedDict[Address, float]" = collections.OrderedDict()
        self.fallback_sent: "collections.OrderedDict[Address, None]" = collections.OrderedDict()
        self.recently_queued: "collections.OrderedDict[Address, None]" = collections.OrderedDict()
        self.stats: Dict[str, int] = {key: 0 for key in COUNTER_KEYS}
        self.lookup_manager = LookupManager(self.send_lookup_query, catalog.add_lookup_result, clock, self.is_foreign_node)
        self.last_bootstrap_time = float("-inf")
        self.next_lookup_scan = float("-inf")
        assert len(self.socket_index) == len(self.node_sockets) and len(self.node_queue) == 0

    # Parents: send_crawl_query, send_lookup_query
    # Keywords: round robin, socket, virtual node
    def pick_node_socket(self) -> NodeSocket:
        assert len(self.node_sockets) >= 1
        result = next(self.socket_cycle)
        assert result in self.node_sockets
        return result

    # Parents: send_find_node, send_sample_infohashes, send_lookup_query, handle_query, handle_ping_query,
    #          handle_find_node_query, handle_get_peers_query, handle_announce_peer_query,
    #          handle_sample_infohashes_query, read_info_hash, handle_error
    # Keywords: udp, send, bencode, datagram
    def send_message(self, node_socket: NodeSocket, message: Message, address: Address) -> bool:
        assert isinstance(message, dict) and node_socket in self.node_sockets
        payload = encode_bencode(message)
        try:
            node_socket.udp_socket.sendto(payload, address)
        except OSError as error:
            LOGGER.debug("send to %s failed: %s", address, error)
            return False
        self.stats["packets_sent"] += 1
        assert self.stats["packets_sent"] >= 1
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
        node_socket = self.pick_node_socket()
        address = (node[1], node[2])
        backoff_until = self.sample_backoff.get(address)
        if backoff_until is not None and backoff_until > self.clock():
            self.send_find_node(node_socket, node)
        else:
            self.send_sample_infohashes(node_socket, node)
        assert self.stats["packets_sent"] >= 0

    # Parents: LookupManager.send_round
    # Keywords: get_peers, lookup, query, neighbor id
    def send_lookup_query(self, transaction_id: bytes, node: Node, info_hash: bytes) -> None:
        assert len(transaction_id) == LOOKUP_TRANSACTION_ID_LENGTH and len(info_hash) == NODE_ID_LENGTH
        node_socket = self.pick_node_socket()
        query = build_get_peers_query(transaction_id, neighbor_node_id(node[0], node_socket.own_id), info_hash)
        self.send_message(node_socket, query, (node[1], node[2]))
        assert query[b"q"] == b"get_peers"

    # Parents: run, crawl_step
    # Keywords: bootstrap, dns, router, join
    def bootstrap(self) -> None:
        assert len(self.bootstrap_nodes) > 0
        self.last_bootstrap_time = self.clock()
        for host, port in self.bootstrap_nodes:
            if host not in self.resolved_bootstrap_hosts:
                try:
                    self.resolved_bootstrap_hosts[host] = socket.gethostbyname(host)
                except OSError as error:
                    LOGGER.warning("bootstrap: cannot resolve %s: %s", host, error)
                    continue
            ip = self.resolved_bootstrap_hosts[host]
            for node_socket in self.node_sockets:
                self.send_find_node(node_socket, (node_socket.own_id, ip, port))
        LOGGER.info("bootstrap: contacted %d of %d routers from %d nodes", len(self.resolved_bootstrap_hosts), len(self.bootstrap_nodes), len(self.node_sockets))
        assert self.last_bootstrap_time > float("-inf")

    # Parents: add_node, handle_response, LookupManager.add_to_shortlist
    # Keywords: node filter, self reference, routable, admission
    def is_foreign_node(self, node: Node) -> bool:
        node_id, ip, port = node
        assert isinstance(ip, str)
        result = (
            is_valid_node_id(node_id)
            and node_id[NEIGHBOR_PREFIX_LENGTH:] not in self.own_suffixes
            and is_routable_address(ip, port)
        )
        assert isinstance(result, bool)
        return result

    # Parents: handle_response, handle_query
    # Keywords: queue, node, filter, dedup
    def add_node(self, node: Node) -> None:
        assert len(node) == 3
        if not self.is_foreign_node(node) or not remember(self.recently_queued, (node[1], node[2]), None, MAX_BACKOFF_ENTRIES):
            return
        self.node_queue.append(node)
        self.recent_nodes.append(node)
        assert len(self.node_queue) <= (self.node_queue.maxlen or 0)

    # Parents: receive_pending, tests
    # Keywords: datagram, dispatch, decode, krpc
    def handle_datagram(self, data: bytes, address: Address, node_socket: NodeSocket) -> None:
        assert isinstance(data, bytes) and node_socket in self.node_sockets
        self.stats["packets_received"] += 1
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
        sender = (response.get(b"id"), address[0], address[1])
        if self.is_foreign_node(sender):
            self.recent_nodes.append(sender)
        compact_nodes = response.get(b"nodes")
        if isinstance(compact_nodes, bytes):
            for node in decode_compact_nodes(compact_nodes):
                self.add_node(node)
        if b"samples" in response:
            self.handle_samples(response, address)
        assert self.stats["responses_received"] >= 1

    # Parents: handle_response
    # Keywords: samples, bep51, record, interval backoff
    def handle_samples(self, response: Dict[bytes, Any], address: Address) -> None:
        assert isinstance(response, dict)
        samples = decode_samples(response.get(b"samples"))
        self.catalog.record_hashes(samples, SOURCE_SAMPLE)
        self.recent_hashes.extend(samples)
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
            if remember(self.fallback_sent, address, None, MAX_BACKOFF_ENTRIES):
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
        sent = self.send_message(node_socket, build_find_node_response(transaction_id, reply_id, encode_compact_nodes(self.recent_nodes)), address)
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
        sent = self.send_message(node_socket, build_get_peers_response(transaction_id, reply_id, token, encode_compact_nodes(self.recent_nodes)), address)
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
        response = build_sample_infohashes_response(transaction_id, reply_id, OWN_SAMPLE_INTERVAL, encode_compact_nodes(self.recent_nodes), len(self.recent_hashes), b"".join(recent))
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
        if not self.node_queue and self.clock() - self.last_bootstrap_time >= BOOTSTRAP_RETRY_SECONDS:
            self.bootstrap()
        limit = self.batch_size * len(self.node_sockets)
        sent = 0
        while self.node_queue and sent < limit:
            self.send_crawl_query(self.node_queue.popleft())
            sent += 1
        assert 0 <= sent <= limit
        return sent

    # Parents: run
    # Keywords: lookups, seed nodes, candidates, rate limited scan
    def start_pending_lookups(self, now: float) -> int:
        assert now >= 0
        slots = self.lookup_manager.free_slots()
        started = 0
        if slots == 0 or now < self.next_lookup_scan:
            return 0
        self.next_lookup_scan = now + LOOKUP_SCAN_INTERVAL_SECONDS
        for info_hash in self.catalog.hashes_needing_peers(slots):
            seed = select_closest_nodes(itertools.islice(self.node_queue, 0, LOOKUP_SEED_SCAN), info_hash, LOOKUP_SEED_COUNT) + list(self.recent_nodes)
            if self.lookup_manager.start_lookup(info_hash, seed, now):
                started += 1
            else:
                self.catalog.release_lookup_claim(info_hash)
        self.stats["lookups_started"] += started
        assert started <= slots
        return started

    # Parents: run, tests
    # Keywords: receive, select, multi socket, drain
    def receive_pending(self, timeout: float) -> int:
        assert timeout >= 0, "timeout must not be negative"
        handled = 0
        readable, _, _ = select.select([node.udp_socket for node in self.node_sockets], [], [], timeout)
        for udp_socket in readable:
            node_socket = self.socket_index[id(udp_socket)]
            for _ in range(MAX_RECEIVE_PER_SOCKET):
                try:
                    data, address = udp_socket.recvfrom(MAX_DATAGRAM_SIZE)
                except BlockingIOError:
                    break
                except ConnectionResetError:
                    continue
                except OSError as error:
                    LOGGER.debug("receive failed: %s", error)
                    break
                self.handle_datagram(data, address, node_socket)
                handled += 1
        assert handled >= 0
        return handled

    # Parents: ScraperRuntime.snapshot_stats, run
    # Keywords: statistics, snapshot, single writer
    def snapshot_stats(self) -> Dict[str, int]:
        assert all(key in self.stats for key in COUNTER_KEYS)
        stats = dict(self.stats)
        stats["queue_size"] = len(self.node_queue)
        stats["active_lookups"] = len(self.lookup_manager.active)
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
                next_send = now + self.interval
            self.receive_pending(max(0.0, min(self.interval, next_send - self.clock())))
        self.lookup_manager.abort_all()
        stats = self.snapshot_stats()
        LOGGER.info("crawler stopped")
        assert all(key in stats for key in STATS_KEYS)
        return stats
