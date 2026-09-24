"""Continuous get_peers lookups (Kademlia style) driven from the crawler thread without blocking."""
import logging
import time
from typing import Callable, Dict, Iterable, List, NamedTuple, Optional, Set, Tuple

from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.krpc_messages import Address, Node, decode_compact_nodes, decode_compact_nodes6, decode_compact_peers, is_routable_address
from dht_scraper.node_identity import NODE_ID_LENGTH, is_valid_node_id, lookup_transaction_id, select_closest_nodes

LOGGER = logging.getLogger(LOGGER_NAME)


class LookupLimits(NamedTuple):
    """Tunable limits of the lookups; see spec/lookup/contract.md."""

    alpha: int = 1
    query_timeout: float = 1.5
    hint_timeout: float = 1.0
    deadline: float = 6.0
    max_queries: int = 16
    enough_peers: int = 8
    max_value_responses: int = 2
    max_active: int = 256
    queries_per_second: float = 420.0
    max_per_node: int = 4
    shortlist_size: int = 32
    max_peers: int = 50


DEFAULT_LOOKUP_LIMITS = LookupLimits()

SendQuery = Callable[[bytes, Node, bytes], None]
OnFinished = Callable[[bytes, List[Address]], None]
OnPeers = Callable[[bytes, List[Address]], object]
NodeFilter = Callable[[Node], bool]


class PeerLookup:
    """State of one get_peers lookup."""

    __slots__ = ("info_hash", "shortlist", "queried", "pending", "found_peers", "queries_sent", "value_responses", "hint_transaction", "deadline", "finished")

    # Parents: LookupManager.start_lookup
    # Keywords: lookup, state, shortlist, pending
    def __init__(self, info_hash: bytes, deadline: float) -> None:
        assert len(info_hash) == NODE_ID_LENGTH
        self.info_hash = info_hash
        self.shortlist: Dict[bytes, Node] = {}
        self.queried: Set[Address] = set()
        self.pending: Dict[bytes, Tuple[Address, float]] = {}
        self.found_peers: Dict[Address, None] = {}
        self.queries_sent = 0
        self.value_responses = 0
        self.hint_transaction: Optional[bytes] = None
        self.deadline = deadline
        self.finished = False
        assert not self.finished and not self.pending


# Parents: PeerLookup users
# Keywords: accept all, default filter
def accept_every_node(node: Node) -> bool:
    assert len(node) == 3
    return True


# Parents: LookupManager.add_to_shortlist, LookupManager.start_lookup
# Keywords: node, validation, filter
def is_usable_node(node: Node, node_filter: NodeFilter) -> bool:
    assert callable(node_filter)
    result = len(node) == 3 and is_valid_node_id(node[0]) and node_filter(node)
    assert isinstance(result, bool)
    return result


class LookupManager:
    """Runs up to `limits.max_active` lookups. All methods are called from the crawler thread."""

    # Parents: DhtCrawler.__init__
    # Keywords: lookup manager, callbacks, clock, limits, token bucket
    def __init__(
        self,
        send_query: SendQuery,
        on_finished: OnFinished,
        clock: Callable[[], float] = time.monotonic,
        node_filter: NodeFilter = accept_every_node,
        on_peers: Optional[OnPeers] = None,
        limits: LookupLimits = DEFAULT_LOOKUP_LIMITS,
    ) -> None:
        assert callable(send_query) and callable(on_finished) and callable(clock)
        assert limits.alpha >= 1 and limits.max_active >= 1 and limits.queries_per_second > 0 and limits.max_per_node >= 1
        self.send_query = send_query
        self.on_finished = on_finished
        self.on_peers = on_peers
        self.clock = clock
        self.node_filter = node_filter
        self.limits = limits
        self.active: Dict[bytes, PeerLookup] = {}
        self.transactions: Dict[bytes, bytes] = {}
        self.in_flight: Dict[Address, int] = {}
        self.tokens = float(limits.queries_per_second)
        self.tokens_time = clock()
        self.sequence = 0
        self.started_total = 0
        self.finished_total = 0
        self.with_peers_total = 0
        self.peers_found_total = 0
        self.queries_total = 0
        self.hint_queries = 0
        self.hint_answers = 0
        self.hint_values = 0
        assert not self.active and not self.transactions

    # Parents: DhtCrawler.start_pending_lookups
    # Keywords: capacity, slots, active lookups
    def free_slots(self) -> int:
        assert len(self.active) <= self.limits.max_active
        result = self.limits.max_active - len(self.active)
        assert 0 <= result <= self.limits.max_active
        return result

    # Parents: DhtCrawler.handle_response, DhtCrawler.handle_error
    # Keywords: transaction id, ownership, routing
    def owns_transaction(self, transaction_id: object) -> bool:
        assert self.transactions is not None
        result = isinstance(transaction_id, bytes) and transaction_id in self.transactions
        assert isinstance(result, bool)
        return result

    # Parents: start_lookup, top_up, tick
    # Keywords: token bucket, refill, query budget
    def refill(self, now: float) -> float:
        assert now >= 0
        elapsed = max(0.0, now - self.tokens_time)
        self.tokens = min(float(self.limits.queries_per_second), self.tokens + elapsed * self.limits.queries_per_second)
        self.tokens_time = max(self.tokens_time, now)
        assert 0 <= self.tokens <= self.limits.queries_per_second
        return self.tokens

    # Parents: DhtCrawler.start_pending_lookups
    # Keywords: start, seed nodes, hint node, first query
    def start_lookup(self, info_hash: bytes, seed_nodes: Iterable[Node], now: float, hint: Optional[Node] = None) -> bool:
        assert len(info_hash) == NODE_ID_LENGTH
        if info_hash in self.active or self.free_slots() == 0 or self.refill(now) < 1:
            return False
        if hint is not None and not is_usable_node(hint, self.node_filter):
            hint = None
        if hint is not None and self.in_flight.get((hint[1], hint[2]), 0) >= self.limits.max_per_node:
            return False
        lookup = PeerLookup(info_hash, now + self.limits.deadline)
        self.add_to_shortlist(lookup, seed_nodes)
        if hint is None and not lookup.shortlist:
            return False
        self.active[info_hash] = lookup
        self.started_total += 1
        if hint is not None:
            lookup.hint_transaction = self.send_to(lookup, hint, now, self.limits.hint_timeout)
            self.hint_queries += 1
        else:
            self.top_up(lookup, now)
        LOGGER.debug("lookup started for %s with %d seed nodes, hint %s", info_hash.hex(), len(lookup.shortlist), hint is not None)
        assert info_hash in self.active
        return True

    # Parents: start_lookup, handle_response
    # Keywords: shortlist, closest, trim, filter
    def add_to_shortlist(self, lookup: PeerLookup, nodes: Iterable[Node]) -> None:
        assert isinstance(lookup, PeerLookup)
        for node in nodes:
            if is_usable_node(node, self.node_filter) and (node[1], node[2]) not in lookup.queried:
                lookup.shortlist[node[0]] = node
        if len(lookup.shortlist) > self.limits.shortlist_size:
            closest = select_closest_nodes(lookup.shortlist.values(), lookup.info_hash, self.limits.shortlist_size)
            lookup.shortlist = {node[0]: node for node in closest}
        assert len(lookup.shortlist) <= self.limits.shortlist_size

    # Parents: start_lookup, top_up
    # Keywords: send, transaction, pending, in flight, token
    def send_to(self, lookup: PeerLookup, node: Node, now: float, timeout: float) -> bytes:
        assert not lookup.finished and timeout > 0
        transaction_id = lookup_transaction_id(self.sequence)
        self.sequence += 1
        address = (node[1], node[2])
        lookup.pending[transaction_id] = (address, now + timeout)
        lookup.queried.add(address)
        lookup.queries_sent += 1
        self.transactions[transaction_id] = lookup.info_hash
        self.in_flight[address] = self.in_flight.get(address, 0) + 1
        self.tokens -= 1
        self.queries_total += 1
        self.send_query(transaction_id, node, lookup.info_hash)
        assert transaction_id in self.transactions
        return transaction_id

    # Parents: handle_response, handle_error, expire_pending, finish
    # Keywords: release, pending, in flight, hint
    def release_transaction(self, lookup: PeerLookup, transaction_id: bytes) -> None:
        assert transaction_id in lookup.pending
        address, _ = lookup.pending.pop(transaction_id)
        self.transactions.pop(transaction_id, None)
        remaining = self.in_flight.get(address, 0) - 1
        if remaining > 0:
            self.in_flight[address] = remaining
        else:
            self.in_flight.pop(address, None)
        if lookup.hint_transaction == transaction_id:
            lookup.hint_transaction = None
        assert transaction_id not in self.transactions

    # Parents: start_lookup, advance
    # Keywords: top up, alpha in flight, closest unqueried, budget
    def top_up(self, lookup: PeerLookup, now: float) -> int:
        assert not lookup.finished
        free = min(self.limits.alpha - len(lookup.pending), self.limits.max_queries - lookup.queries_sent)
        if free <= 0 or self.refill(now) < 1:
            return 0
        candidates = [
            node for node in lookup.shortlist.values()
            if (node[1], node[2]) not in lookup.queried and self.in_flight.get((node[1], node[2]), 0) < self.limits.max_per_node
        ]
        sent = 0
        for node in select_closest_nodes(candidates, lookup.info_hash, free):
            if self.tokens < 1:
                break
            self.send_to(lookup, node, now, self.limits.query_timeout)
            sent += 1
        assert 0 <= sent <= self.limits.alpha
        return sent

    # Parents: advance
    # Keywords: convergence, unqueried, query cap
    def can_query_more(self, lookup: PeerLookup) -> bool:
        assert isinstance(lookup, PeerLookup)
        result = lookup.queries_sent < self.limits.max_queries and any((node[1], node[2]) not in lookup.queried for node in lookup.shortlist.values())
        assert isinstance(result, bool)
        return result

    # Parents: DhtCrawler.handle_response
    # Keywords: response, values, stream peers, nodes, advance
    def handle_response(self, transaction_id: bytes, response: object, address: Address) -> None:
        assert self.owns_transaction(transaction_id)
        info_hash = self.transactions[transaction_id]
        lookup = self.active.get(info_hash)
        if lookup is None or lookup.pending.get(transaction_id, (None, 0.0))[0] != address:
            LOGGER.debug("lookup response for %s ignored: unexpected sender %s", transaction_id.hex(), address)
            return
        from_hint = lookup.hint_transaction == transaction_id
        self.release_transaction(lookup, transaction_id)
        self.hint_answers += 1 if from_hint else 0
        if isinstance(response, dict):
            values = decode_compact_peers(response.get(b"values"))
            self.hint_values += 1 if from_hint and values else 0
            new_peers: List[Address] = []
            for peer in values:
                if is_routable_address(peer[0], peer[1]) and peer not in lookup.found_peers and len(lookup.found_peers) < self.limits.max_peers:
                    lookup.found_peers[peer] = None
                    new_peers.append(peer)
            if values:
                lookup.value_responses += 1
            if new_peers and self.on_peers is not None:
                self.on_peers(info_hash, new_peers)
            for key, decoder in ((b"nodes", decode_compact_nodes), (b"nodes6", decode_compact_nodes6)):
                compact_nodes = response.get(key)
                if isinstance(compact_nodes, bytes):
                    self.add_to_shortlist(lookup, decoder(compact_nodes))
        self.advance(lookup, self.clock())
        assert transaction_id not in lookup.pending

    # Parents: DhtCrawler.handle_error
    # Keywords: error, pending, drop
    def handle_error(self, transaction_id: bytes) -> None:
        assert self.owns_transaction(transaction_id)
        lookup = self.active.get(self.transactions[transaction_id])
        if lookup is not None and transaction_id in lookup.pending:
            self.release_transaction(lookup, transaction_id)
            self.advance(lookup, self.clock())
        else:
            self.transactions.pop(transaction_id, None)
        assert transaction_id not in self.transactions

    # Parents: tick
    # Keywords: expire, timeout, pending
    def expire_pending(self, lookup: PeerLookup, now: float) -> int:
        assert isinstance(lookup, PeerLookup)
        expired = [transaction_id for transaction_id, (_, expiry) in lookup.pending.items() if expiry <= now]
        for transaction_id in expired:
            self.release_transaction(lookup, transaction_id)
        assert all(expiry > now for _, expiry in lookup.pending.values())
        return len(expired)

    # Parents: handle_response, handle_error, tick
    # Keywords: state machine, finish, top up, hint wait
    def advance(self, lookup: PeerLookup, now: float) -> None:
        assert isinstance(lookup, PeerLookup)
        if lookup.finished:
            return
        limits = self.limits
        if now >= lookup.deadline or len(lookup.found_peers) >= limits.enough_peers or lookup.value_responses >= limits.max_value_responses:
            self.finish(lookup)
        elif lookup.hint_transaction is None:
            self.top_up(lookup, now)
            if not lookup.pending and not self.can_query_more(lookup):
                self.finish(lookup)
        assert lookup.finished or lookup.info_hash in self.active

    # Parents: advance, abort_all
    # Keywords: finish, callback, cleanup
    def finish(self, lookup: PeerLookup) -> None:
        assert not lookup.finished
        lookup.finished = True
        for transaction_id in list(lookup.pending):
            self.release_transaction(lookup, transaction_id)
        self.active.pop(lookup.info_hash, None)
        self.finished_total += 1
        peers = sorted(lookup.found_peers)
        self.peers_found_total += len(peers)
        self.with_peers_total += 1 if peers else 0
        LOGGER.debug("lookup finished for %s: %d peers after %d queries", lookup.info_hash.hex(), len(peers), lookup.queries_sent)
        self.on_finished(lookup.info_hash, peers)
        assert lookup.info_hash not in self.active

    # Parents: DhtCrawler.run
    # Keywords: tick, expire, advance, periodic
    def tick(self, now: float) -> None:
        assert now >= 0
        self.refill(now)
        for lookup in list(self.active.values()):
            self.expire_pending(lookup, now)
            self.advance(lookup, now)
        assert all(not lookup.finished for lookup in self.active.values())

    # Parents: DhtCrawler.snapshot_stats
    # Keywords: statistics, counters, lookups, read from another thread, no cross-counter check
    def snapshot_counts(self) -> Dict[str, int]:
        assert self.limits.max_active >= 1
        result = {
            "lookups_finished": self.finished_total,
            "lookups_with_peers": self.with_peers_total,
            "lookup_peers_found": self.peers_found_total,
            "lookup_queries_sent": self.queries_total,
            "lookup_hint_queries": self.hint_queries,
            "lookup_hint_answers": self.hint_answers,
            "lookup_hint_values": self.hint_values,
            "active_lookups": len(self.active),
        }
        assert all(value >= 0 for value in result.values())
        return result

    # Parents: DhtCrawler.run
    # Keywords: abort, shutdown, finish all
    def abort_all(self) -> None:
        assert self.active is not None
        for lookup in list(self.active.values()):
            self.finish(lookup)
        assert not self.active and not self.transactions
