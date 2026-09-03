"""Iterative get_peers lookups (Kademlia style) driven from the crawler thread without blocking."""
import logging
import time
from typing import Callable, Dict, Iterable, List, Optional, Set, Tuple

from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.krpc_messages import Address, Node, decode_compact_nodes, decode_compact_peers, is_routable_address
from dht_scraper.node_identity import NODE_ID_LENGTH, generate_lookup_transaction_id, is_valid_node_id, select_closest_nodes

LOGGER = logging.getLogger(LOGGER_NAME)

LOOKUP_ALPHA = 8
LOOKUP_MAX_ROUNDS = 4
LOOKUP_QUERY_TIMEOUT = 2.0
LOOKUP_DEADLINE = 8.0
LOOKUP_MAX_ACTIVE = 16
LOOKUP_SHORTLIST_SIZE = 32
LOOKUP_MAX_PEERS = 50
LOOKUP_MIN_PEERS = 1

SendQuery = Callable[[bytes, Node, bytes], None]
OnFinished = Callable[[bytes, List[Address]], None]
NodeFilter = Callable[[Node], bool]


class PeerLookup:
    """State of one get_peers lookup."""

    __slots__ = ("info_hash", "shortlist", "queried", "pending", "found_peers", "rounds_sent", "deadline", "finished")

    # Parents: LookupManager.start_lookup
    # Keywords: lookup, state, shortlist, pending
    def __init__(self, info_hash: bytes, deadline: float) -> None:
        assert len(info_hash) == NODE_ID_LENGTH
        self.info_hash = info_hash
        self.shortlist: Dict[bytes, Node] = {}
        self.queried: Set[Address] = set()
        self.pending: Dict[bytes, Tuple[Address, float]] = {}
        self.found_peers: Set[Address] = set()
        self.rounds_sent = 0
        self.deadline = deadline
        self.finished = False
        assert not self.finished and not self.pending


# Parents: PeerLookup users
# Keywords: accept all, default filter
def accept_every_node(node: Node) -> bool:
    assert len(node) == 3
    return True


class LookupManager:
    """Runs up to LOOKUP_MAX_ACTIVE lookups. All methods are called from the crawler thread."""

    # Parents: DhtCrawler.__init__
    # Keywords: lookup manager, callbacks, clock
    def __init__(self, send_query: SendQuery, on_finished: OnFinished, clock: Callable[[], float] = time.monotonic, node_filter: NodeFilter = accept_every_node) -> None:
        assert callable(send_query) and callable(on_finished) and callable(clock)
        self.send_query = send_query
        self.on_finished = on_finished
        self.clock = clock
        self.node_filter = node_filter
        self.active: Dict[bytes, PeerLookup] = {}
        self.transactions: Dict[bytes, bytes] = {}
        self.started_total = 0
        self.finished_total = 0
        self.peers_found_total = 0
        assert not self.active and not self.transactions

    # Parents: DhtCrawler.start_pending_lookups
    # Keywords: capacity, slots, active lookups
    def free_slots(self) -> int:
        assert len(self.active) <= LOOKUP_MAX_ACTIVE
        result = LOOKUP_MAX_ACTIVE - len(self.active)
        assert 0 <= result <= LOOKUP_MAX_ACTIVE
        return result

    # Parents: DhtCrawler.handle_response, DhtCrawler.handle_error
    # Keywords: transaction id, ownership, routing
    def owns_transaction(self, transaction_id: object) -> bool:
        assert self.transactions is not None
        result = isinstance(transaction_id, bytes) and transaction_id in self.transactions
        assert isinstance(result, bool)
        return result

    # Parents: DhtCrawler.start_pending_lookups
    # Keywords: start, seed nodes, first round
    def start_lookup(self, info_hash: bytes, seed_nodes: Iterable[Node], now: float) -> bool:
        assert len(info_hash) == NODE_ID_LENGTH
        if info_hash in self.active or self.free_slots() == 0:
            return False
        lookup = PeerLookup(info_hash, now + LOOKUP_DEADLINE)
        self.add_to_shortlist(lookup, seed_nodes)
        if not lookup.shortlist:
            return False
        self.active[info_hash] = lookup
        self.started_total += 1
        self.send_round(lookup, now)
        LOGGER.debug("lookup started for %s with %d seed nodes", info_hash.hex(), len(lookup.shortlist))
        assert info_hash in self.active
        return True

    # Parents: start_lookup, handle_response
    # Keywords: shortlist, closest, trim, filter
    def add_to_shortlist(self, lookup: PeerLookup, nodes: Iterable[Node]) -> None:
        assert isinstance(lookup, PeerLookup)
        for node in nodes:
            if is_valid_node_id(node[0]) and self.node_filter(node) and (node[1], node[2]) not in lookup.queried:
                lookup.shortlist[node[0]] = node
        if len(lookup.shortlist) > LOOKUP_SHORTLIST_SIZE:
            closest = select_closest_nodes(lookup.shortlist.values(), lookup.info_hash, LOOKUP_SHORTLIST_SIZE)
            lookup.shortlist = {node[0]: node for node in closest}
        assert len(lookup.shortlist) <= LOOKUP_SHORTLIST_SIZE

    # Parents: start_lookup, advance
    # Keywords: round, alpha, send, closest unqueried
    def send_round(self, lookup: PeerLookup, now: float) -> int:
        assert not lookup.finished and lookup.rounds_sent < LOOKUP_MAX_ROUNDS
        unqueried = [node for node in lookup.shortlist.values() if (node[1], node[2]) not in lookup.queried]
        sent = 0
        for node in select_closest_nodes(unqueried, lookup.info_hash, LOOKUP_ALPHA):
            transaction_id = generate_lookup_transaction_id()
            address = (node[1], node[2])
            lookup.pending[transaction_id] = (address, now + LOOKUP_QUERY_TIMEOUT)
            lookup.queried.add(address)
            self.transactions[transaction_id] = lookup.info_hash
            self.send_query(transaction_id, node, lookup.info_hash)
            sent += 1
        lookup.rounds_sent += 1
        assert 0 <= sent <= LOOKUP_ALPHA
        return sent

    # Parents: DhtCrawler.handle_response
    # Keywords: response, values, nodes, advance
    def handle_response(self, transaction_id: bytes, response: object, address: Address) -> None:
        assert self.owns_transaction(transaction_id)
        info_hash = self.transactions[transaction_id]
        lookup = self.active.get(info_hash)
        if lookup is None or lookup.pending.get(transaction_id, (None, 0.0))[0] != address:
            LOGGER.debug("lookup response for %s ignored: unexpected sender %s", transaction_id.hex(), address)
            return
        del self.transactions[transaction_id]
        del lookup.pending[transaction_id]
        if isinstance(response, dict):
            for peer in decode_compact_peers(response.get(b"values")):
                if is_routable_address(peer[0], peer[1]) and len(lookup.found_peers) < LOOKUP_MAX_PEERS:
                    lookup.found_peers.add(peer)
            compact_nodes = response.get(b"nodes")
            if isinstance(compact_nodes, bytes):
                self.add_to_shortlist(lookup, decode_compact_nodes(compact_nodes))
        self.advance(lookup, self.clock())
        assert transaction_id not in lookup.pending

    # Parents: DhtCrawler.handle_error
    # Keywords: error, pending, drop
    def handle_error(self, transaction_id: bytes) -> None:
        assert self.owns_transaction(transaction_id)
        info_hash = self.transactions.pop(transaction_id)
        lookup = self.active.get(info_hash)
        if lookup is not None:
            lookup.pending.pop(transaction_id, None)
            self.advance(lookup, self.clock())
        assert transaction_id not in self.transactions

    # Parents: tick
    # Keywords: expire, timeout, pending
    def expire_pending(self, lookup: PeerLookup, now: float) -> int:
        assert isinstance(lookup, PeerLookup)
        expired = [transaction_id for transaction_id, (_, expiry) in lookup.pending.items() if expiry <= now]
        for transaction_id in expired:
            del lookup.pending[transaction_id]
            self.transactions.pop(transaction_id, None)
        assert all(expiry > now for _, expiry in lookup.pending.values())
        return len(expired)

    # Parents: handle_response, handle_error, tick
    # Keywords: state machine, finish, next round
    def advance(self, lookup: PeerLookup, now: float) -> None:
        assert isinstance(lookup, PeerLookup)
        if lookup.finished:
            return
        if now >= lookup.deadline or len(lookup.found_peers) >= LOOKUP_MIN_PEERS:
            self.finish(lookup)
        elif lookup.pending:
            return
        elif lookup.rounds_sent >= LOOKUP_MAX_ROUNDS or self.send_round(lookup, now) == 0:
            self.finish(lookup)
        assert lookup.finished or lookup.pending

    # Parents: advance, abort_all
    # Keywords: finish, callback, cleanup
    def finish(self, lookup: PeerLookup) -> None:
        assert not lookup.finished
        lookup.finished = True
        for transaction_id in lookup.pending:
            self.transactions.pop(transaction_id, None)
        lookup.pending.clear()
        self.active.pop(lookup.info_hash, None)
        self.finished_total += 1
        peers = sorted(lookup.found_peers)
        self.peers_found_total += len(peers)
        LOGGER.debug("lookup finished for %s: %d peers after %d rounds", lookup.info_hash.hex(), len(peers), lookup.rounds_sent)
        self.on_finished(lookup.info_hash, peers)
        assert lookup.info_hash not in self.active

    # Parents: DhtCrawler.run
    # Keywords: tick, expire, advance, periodic
    def tick(self, now: float) -> None:
        assert now >= 0
        for lookup in list(self.active.values()):
            self.expire_pending(lookup, now)
            self.advance(lookup, now)
        assert all(not lookup.finished for lookup in self.active.values())

    # Parents: DhtCrawler.run
    # Keywords: abort, shutdown, finish all
    def abort_all(self) -> None:
        assert self.active is not None
        for lookup in list(self.active.values()):
            self.finish(lookup)
        assert not self.active and not self.transactions
