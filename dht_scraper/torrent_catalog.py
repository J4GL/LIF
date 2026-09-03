"""Bounded in-memory catalog of observed torrents with fetch and lookup state. One lock, no I/O."""
import collections
import operator
import threading
import time
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

from dht_scraper.bounded_recent_map import remember
from dht_scraper.node_identity import NODE_ID_LENGTH
from dht_scraper.torrent_info_summary import TorrentMetadata

MAX_CATALOG_ENTRIES = 50000
MAX_PEERS_PER_HASH = 32
MAX_FETCH_ATTEMPTS = 3
FETCH_RETRY_SECONDS = 60.0
MAX_LOOKUPS_PER_HASH = 2
LOOKUP_RETRY_SECONDS = 30.0
EVICTION_DIVISOR = 20

FETCH_PENDING = "pending"
FETCH_IN_PROGRESS = "in_progress"
FETCH_DONE = "done"
FETCH_FAILED = "failed"
FETCH_STATES = (FETCH_PENDING, FETCH_IN_PROGRESS, FETCH_DONE, FETCH_FAILED)

SOURCE_GET_PEERS = "get_peers"
SOURCE_ANNOUNCE_PEER = "announce_peer"
SOURCE_SAMPLE = "sample"
VALID_SOURCES = (SOURCE_GET_PEERS, SOURCE_ANNOUNCE_PEER, SOURCE_SAMPLE)

Peer = Tuple[str, int]
FetchCandidate = Tuple[bytes, List[Peer]]
RANK_ATTRIBUTES = operator.attrgetter("seen_count", "last_seen")


class TorrentEntry:
    """Mutable per-hash record. Never leaves the catalog."""

    __slots__ = (
        "info_hash", "first_seen", "last_seen", "seen_count", "announce_count", "peers", "fetch_state",
        "fetch_attempts", "retry_after", "last_error", "lookups_started", "lookup_in_progress", "next_lookup_time", "metadata",
    )

    # Parents: TorrentCatalog._get_or_create_locked
    # Keywords: entry, counters, fetch state, lookup state
    def __init__(self, info_hash: bytes, now: float) -> None:
        assert len(info_hash) == NODE_ID_LENGTH and now >= 0
        self.info_hash = info_hash
        self.first_seen = now
        self.last_seen = now
        self.seen_count = 0
        self.announce_count = 0
        self.peers: "collections.OrderedDict[Peer, None]" = collections.OrderedDict()
        self.fetch_state = FETCH_PENDING
        self.fetch_attempts = 0
        self.retry_after = 0.0
        self.last_error = ""
        self.lookups_started = 0
        self.lookup_in_progress = False
        self.next_lookup_time = 0.0
        self.metadata: Optional[TorrentMetadata] = None
        assert self.fetch_state == FETCH_PENDING and self.metadata is None


# Parents: TorrentCatalog.search
# Keywords: ranking, seen count, popularity, priority
def rank_key(entry: TorrentEntry) -> Tuple[int, float]:
    assert isinstance(entry, TorrentEntry)
    result = RANK_ATTRIBUTES(entry)
    assert result[0] >= 0
    return result


# Parents: TorrentCatalog._evict_locked
# Keywords: eviction, lowest value, metadata kept
def eviction_key(entry: TorrentEntry) -> Tuple[int, int, float]:
    assert isinstance(entry, TorrentEntry)
    result = (1 if entry.metadata is not None else 0, entry.seen_count, entry.last_seen)
    assert result[0] in (0, 1)
    return result


# Parents: TorrentCatalog public methods
# Keywords: clock, wall time, default now
def moment_or_now(now: Optional[float]) -> float:
    assert now is None or now >= 0
    result = time.time() if now is None else now
    assert result >= 0
    return result


class TorrentCatalog:
    """Thread-safe bounded catalog. Public methods take the lock once; `_locked` helpers require it."""

    # Parents: run_scraper, tests
    # Keywords: catalog, init, lock, limits
    def __init__(
        self,
        max_entries: int = MAX_CATALOG_ENTRIES,
        max_peers_per_hash: int = MAX_PEERS_PER_HASH,
        max_fetch_attempts: int = MAX_FETCH_ATTEMPTS,
        retry_seconds: float = FETCH_RETRY_SECONDS,
        max_lookups: int = MAX_LOOKUPS_PER_HASH,
        lookup_retry_seconds: float = LOOKUP_RETRY_SECONDS,
    ) -> None:
        assert max_entries >= 1 and max_peers_per_hash >= 1 and max_fetch_attempts >= 1 and retry_seconds >= 0
        assert max_lookups >= 0 and lookup_retry_seconds >= 0
        self._lock = threading.Lock()
        self._entries: Dict[bytes, TorrentEntry] = {}
        self._fetched: Dict[bytes, TorrentEntry] = {}
        self._fetchable: Dict[bytes, TorrentEntry] = {}
        self._state_counts: Dict[str, int] = {state: 0 for state in FETCH_STATES}
        self._observations = 0
        self._evicted = 0
        self._lookups_in_progress = 0
        self.max_entries = max_entries
        self.max_peers_per_hash = max_peers_per_hash
        self.max_fetch_attempts = max_fetch_attempts
        self.retry_seconds = retry_seconds
        self.max_lookups = max_lookups
        self.lookup_retry_seconds = lookup_retry_seconds
        assert len(self._entries) == 0 and not self._lock.locked()

    # Parents: DhtCrawler.record_hash
    # Keywords: record, sighting, counters, peer
    def record_hash(self, info_hash: bytes, source: str, peer: Optional[Peer] = None, now: Optional[float] = None) -> bool:
        assert len(info_hash) == NODE_ID_LENGTH and source in VALID_SOURCES
        moment = moment_or_now(now)
        with self._lock:
            created = self._record_locked(info_hash, source, peer, moment)
            self._assert_invariants_locked()
        assert isinstance(created, bool)
        return created

    # Parents: DhtCrawler.handle_samples
    # Keywords: record, batch, samples, one lock
    def record_hashes(self, info_hashes: Sequence[bytes], source: str, now: Optional[float] = None) -> int:
        assert all(len(info_hash) == NODE_ID_LENGTH for info_hash in info_hashes) and source in VALID_SOURCES
        moment = moment_or_now(now)
        created = 0
        with self._lock:
            for info_hash in info_hashes:
                created += 1 if self._record_locked(info_hash, source, None, moment) else 0
            self._assert_invariants_locked()
        assert 0 <= created <= len(info_hashes)
        return created

    # Parents: tests, lookup results (via add_lookup_result)
    # Keywords: peers, add, dedup, bounded
    def add_peers(self, info_hash: bytes, peers: Sequence[Peer], now: Optional[float] = None) -> int:
        assert len(info_hash) == NODE_ID_LENGTH and all(len(peer) == 2 for peer in peers)
        added = 0
        with self._lock:
            entry = self._entries.get(info_hash)
            if entry is not None:
                for peer in peers:
                    added += 1 if self._add_peer_locked(entry, peer) else 0
                self._index_entry_locked(entry)
        assert 0 <= added <= len(peers)
        return added

    # Parents: DhtCrawler.start_pending_lookups
    # Keywords: lookup, candidates, no peers, ranking
    def hashes_needing_peers(self, limit: int, now: Optional[float] = None) -> List[bytes]:
        assert limit >= 0
        moment = moment_or_now(now)
        with self._lock:
            eligible = [
                entry for entry in self._entries.values()
                if entry.fetch_state == FETCH_PENDING and not entry.peers and not entry.lookup_in_progress
                and entry.lookups_started < self.max_lookups and entry.next_lookup_time <= moment
            ]
            chosen = sorted(eligible, key=RANK_ATTRIBUTES, reverse=True)[:limit]
            for entry in chosen:
                entry.lookup_in_progress = True
                self._lookups_in_progress += 1
            result = [entry.info_hash for entry in chosen]
        assert len(result) <= limit
        return result

    # Parents: DhtCrawler.start_pending_lookups
    # Keywords: lookup claim, release, not started
    def release_lookup_claim(self, info_hash: bytes) -> None:
        assert len(info_hash) == NODE_ID_LENGTH
        with self._lock:
            entry = self._entries.get(info_hash)
            if entry is not None and entry.lookup_in_progress:
                entry.lookup_in_progress = False
                self._lookups_in_progress -= 1
        assert self._lookups_in_progress >= 0

    # Parents: LookupManager.finish (via on_finished)
    # Keywords: lookup result, peers, backoff, give up
    def add_lookup_result(self, info_hash: bytes, peers: Sequence[Peer], now: Optional[float] = None) -> None:
        assert len(info_hash) == NODE_ID_LENGTH
        moment = moment_or_now(now)
        with self._lock:
            entry = self._entries.get(info_hash)
            if entry is not None:
                if entry.lookup_in_progress:
                    entry.lookup_in_progress = False
                    self._lookups_in_progress -= 1
                entry.lookups_started += 1
                for peer in peers:
                    self._add_peer_locked(entry, peer)
                if not entry.peers:
                    entry.next_lookup_time = moment + self.lookup_retry_seconds * entry.lookups_started
                    if entry.lookups_started >= self.max_lookups and entry.fetch_state == FETCH_PENDING:
                        entry.last_error = "no_peers"
                        self._set_state_locked(entry, FETCH_FAILED)
                self._index_entry_locked(entry)
            self._assert_invariants_locked()
        assert self._lookups_in_progress >= 0

    # Parents: FetchScheduler.schedule_round
    # Keywords: fetch candidates, ranking, claim, in progress
    def next_fetch_candidates(self, limit: int, now: Optional[float] = None) -> List[FetchCandidate]:
        assert limit >= 0
        moment = moment_or_now(now)
        with self._lock:
            eligible = [entry for entry in self._fetchable.values() if entry.retry_after <= moment]
            result: List[FetchCandidate] = []
            for entry in sorted(eligible, key=RANK_ATTRIBUTES, reverse=True)[:limit]:
                self._set_state_locked(entry, FETCH_IN_PROGRESS)
                self._index_entry_locked(entry)
                result.append((entry.info_hash, list(entry.peers)))
            self._assert_invariants_locked()
        assert len(result) <= limit and all(peers for _, peers in result)
        return result

    # Parents: FetchWorkerPool.process_candidate
    # Keywords: metadata, store, done, search index
    def store_metadata(self, metadata: TorrentMetadata, now: Optional[float] = None) -> bool:
        assert isinstance(metadata, TorrentMetadata)
        moment = moment_or_now(now)
        with self._lock:
            entry, created = self._get_or_create_locked(metadata.info_hash, moment)
            entry.metadata = metadata
            entry.last_error = ""
            self._set_state_locked(entry, FETCH_DONE)
            self._fetched[entry.info_hash] = entry
            self._index_entry_locked(entry)
            self._assert_invariants_locked()
        assert isinstance(created, bool)
        return created

    # Parents: FetchWorkerPool.process_candidate
    # Keywords: fetch failed, retry, backoff, tried peers
    def mark_fetch_failed(self, info_hash: bytes, tried_peers: Sequence[Peer], reason: str, now: Optional[float] = None) -> str:
        assert len(info_hash) == NODE_ID_LENGTH and isinstance(reason, str)
        moment = moment_or_now(now)
        with self._lock:
            entry = self._entries.get(info_hash)
            if entry is None:
                return FETCH_FAILED
            for peer in tried_peers:
                entry.peers.pop(peer, None)
            entry.fetch_attempts += 1
            entry.last_error = reason
            if entry.fetch_attempts >= self.max_fetch_attempts:
                self._set_state_locked(entry, FETCH_FAILED)
            elif not entry.peers and entry.lookups_started >= self.max_lookups:
                entry.last_error = "no_peers"
                self._set_state_locked(entry, FETCH_FAILED)
            else:
                self._set_state_locked(entry, FETCH_PENDING)
                entry.retry_after = moment + self.retry_seconds * entry.fetch_attempts
                if not entry.peers:
                    entry.next_lookup_time = moment
            self._index_entry_locked(entry)
            result = entry.fetch_state
            self._assert_invariants_locked()
        assert result in FETCH_STATES
        return result

    # Parents: FetchScheduler.drain_queue, FetchWorkerPool.process_candidate
    # Keywords: release, claim, pending, shutdown
    def release_fetch_claim(self, info_hash: bytes) -> None:
        assert len(info_hash) == NODE_ID_LENGTH
        with self._lock:
            entry = self._entries.get(info_hash)
            if entry is not None and entry.fetch_state == FETCH_IN_PROGRESS:
                self._set_state_locked(entry, FETCH_PENDING)
                self._index_entry_locked(entry)
            self._assert_invariants_locked()
            released = entry is None or entry.fetch_state != FETCH_IN_PROGRESS
        assert released

    # Parents: ScraperRuntime.snapshot_stats, tests
    # Keywords: counts, statistics, snapshot, O(1)
    def snapshot_counts(self) -> Dict[str, int]:
        assert self.max_entries >= 1
        with self._lock:
            result = {
                "hashes_seen": len(self._entries),
                "observations": self._observations,
                "with_metadata": len(self._fetched),
                "fetch_pending": self._state_counts[FETCH_PENDING],
                "fetch_in_progress": self._state_counts[FETCH_IN_PROGRESS],
                "fetch_done": self._state_counts[FETCH_DONE],
                "fetch_failed": self._state_counts[FETCH_FAILED],
                "fetchable": len(self._fetchable),
                "lookups_in_progress": self._lookups_in_progress,
                "evicted": self._evicted,
                "capacity": self.max_entries,
            }
        assert result["fetch_pending"] + result["fetch_in_progress"] + result["fetch_done"] + result["fetch_failed"] == result["hashes_seen"]
        return result

    # Parents: CatalogRequestHandler.render_search
    # Keywords: search, substring, case insensitive, ranked
    def search(self, query: str, limit: int) -> List[Dict[str, Any]]:
        assert isinstance(query, str) and limit >= 0
        needle = query.strip().lower()
        with self._lock:
            matching = [entry for entry in self._fetched.values() if not needle or needle in entry.metadata.search_text]
            chosen = sorted(matching, key=rank_key, reverse=True)[:limit]
            result = [self._search_record_locked(entry) for entry in chosen]
        assert len(result) <= limit
        return result

    # Parents: CatalogRequestHandler.render_torrent
    # Keywords: detail, files, peers, fetch state
    def torrent_detail(self, info_hash: bytes) -> Optional[Dict[str, Any]]:
        assert len(info_hash) == NODE_ID_LENGTH
        with self._lock:
            entry = self._entries.get(info_hash)
            result = None if entry is None else self._detail_record_locked(entry)
        assert result is None or result["info_hash"] == info_hash.hex()
        return result

    # Parents: record_hash, record_hashes
    # Keywords: record, counters, peer, evict
    def _record_locked(self, info_hash: bytes, source: str, peer: Optional[Peer], moment: float) -> bool:
        assert self._lock.locked()
        entry, created = self._get_or_create_locked(info_hash, moment)
        entry.seen_count += 1
        entry.last_seen = max(entry.last_seen, moment)
        if source == SOURCE_ANNOUNCE_PEER:
            entry.announce_count += 1
        if peer is not None:
            self._add_peer_locked(entry, peer)
            self._index_entry_locked(entry)
        self._observations += 1
        if len(self._entries) > self.max_entries:
            self._evict_locked()
        assert entry.seen_count >= 1
        return created

    # Parents: _record_locked, store_metadata
    # Keywords: get or create, entry, locked
    def _get_or_create_locked(self, info_hash: bytes, now: float) -> Tuple[TorrentEntry, bool]:
        assert self._lock.locked()
        entry = self._entries.get(info_hash)
        created = entry is None
        if entry is None:
            entry = TorrentEntry(info_hash, now)
            self._entries[info_hash] = entry
            self._state_counts[FETCH_PENDING] += 1
        assert entry.info_hash == info_hash
        return entry, created

    # Parents: _record_locked, add_peers, add_lookup_result
    # Keywords: peer, add, bounded, ordered
    def _add_peer_locked(self, entry: TorrentEntry, peer: Peer) -> bool:
        assert self._lock.locked() and len(peer) == 2
        is_new = remember(entry.peers, peer, None, self.max_peers_per_hash)
        assert len(entry.peers) <= self.max_peers_per_hash
        return is_new

    # Parents: add_lookup_result, next_fetch_candidates, store_metadata, mark_fetch_failed, release_fetch_claim
    # Keywords: state transition, counts, locked
    def _set_state_locked(self, entry: TorrentEntry, state: str) -> None:
        assert self._lock.locked() and state in FETCH_STATES
        self._state_counts[entry.fetch_state] -= 1
        entry.fetch_state = state
        self._state_counts[state] += 1
        assert self._state_counts[state] >= 1

    # Parents: _record_locked, add_peers, add_lookup_result, next_fetch_candidates, store_metadata, mark_fetch_failed, release_fetch_claim
    # Keywords: fetchable index, pending with peers, maintain
    def _index_entry_locked(self, entry: TorrentEntry) -> None:
        assert self._lock.locked()
        if entry.fetch_state == FETCH_PENDING and entry.peers:
            self._fetchable[entry.info_hash] = entry
        else:
            self._fetchable.pop(entry.info_hash, None)
        assert (entry.info_hash in self._fetchable) == (entry.fetch_state == FETCH_PENDING and bool(entry.peers))

    # Parents: _record_locked
    # Keywords: eviction, bounded, lowest value, never in progress
    def _evict_locked(self) -> int:
        assert self._lock.locked()
        wanted = max(1, self.max_entries // EVICTION_DIVISOR)
        candidates = [entry for entry in self._entries.values() if entry.fetch_state != FETCH_IN_PROGRESS and not entry.lookup_in_progress]
        removed = 0
        for entry in sorted(candidates, key=eviction_key)[:wanted]:
            del self._entries[entry.info_hash]
            self._fetched.pop(entry.info_hash, None)
            self._fetchable.pop(entry.info_hash, None)
            self._state_counts[entry.fetch_state] -= 1
            removed += 1
        self._evicted += removed
        assert removed <= wanted
        return removed

    # Parents: search
    # Keywords: search record, copy, json ready
    def _search_record_locked(self, entry: TorrentEntry) -> Dict[str, Any]:
        assert self._lock.locked() and entry.metadata is not None
        metadata = entry.metadata
        result = {
            "info_hash": entry.info_hash.hex(),
            "name": metadata.name,
            "size": metadata.total_size,
            "file_count": metadata.file_count,
            "seen_count": entry.seen_count,
            "announce_count": entry.announce_count,
            "first_seen": entry.first_seen,
            "last_seen": entry.last_seen,
        }
        assert result["info_hash"] == entry.info_hash.hex()
        return result

    # Parents: torrent_detail
    # Keywords: detail record, files, peers, copy
    def _detail_record_locked(self, entry: TorrentEntry) -> Dict[str, Any]:
        assert self._lock.locked()
        metadata_record: Optional[Dict[str, Any]] = None
        if entry.metadata is not None:
            metadata = entry.metadata
            metadata_record = {
                "name": metadata.name,
                "size": metadata.total_size,
                "piece_length": metadata.piece_length,
                "file_count": metadata.file_count,
                "files": [{"path": entry_file.path, "length": entry_file.length} for entry_file in metadata.files],
                "is_private": metadata.is_private,
                "fetched_at": metadata.fetched_at,
                "source_peer": None if metadata.source_peer is None else {"ip": metadata.source_peer[0], "port": metadata.source_peer[1]},
            }
        result = {
            "info_hash": entry.info_hash.hex(),
            "seen_count": entry.seen_count,
            "announce_count": entry.announce_count,
            "first_seen": entry.first_seen,
            "last_seen": entry.last_seen,
            "fetch_state": entry.fetch_state,
            "fetch_attempts": entry.fetch_attempts,
            "retry_after": entry.retry_after,
            "last_error": entry.last_error,
            "lookups_started": entry.lookups_started,
            "peers": [{"ip": ip, "port": port} for ip, port in entry.peers],
            "metadata": metadata_record,
        }
        assert (result["metadata"] is None) == (entry.fetch_state != FETCH_DONE)
        return result

    # Parents: record_hash, record_hashes, add_lookup_result, next_fetch_candidates, store_metadata, mark_fetch_failed, release_fetch_claim
    # Keywords: invariants, counts, consistency
    def _assert_invariants_locked(self) -> None:
        assert self._lock.locked()
        assert sum(self._state_counts.values()) == len(self._entries)
        assert len(self._fetched) == self._state_counts[FETCH_DONE]
        assert self._lookups_in_progress >= 0
