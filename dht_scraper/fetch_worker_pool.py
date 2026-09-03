"""Worker threads that fetch metadata from peers and store it in the catalog."""
import logging
import queue
import threading
import time
from typing import Callable, Dict, List, Optional

from dht_scraper.bencode_codec import decode_bencode
from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.krpc_messages import Address
from dht_scraper.metadata_fetcher import MetadataFetchError
from dht_scraper.node_identity import NODE_ID_LENGTH
from dht_scraper.torrent_catalog import FetchCandidate, TorrentCatalog
from dht_scraper.torrent_info_summary import summarize_info_dict

LOGGER = logging.getLogger(LOGGER_NAME)
DEFAULT_FETCH_WORKERS = 32
MAX_FETCH_WORKERS = 256
MAX_PEERS_PER_ATTEMPT = 4
WORKER_QUEUE_TIMEOUT_SECONDS = 0.5
FetchFunction = Callable[[bytes, Address, Optional[threading.Event]], bytes]


class FetchWorkerPool:
    """Consumes claimed candidates, tries a few peers each, stores metadata or reports failure."""

    # Parents: ScraperRuntime.start_fetching
    # Keywords: worker pool, threads, fetch function, init
    def __init__(self, catalog: TorrentCatalog, candidate_queue: "queue.Queue[FetchCandidate]", stop_event: threading.Event, fetch_function: FetchFunction, worker_count: int) -> None:
        assert 1 <= worker_count <= MAX_FETCH_WORKERS and callable(fetch_function)
        self.catalog = catalog
        self.candidate_queue = candidate_queue
        self.stop_event = stop_event
        self.fetch_function = fetch_function
        self.worker_count = worker_count
        self.threads: List[threading.Thread] = []
        self._lock = threading.Lock()
        self.attempts = 0
        self.successes = 0
        self.failures = 0
        assert not self.threads

    # Parents: ScraperRuntime.start_fetching
    # Keywords: start, threads, daemon
    def start(self) -> None:
        assert not self.threads
        for index in range(self.worker_count):
            thread = threading.Thread(target=self.worker_loop, args=(index,), name="fetch-%d" % index, daemon=True)
            thread.start()
            self.threads.append(thread)
        LOGGER.info("fetch workers started: %d", self.worker_count)
        assert len(self.threads) == self.worker_count

    # Parents: start (thread target)
    # Keywords: worker loop, queue get, stop event
    def worker_loop(self, worker_index: int) -> None:
        assert 0 <= worker_index < self.worker_count
        while not self.stop_event.is_set():
            try:
                candidate = self.candidate_queue.get(timeout=WORKER_QUEUE_TIMEOUT_SECONDS)
            except queue.Empty:
                continue
            try:
                self.process_candidate(candidate)
            except Exception:
                LOGGER.exception("worker %d failed on %s", worker_index, candidate[0].hex())
                self.catalog.release_fetch_claim(candidate[0])
        assert self.stop_event.is_set()

    # Parents: worker_loop, tests
    # Keywords: fetch, peers, summarize, store, mark failed
    def process_candidate(self, candidate: FetchCandidate) -> bool:
        info_hash, peers = candidate
        assert len(info_hash) == NODE_ID_LENGTH and len(peers) >= 1
        tried: List[Address] = []
        reason = "stopped"
        stored = False
        for peer in list(reversed(peers))[:MAX_PEERS_PER_ATTEMPT]:
            if self.stop_event.is_set():
                break
            tried.append(peer)
            with self._lock:
                self.attempts += 1
            try:
                raw = self.fetch_function(info_hash, peer, self.stop_event)
                metadata = summarize_info_dict(info_hash, decode_bencode(raw), time.time(), peer)
            except MetadataFetchError as error:
                reason = error.reason
                continue
            except (ValueError, RecursionError):
                reason = "bad_info_dict"
                continue
            except Exception:
                LOGGER.exception("unexpected error while fetching %s from %s", info_hash.hex(), peer)
                reason = "unexpected"
                continue
            self.catalog.store_metadata(metadata)
            stored = True
            LOGGER.info("metadata stored: %s name=%r size=%d files=%d from %s:%d", info_hash.hex(), metadata.name, metadata.total_size, metadata.file_count, peer[0], peer[1])
            break
        with self._lock:
            if stored:
                self.successes += 1
            elif tried:
                self.failures += 1
        if not stored:
            if not tried:
                self.catalog.release_fetch_claim(info_hash)
            else:
                state = self.catalog.mark_fetch_failed(info_hash, tried, reason)
                LOGGER.debug("fetch failed for %s (%s), state now %s", info_hash.hex(), reason, state)
        assert isinstance(stored, bool)
        return stored

    # Parents: ScraperRuntime.snapshot_stats
    # Keywords: counts, statistics, workers alive
    def snapshot_counts(self) -> Dict[str, int]:
        assert self.worker_count >= 1
        with self._lock:
            result = {
                "fetch_attempts": self.attempts,
                "fetch_successes": self.successes,
                "fetch_failures": self.failures,
                "fetch_workers": self.worker_count,
                "fetch_workers_alive": self.alive_count(),
            }
        assert result["fetch_workers_alive"] <= result["fetch_workers"]
        return result

    # Parents: snapshot_counts, join
    # Keywords: threads alive, count
    def alive_count(self) -> int:
        assert self.threads is not None
        result = sum(1 for thread in self.threads if thread.is_alive())
        assert 0 <= result <= self.worker_count
        return result

    # Parents: ScraperRuntime.stop
    # Keywords: join, shutdown, timeout
    def join(self, timeout: float) -> int:
        assert timeout >= 0
        deadline = time.monotonic() + timeout
        for thread in self.threads:
            thread.join(max(0.0, deadline - time.monotonic()))
        alive = self.alive_count()
        if alive:
            LOGGER.warning("%d fetch workers still running after %.1fs", alive, timeout)
        assert alive >= 0
        return alive
