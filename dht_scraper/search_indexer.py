"""Index thread: copies torrents with metadata and their counters from the catalog to the SQLite database."""
import logging
import sqlite3
import threading
import time
from typing import Any, Callable, Dict, List, Optional

from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.torrent_catalog import IndexBase, IndexSnapshot, TorrentCatalog
from dht_scraper.torrent_database import TorrentDatabase

LOGGER = logging.getLogger(LOGGER_NAME)
DOCUMENT_SECONDS = 1.0
COUNTER_SECONDS = 30.0
COUNT_SECONDS = 10.0
BACKOFF_START_SECONDS = 1.0
BACKOFF_MAX_SECONDS = 30.0
FINAL_FLUSH_SECONDS = 3.0
BATCH_SIZE = 250
POLL_SECONDS = 0.5
NO_BASE: IndexBase = (0, 0, None)
Clock = Callable[[], float]


# Parents: SearchIndexer.flush_documents
# Keywords: document, absolute counters, base, files
def build_document(snapshot: IndexSnapshot, base: IndexBase) -> Dict[str, Any]:
    assert len(base) == 3
    metadata = snapshot.metadata
    result = {
        "info_hash": snapshot.info_hash.hex(),
        "name": metadata.name,
        "size": metadata.total_size,
        "file_count": metadata.file_count,
        "piece_length": metadata.piece_length,
        "is_private": metadata.is_private,
        "fetched_at": metadata.fetched_at,
        "seen_count": base[0] + snapshot.seen_count,
        "announce_count": base[1] + snapshot.announce_count,
        "first_seen": snapshot.first_seen if base[2] is None else min(base[2], snapshot.first_seen),
        "last_seen": snapshot.last_seen,
        "files": [[item.path, item.length] for item in metadata.files],
    }
    assert result["seen_count"] >= snapshot.seen_count
    return result


# Parents: SearchIndexer.flush_counters
# Keywords: counter update, absolute counters, base
def build_counter_update(snapshot: IndexSnapshot) -> Dict[str, Any]:
    assert snapshot.base is not None
    result = {
        "info_hash": snapshot.info_hash.hex(),
        "seen_count": snapshot.base[0] + snapshot.seen_count,
        "announce_count": snapshot.base[1] + snapshot.announce_count,
        "last_seen": snapshot.last_seen,
    }
    assert result["seen_count"] >= snapshot.seen_count
    return result


class SearchIndexer:
    """Flushes new documents every second and counters every 30 s; retries with backoff on failure."""

    # Parents: ScraperRuntime.start_indexer, tests
    # Keywords: indexer, intervals, backoff, statistics
    def __init__(
        self,
        catalog: TorrentCatalog,
        database: TorrentDatabase,
        document_seconds: float = DOCUMENT_SECONDS,
        counter_seconds: float = COUNTER_SECONDS,
        count_seconds: float = COUNT_SECONDS,
        backoff_start: float = BACKOFF_START_SECONDS,
        backoff_max: float = BACKOFF_MAX_SECONDS,
        batch_size: int = BATCH_SIZE,
        final_flush_seconds: float = FINAL_FLUSH_SECONDS,
        clock: Clock = time.monotonic,
    ) -> None:
        assert document_seconds > 0 and counter_seconds > 0 and count_seconds > 0 and 0 < backoff_start <= backoff_max and batch_size >= 1
        self.catalog = catalog
        self.database = database
        self.document_seconds = document_seconds
        self.counter_seconds = counter_seconds
        self.count_seconds = count_seconds
        self.backoff_start = backoff_start
        self.backoff_max = backoff_max
        self.batch_size = batch_size
        self.final_flush_seconds = final_flush_seconds
        self.clock = clock
        self.stop_event = threading.Event()
        self.thread: Optional[threading.Thread] = None
        self.lock = threading.Lock()
        self.documents: Optional[int] = None
        self.upserts = 0
        self.updates = 0
        self.errors = 0
        self.backoff = 0.0
        self.next_documents = self.next_counters = self.next_count = self.wake_at = 0.0
        assert self.thread is None

    # Parents: ScraperRuntime.start_indexer, tests
    # Keywords: start, first count, thread index
    def start(self) -> None:
        assert self.thread is None
        try:
            self.refresh_count()
        except sqlite3.Error as error:
            self.record_error(error)
        now = self.clock()
        self.next_documents, self.next_counters, self.next_count = now, now + self.counter_seconds, now + self.count_seconds
        self.thread = threading.Thread(target=self.run, name="index", daemon=True)
        self.thread.start()
        assert self.thread.is_alive()

    # Parents: ScraperRuntime.stop, tests
    # Keywords: stop, event
    def stop(self) -> None:
        self.stop_event.set()
        assert self.stop_event.is_set()

    # Parents: ScraperRuntime.stop, tests
    # Keywords: join, timeout, finished
    def join(self, timeout: float) -> bool:
        assert timeout >= 0
        if self.thread is not None:
            self.thread.join(timeout)
        result = self.thread is None or not self.thread.is_alive()
        assert isinstance(result, bool)
        return result

    # Parents: start (thread target)
    # Keywords: loop, due work, backoff, final flush
    def run(self) -> None:
        assert self.thread is not None
        while not self.stop_event.is_set():
            if self.clock() >= self.wake_at:
                self.run_due_work()
            self.stop_event.wait(max(0.0, min(self.wake_at - self.clock(), POLL_SECONDS)))
        self.final_flush()
        assert self.stop_event.is_set()

    # Parents: run
    # Keywords: due work, documents, counters, count, retry
    def run_due_work(self) -> None:
        now = self.clock()
        try:
            if now >= self.next_documents:
                self.flush_documents()
                self.next_documents = now + self.document_seconds
            if now >= self.next_counters:
                self.flush_counters()
                self.next_counters = now + self.counter_seconds
            if now >= self.next_count:
                self.refresh_count()
                self.next_count = now + self.count_seconds
            self.backoff = 0.0
            self.wake_at = min(self.next_documents, self.next_counters, self.next_count)
        except sqlite3.Error as error:
            self.record_error(error)
            self.backoff = self.backoff_start if self.backoff == 0.0 else min(2.0 * self.backoff, self.backoff_max)
            self.wake_at = now + self.backoff
        assert self.wake_at >= now or self.backoff == 0.0

    # Parents: run
    # Keywords: stop, last flush, bounded time
    def final_flush(self) -> None:
        deadline = self.clock() + self.final_flush_seconds
        try:
            self.flush_documents(deadline)
            self.flush_counters(deadline)
        except sqlite3.Error as error:
            self.record_error(error)
        assert self.stop_event.is_set()

    # Parents: run_due_work, final_flush, tests
    # Keywords: documents, base lookup, write, requeue on failure
    def flush_documents(self, deadline: Optional[float] = None) -> None:
        while deadline is None or self.clock() < deadline:
            batch = self.catalog.drain_index_documents(self.batch_size)
            if not batch:
                return
            try:
                unknown = [snapshot.info_hash.hex() for snapshot in batch if snapshot.base is None]
                found = self.database.lookup_counters(unknown) if unknown else {}
                bases: List[IndexBase] = [snapshot.base if snapshot.base is not None else found.get(snapshot.info_hash.hex(), NO_BASE) for snapshot in batch]
                self.database.write_documents([build_document(snapshot, base) for snapshot, base in zip(batch, bases)])
            except sqlite3.Error:
                self.catalog.requeue_index_documents(batch)
                raise
            for snapshot, base in zip(batch, bases):
                self.catalog.set_index_base(snapshot.info_hash, base)
            with self.lock:
                self.upserts += len(batch)
        assert deadline is not None

    # Parents: run_due_work, final_flush
    # Keywords: counters, update, missing row, resend in full
    def flush_counters(self, deadline: Optional[float] = None) -> None:
        while deadline is None or self.clock() < deadline:
            batch = self.catalog.drain_index_counters(self.batch_size)
            if not batch:
                return
            try:
                missing = self.database.update_counters([build_counter_update(snapshot) for snapshot in batch])
            except sqlite3.Error:
                self.catalog.requeue_index_counters(batch)
                raise
            for info_hash in missing:
                self.catalog.reset_index_base(bytes.fromhex(info_hash))
            with self.lock:
                self.updates += len(batch) - len(missing)
        assert deadline is not None

    # Parents: start, run_due_work
    # Keywords: count, documents, statistics
    def refresh_count(self) -> None:
        count = self.database.count_documents()
        with self.lock:
            self.documents = count
        assert count >= 0

    # Parents: start, run_due_work, final_flush
    # Keywords: error, count, log
    def record_error(self, error: sqlite3.Error) -> None:
        assert isinstance(error, sqlite3.Error)
        with self.lock:
            self.errors += 1
        LOGGER.error("search database: %s", error)

    # Parents: ScraperRuntime.snapshot_stats, tests
    # Keywords: statistics, backlog, documents, errors
    def snapshot_counts(self) -> Dict[str, int]:
        backlog = self.catalog.index_backlog()
        with self.lock:
            result = {
                "index_upserts": self.upserts,
                "index_updates": self.updates,
                "index_errors": self.errors,
                "index_backlog": backlog["documents"] + backlog["counters"],
                "index_dropped": backlog["dropped"],
            }
            if self.documents is not None:
                result["index_documents"] = self.documents
        assert result["index_errors"] >= 0
        return result
