"""Scheduler thread: re-ranks fetch candidates by popularity and feeds the worker queue."""
import logging
import queue
import threading
from typing import Optional

from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.torrent_catalog import FetchCandidate, TorrentCatalog

LOGGER = logging.getLogger(LOGGER_NAME)
SCHEDULER_IDLE_SECONDS = 0.5
SCHEDULER_BUSY_SECONDS = 0.05


class FetchScheduler:
    """Fills the bounded candidate queue from the catalog so ranking is recomputed on every round."""

    # Parents: ScraperRuntime.start_fetching
    # Keywords: scheduler, queue, ranking, init
    def __init__(self, catalog: TorrentCatalog, candidate_queue: "queue.Queue[FetchCandidate]", stop_event: threading.Event, idle_seconds: float = SCHEDULER_IDLE_SECONDS, busy_seconds: float = SCHEDULER_BUSY_SECONDS) -> None:
        assert candidate_queue.maxsize >= 1 and idle_seconds > 0 and busy_seconds > 0
        self.catalog = catalog
        self.candidate_queue = candidate_queue
        self.stop_event = stop_event
        self.idle_seconds = idle_seconds
        self.busy_seconds = busy_seconds
        self.rounds = 0
        self.queued_total = 0
        self.drained_total = 0
        assert self.rounds == 0

    # Parents: schedule_round
    # Keywords: free slots, queue capacity
    def free_slots(self) -> int:
        assert self.candidate_queue.maxsize >= 1
        result = max(0, self.candidate_queue.maxsize - self.candidate_queue.qsize())
        assert 0 <= result <= self.candidate_queue.maxsize
        return result

    # Parents: run, tests
    # Keywords: round, candidates, claim, enqueue
    def schedule_round(self, now: Optional[float] = None) -> int:
        assert now is None or now >= 0
        self.rounds += 1
        wanted = self.free_slots()
        if wanted == 0:
            return 0
        queued = 0
        for candidate in self.catalog.next_fetch_candidates(wanted, now):
            self.candidate_queue.put_nowait(candidate)
            queued += 1
        self.queued_total += queued
        if queued:
            LOGGER.debug("scheduler queued %d candidates", queued)
        assert 0 <= queued <= wanted
        return queued

    # Parents: run
    # Keywords: drain, release claims, shutdown
    def drain_queue(self) -> int:
        assert self.candidate_queue is not None
        drained = 0
        while True:
            try:
                candidate = self.candidate_queue.get_nowait()
            except queue.Empty:
                break
            self.catalog.release_fetch_claim(candidate[0])
            drained += 1
        self.drained_total += drained
        assert self.candidate_queue.empty()
        return drained

    # Parents: ScraperRuntime.start_fetching (thread target)
    # Keywords: scheduler loop, stop event, wait
    def run(self) -> None:
        assert isinstance(self.stop_event, threading.Event)
        LOGGER.info("scheduler started")
        try:
            while not self.stop_event.is_set():
                queued = self.schedule_round()
                self.stop_event.wait(self.busy_seconds if queued else self.idle_seconds)
        finally:
            self.drain_queue()
        LOGGER.info("scheduler stopped: rounds=%d queued=%d drained=%d", self.rounds, self.queued_total, self.drained_total)
        assert self.candidate_queue.empty()
