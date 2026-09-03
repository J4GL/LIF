"""Wires the crawler, scheduler, fetch workers and web server together and owns shutdown."""
import logging
import math
import queue
import threading
import time
import webbrowser
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple

from dht_scraper.dht_crawler import DhtCrawler
from dht_scraper.dht_node_sockets import MAX_NODES, NodeSocket, close_node_sockets, create_node_sockets
from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.fetch_scheduler import FetchScheduler
from dht_scraper.fetch_worker_pool import DEFAULT_FETCH_WORKERS, MAX_FETCH_WORKERS, FetchFunction, FetchWorkerPool
from dht_scraper.torrent_catalog import FetchCandidate, TorrentCatalog
from dht_scraper.web_interface import DEFAULT_WEB_HOST, DEFAULT_WEB_PORT, CatalogWebServer, create_web_server, serve_web_server, web_url

LOGGER = logging.getLogger(LOGGER_NAME)
DEFAULT_PORT = 6881
DEFAULT_NODES = 8
DEFAULT_BATCH_SIZE = 5
DEFAULT_INTERVAL = 0.1
THREAD_JOIN_TIMEOUT_SECONDS = 5.0
WAIT_POLL_SECONDS = 0.5
SUMMARY_INTERVAL_SECONDS = 10.0
STOP_REASONS = ("duration", "interrupt", "thread_failure")
SocketFactory = Callable[[int, int], List[NodeSocket]]
BrowserOpener = Callable[[str], Any]


# Parents: RuntimeSettings.__init__, parse_arguments
# Keywords: settings, validation, error message, single source
def settings_error(port: int, nodes: int, web_host: str, web_port: int, duration: Optional[float], fetch_workers: int, batch_size: int, interval: float) -> Optional[str]:
    assert isinstance(nodes, int) and isinstance(fetch_workers, int)
    result: Optional[str] = None
    if not 0 <= port <= 65535:
        result = "--port must be between 0 and 65535"
    elif not 1 <= nodes <= MAX_NODES:
        result = "--nodes must be between 1 and %d" % MAX_NODES
    elif port != 0 and port + nodes - 1 > 65535:
        result = "--port plus --nodes exceeds port 65535"
    elif not web_host:
        result = "--web-host must not be empty"
    elif not 0 <= web_port <= 65535:
        result = "--web-port must be between 0 and 65535"
    elif duration is not None and (not math.isfinite(duration) or duration < 0):
        result = "--duration must be a finite number that is not negative"
    elif not 1 <= fetch_workers <= MAX_FETCH_WORKERS:
        result = "--fetch-workers must be between 1 and %d" % MAX_FETCH_WORKERS
    elif batch_size < 1:
        result = "--batch-size must be at least 1"
    elif not math.isfinite(interval) or interval <= 0:
        result = "--interval must be a finite number greater than 0"
    assert result is None or result.startswith("--")
    return result


class RuntimeSettings:
    """Validated run settings."""

    __slots__ = ("port", "nodes", "web_host", "web_port", "duration", "fetch_workers", "batch_size", "interval", "fetch_enabled", "open_browser")

    # Parents: build_settings, tests
    # Keywords: settings, validation, defaults
    def __init__(
        self,
        port: int = DEFAULT_PORT,
        nodes: int = DEFAULT_NODES,
        web_host: str = DEFAULT_WEB_HOST,
        web_port: int = DEFAULT_WEB_PORT,
        duration: Optional[float] = None,
        fetch_workers: int = DEFAULT_FETCH_WORKERS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        interval: float = DEFAULT_INTERVAL,
        fetch_enabled: bool = True,
        open_browser: bool = True,
    ) -> None:
        assert settings_error(port, nodes, web_host, web_port, duration, fetch_workers, batch_size, interval) is None
        self.port = port
        self.nodes = nodes
        self.web_host = web_host
        self.web_port = web_port
        self.duration = duration
        self.fetch_workers = fetch_workers
        self.batch_size = batch_size
        self.interval = interval
        self.fetch_enabled = fetch_enabled
        self.open_browser = open_browser
        assert self.nodes >= 1


class ScraperRuntime:
    """Starts and stops every thread of the scraper."""

    # Parents: run_scraper, tests
    # Keywords: runtime, wiring, init
    def __init__(
        self,
        settings: RuntimeSettings,
        catalog: TorrentCatalog,
        fetch_function: FetchFunction,
        bootstrap_nodes: Sequence[Tuple[str, int]],
        socket_factory: SocketFactory = create_node_sockets,
        browser_opener: BrowserOpener = webbrowser.open,
    ) -> None:
        assert isinstance(settings, RuntimeSettings) and len(bootstrap_nodes) > 0
        self.settings = settings
        self.catalog = catalog
        self.fetch_function = fetch_function
        self.bootstrap_nodes = list(bootstrap_nodes)
        self.socket_factory = socket_factory
        self.browser_opener = browser_opener
        self.stop_event = threading.Event()
        self.node_sockets: List[NodeSocket] = []
        self.crawler: Optional[DhtCrawler] = None
        self.crawler_thread: Optional[threading.Thread] = None
        self.candidate_queue: "queue.Queue[FetchCandidate]" = queue.Queue(maxsize=settings.fetch_workers)
        self.scheduler: Optional[FetchScheduler] = None
        self.scheduler_thread: Optional[threading.Thread] = None
        self.worker_pool: Optional[FetchWorkerPool] = None
        self.web_server: Optional[CatalogWebServer] = None
        self.web_thread: Optional[threading.Thread] = None
        self.start_time = time.monotonic()
        self.last_summary_time = self.start_time
        self.stopped = False
        assert not self.stop_event.is_set()

    # Parents: start
    # Keywords: sockets, bind, nodes
    def open_sockets(self) -> None:
        assert not self.node_sockets
        self.node_sockets = self.socket_factory(self.settings.port, self.settings.nodes)
        assert len(self.node_sockets) == self.settings.nodes

    # Parents: start
    # Keywords: crawler, create, lookup
    def create_crawler(self) -> None:
        assert self.node_sockets and self.crawler is None
        self.crawler = DhtCrawler(self.node_sockets, self.catalog, self.bootstrap_nodes, self.settings.batch_size, self.settings.interval)
        assert self.crawler is not None

    # Parents: start
    # Keywords: web server, thread, bind first
    def start_web(self) -> None:
        assert self.web_server is None
        self.web_server = create_web_server(self.settings.web_host, self.settings.web_port, self.catalog, self.snapshot_stats)
        self.web_thread = threading.Thread(target=serve_web_server, args=(self.web_server,), name="web", daemon=True)
        self.web_thread.start()
        assert self.web_thread.is_alive()

    # Parents: start
    # Keywords: scheduler, workers, fetch enabled
    def start_fetching(self) -> None:
        assert self.scheduler is None and self.worker_pool is None
        if not self.settings.fetch_enabled:
            LOGGER.info("metadata fetching disabled")
            return
        self.worker_pool = FetchWorkerPool(self.catalog, self.candidate_queue, self.stop_event, self.fetch_function, self.settings.fetch_workers)
        self.worker_pool.start()
        self.scheduler = FetchScheduler(self.catalog, self.candidate_queue, self.stop_event)
        self.scheduler_thread = threading.Thread(target=self.scheduler.run, name="scheduler", daemon=True)
        self.scheduler_thread.start()
        assert self.scheduler_thread.is_alive()

    # Parents: start
    # Keywords: crawler thread, start
    def start_crawler(self) -> None:
        assert self.crawler is not None and self.crawler_thread is None
        self.crawler_thread = threading.Thread(target=self.run_crawler_thread, name="crawler", daemon=True)
        self.crawler_thread.start()
        assert self.crawler_thread.is_alive()

    # Parents: start_crawler (thread target)
    # Keywords: crawler, fail fast, stop event
    def run_crawler_thread(self) -> None:
        assert self.crawler is not None
        try:
            self.crawler.run(self.stop_event)
        except Exception:
            LOGGER.exception("crawler thread failed")
            self.stop_event.set()
        assert True

    # Parents: start
    # Keywords: browser, open, helper thread
    def open_browser_page(self) -> None:
        assert self.web_server is not None
        if not self.settings.open_browser:
            return
        url = web_url(self.settings.web_host, self.web_server.server_address[1])
        LOGGER.info("opening %s in the default browser", url)
        browser_thread = threading.Thread(target=self.open_browser_quietly, args=(url,), name="browser", daemon=True)
        browser_thread.start()
        assert browser_thread.daemon

    # Parents: open_browser_page (thread target)
    # Keywords: browser, open, failure logged
    def open_browser_quietly(self, url: str) -> None:
        assert url.startswith("http://")
        try:
            self.browser_opener(url)
        except Exception as error:
            LOGGER.warning("cannot open the browser: %s", error)
        assert True

    # Parents: run, tests
    # Keywords: start, ordering, cleanup on failure
    def start(self) -> None:
        assert not self.stopped and self.crawler_thread is None
        try:
            self.open_sockets()
            self.create_crawler()
            self.start_web()
            self.start_fetching()
            self.start_crawler()
            self.open_browser_page()
        except Exception:
            self.stop()
            raise
        assert self.crawler_thread is not None and self.web_server is not None

    # Parents: run
    # Keywords: wait, duration, interrupt, summary
    def wait_until_stopped(self, duration: Optional[float]) -> str:
        assert duration is None or duration >= 0
        deadline = None if duration is None else time.monotonic() + duration
        reason = "thread_failure"
        try:
            while not self.stop_event.wait(WAIT_POLL_SECONDS):
                if deadline is not None and time.monotonic() >= deadline:
                    reason = "duration"
                    break
                if time.monotonic() - self.last_summary_time >= SUMMARY_INTERVAL_SECONDS:
                    self.log_summary()
        except KeyboardInterrupt:
            LOGGER.info("interrupted by user")
            reason = "interrupt"
        assert reason in STOP_REASONS
        return reason

    # Parents: run, start
    # Keywords: shutdown, join, idempotent
    def stop(self) -> None:
        assert self.settings is not None
        if self.stopped:
            return
        self.stopped = True
        self.stop_event.set()
        LOGGER.info("shutdown: stopping threads")
        if self.web_server is not None:
            self.web_server.shutdown()
            self.web_server.server_close()
        if self.web_thread is not None:
            self.web_thread.join(THREAD_JOIN_TIMEOUT_SECONDS)
        if self.scheduler_thread is not None:
            self.scheduler_thread.join(THREAD_JOIN_TIMEOUT_SECONDS)
        if self.worker_pool is not None:
            self.worker_pool.join(THREAD_JOIN_TIMEOUT_SECONDS)
        if self.crawler_thread is not None:
            self.crawler_thread.join(THREAD_JOIN_TIMEOUT_SECONDS)
        if self.crawler_thread is not None and self.crawler_thread.is_alive():
            LOGGER.warning("crawler thread still running (blocked in DNS?); leaving its sockets open")
        else:
            close_node_sockets(self.node_sockets)
        self.node_sockets = []
        self.log_summary()
        assert self.stopped and self.stop_event.is_set()

    # Parents: start_web (stats provider), log_summary, run
    # Keywords: statistics, merge, snapshot
    def snapshot_stats(self) -> Dict[str, Any]:
        assert self.catalog is not None
        stats: Dict[str, Any] = {"uptime_seconds": time.monotonic() - self.start_time, "nodes": self.settings.nodes, "fetch_enabled": self.settings.fetch_enabled}
        if self.crawler is not None:
            stats.update(self.crawler.snapshot_stats())
        stats.update(self.catalog.snapshot_counts())
        if self.worker_pool is not None:
            stats.update(self.worker_pool.snapshot_counts())
        stats["fetch_queue_size"] = self.candidate_queue.qsize()
        assert "hashes_seen" in stats and "uptime_seconds" in stats
        return stats

    # Parents: wait_until_stopped, stop
    # Keywords: summary, log, progress
    def log_summary(self) -> None:
        assert self.catalog is not None
        stats = self.snapshot_stats()
        LOGGER.info(
            "summary: nodes=%d sent=%d received=%d samples=%d hashes=%d metadata=%d pending=%d in_progress=%d failed=%d fetch_queue=%d crawl_queue=%d lookups=%d",
            stats["nodes"], stats.get("packets_sent", 0), stats.get("packets_received", 0), stats.get("samples_received", 0),
            stats["hashes_seen"], stats["with_metadata"], stats["fetch_pending"], stats["fetch_in_progress"], stats["fetch_failed"],
            stats["fetch_queue_size"], stats.get("queue_size", 0), stats.get("active_lookups", 0),
        )
        self.last_summary_time = time.monotonic()
        assert self.last_summary_time >= self.start_time

    # Parents: run_scraper
    # Keywords: run, start, wait, stop
    def run(self, duration: Optional[float]) -> Dict[str, Any]:
        assert duration is None or duration >= 0
        self.start()
        reason = self.wait_until_stopped(duration)
        self.stop()
        stats = self.snapshot_stats()
        LOGGER.info("finished: reason=%s hashes=%d metadata=%d", reason, stats["hashes_seen"], stats["with_metadata"])
        assert self.stopped
        return stats
