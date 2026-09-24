"""Fetch engine: one thread runs an asyncio loop that downloads metadata from many peers at once."""
import asyncio
import collections
import hashlib
import logging
import threading
import time
from typing import Awaitable, Callable, Dict, List, Optional, Set, Tuple

try:
    import resource
except ImportError:  # pragma: no cover (Windows has no resource module)
    resource = None  # type: ignore

from dht_scraper.bencode_codec import decode_bencode
from dht_scraper.bounded_recent_map import remember
from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.krpc_messages import Address
from dht_scraper.metadata_fetcher import MetadataFetchError
from dht_scraper.node_identity import NODE_ID_LENGTH
from dht_scraper.torrent_catalog import TorrentCatalog
from dht_scraper.torrent_info_summary import TorrentMetadata, summarize_info_dict
from dht_scraper.utp_transport import UtpSocket, open_utp_socket

LOGGER = logging.getLogger(LOGGER_NAME)
DEFAULT_MAX_CONNECTIONS = 256
MAX_CONNECTIONS_LIMIT = 1024
PEER_PARALLELISM = 3
STAGGER_SECONDS = 0.3
SCHEDULE_SECONDS = 0.05
LOCAL_ERROR_PAUSE_SECONDS = 1.0
STOP_GRACE_SECONDS = 1.0
FILE_LIMIT_MARGIN = 128
FILE_LIMIT_CEILING = 10240
UNBLAMED_REASONS = frozenset({"local_error", "stopped"})
UNREACHABLE_SECONDS = 600.0
UNREACHABLE_ENTRIES = 65536
UTP_AFTER_REASONS = frozenset({"connect"})
UTP_MAX_CONNECTIONS = 48
FetchFunction = Callable[[bytes, Address], Awaitable[bytes]]
UtpFunction = Callable[[bytes, Address, UtpSocket], Awaitable[bytes]]
AttemptOutcome = Tuple[Optional[TorrentMetadata], str]
LimitReader = Callable[[], Tuple[int, int]]
LimitWriter = Callable[[Tuple[int, int]], object]


# Parents: raise_open_file_limit
# Keywords: rlimit, nofile, read
def read_file_limit() -> Tuple[int, int]:
    assert resource is not None
    result = resource.getrlimit(resource.RLIMIT_NOFILE)
    assert len(result) == 2
    return result


# Parents: raise_open_file_limit
# Keywords: rlimit, nofile, write
def write_file_limit(limits: Tuple[int, int]) -> None:
    assert resource is not None and len(limits) == 2
    resource.setrlimit(resource.RLIMIT_NOFILE, limits)
    assert True


# Parents: ScraperRuntime.start_fetching
# Keywords: open files, soft limit, macOS 256, connections
def raise_open_file_limit(wanted: int, get_limit: Optional[LimitReader] = None, set_limit: Optional[LimitWriter] = None) -> int:
    assert wanted >= 1
    if resource is None and get_limit is None:
        return -1
    soft, hard = (get_limit or read_file_limit)()
    infinite = resource.RLIM_INFINITY if resource is not None else -1
    ceiling = FILE_LIMIT_CEILING if hard == infinite else min(FILE_LIMIT_CEILING, hard)
    target = min(wanted, ceiling)
    if soft >= wanted or target <= soft:
        return soft
    try:
        (set_limit or write_file_limit)((target, hard))
    except (ValueError, OSError) as error:
        LOGGER.warning("cannot raise the open file limit from %d to %d: %s", soft, target, error)
        return soft
    LOGGER.info("open file limit raised from %d to %d", soft, target)
    assert target > soft
    return target


class FetchEngine:
    """Claims fetch candidates while connection slots are free and runs their attempts concurrently."""

    # Parents: ScraperRuntime.start_fetching, tests
    # Keywords: engine, init, limits, counters
    def __init__(
        self,
        catalog: TorrentCatalog,
        stop_event: threading.Event,
        fetch_function: FetchFunction,
        max_connections: int = DEFAULT_MAX_CONNECTIONS,
        peer_parallelism: int = PEER_PARALLELISM,
        stagger_seconds: float = STAGGER_SECONDS,
        local_error_pause: float = LOCAL_ERROR_PAUSE_SECONDS,
        retry_function: Optional[FetchFunction] = None,
        utp_function: Optional[UtpFunction] = None,
        utp_after_reasons: frozenset = UTP_AFTER_REASONS,
    ) -> None:
        assert 1 <= max_connections <= MAX_CONNECTIONS_LIMIT and peer_parallelism >= 1 and stagger_seconds >= 0 and callable(fetch_function)
        self.catalog = catalog
        self.stop_event = stop_event
        self.fetch_function = fetch_function
        self.retry_function = retry_function
        self.utp_function = utp_function
        self.utp_after_reasons = utp_after_reasons
        self.utp_socket: Optional[UtpSocket] = None
        self.utp_slots: Optional[asyncio.Semaphore] = None
        self.max_connections = max_connections
        self.peer_parallelism = peer_parallelism
        self.stagger_seconds = stagger_seconds
        self.local_error_pause = local_error_pause
        self.thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self.attempts = 0
        self.successes = 0
        self.failures = 0
        self.encrypted_attempts = 0
        self.encrypted_successes = 0
        self.utp_attempts = 0
        self.utp_successes = 0
        self.reasons: Dict[str, int] = {}
        self.active = 0
        self.waiting = 0
        self.max_active = 0
        self.pause_until = 0.0
        self.unreachable: "collections.OrderedDict[object, float]" = collections.OrderedDict()
        assert self.active == 0 and self.thread is None

    # Parents: ScraperRuntime.start_fetching, tests
    # Keywords: start, thread, event loop
    def start(self) -> None:
        assert self.thread is None
        self.thread = threading.Thread(target=self.run, name="fetch", daemon=True)
        self.thread.start()
        LOGGER.info("fetch engine started: up to %d connections", self.max_connections)
        assert self.thread.is_alive()

    # Parents: start (thread target)
    # Keywords: event loop, run, close
    def run(self) -> None:
        assert threading.current_thread() is self.thread
        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(self.main())
        except Exception:
            LOGGER.exception("fetch engine failed")
        finally:
            loop.close()
        LOGGER.info("fetch engine stopped: attempts=%d successes=%d failures=%d", self.attempts, self.successes, self.failures)
        assert loop.is_closed()

    # Parents: run
    # Keywords: scheduling, free slots, claim, stop polling, cancel on stop
    async def main(self) -> None:
        assert self.max_connections >= 1
        loop = asyncio.get_running_loop()
        slots = asyncio.Semaphore(self.max_connections)
        jobs: Set["asyncio.Task[None]"] = set()
        if self.utp_function is not None:
            try:
                self.utp_socket = await open_utp_socket()
                self.utp_slots = asyncio.Semaphore(UTP_MAX_CONNECTIONS)
            except OSError as error:
                LOGGER.warning("uTP disabled, cannot open its UDP socket: %s", error)
        try:
            while not self.stop_event.is_set():
                free = self.max_connections - self.active - self.waiting
                if free > 0 and loop.time() >= self.pause_until:
                    for info_hash, peers in self.catalog.next_fetch_candidates(free):
                        job = loop.create_task(self.run_job(info_hash, peers, slots))
                        jobs.add(job)
                        job.add_done_callback(jobs.discard)
                await asyncio.sleep(SCHEDULE_SECONDS)
        finally:
            for job in list(jobs):
                job.cancel()
            if jobs:
                await asyncio.wait(set(jobs), timeout=STOP_GRACE_SECONDS)
            if self.utp_socket is not None:
                self.utp_socket.close()
        assert self.stop_event.is_set()

    # Parents: main
    # Keywords: job, peers newest first, stagger, first success wins, blame
    async def run_job(self, info_hash: bytes, peers: List[Address], slots: asyncio.Semaphore) -> None:
        assert len(info_hash) == NODE_ID_LENGTH and peers
        loop = asyncio.get_running_loop()
        waiting = list(reversed(peers))
        running: Dict["asyncio.Task[AttemptOutcome]", Address] = {}
        blamed: List[Address] = []
        reason = "stopped"
        metadata: Optional[TorrentMetadata] = None
        try:
            while metadata is None and (waiting or running):
                timeout = None
                if waiting and len(running) < self.peer_parallelism:
                    peer = waiting.pop(0)
                    if self.is_known_unreachable(peer, loop.time()):
                        blamed.append(peer)
                        reason = self.count_reason("skipped_unreachable")
                        continue
                    running[loop.create_task(self.attempt(info_hash, peer, slots))] = peer
                    if waiting and len(running) < self.peer_parallelism:
                        timeout = self.stagger_seconds
                done, _ = await asyncio.wait(set(running), timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    peer = running.pop(task)
                    outcome, attempt_reason = task.result()
                    if outcome is not None:
                        metadata = metadata or outcome
                        continue
                    reason = attempt_reason
                    if attempt_reason not in UNBLAMED_REASONS:
                        blamed.append(peer)
                    if attempt_reason == "local_error":
                        self.pause_until = loop.time() + self.local_error_pause
        except asyncio.CancelledError:
            for task in running:
                task.cancel()
            self.catalog.release_fetch_claim(info_hash)
            raise
        for task in running:
            task.cancel()
        self.finish_job(info_hash, metadata, blamed, reason)
        assert not running or metadata is not None

    # Parents: run_job
    # Keywords: store, mark failed, release claim, counters
    def finish_job(self, info_hash: bytes, metadata: Optional[TorrentMetadata], blamed: List[Address], reason: str) -> None:
        assert len(info_hash) == NODE_ID_LENGTH
        if metadata is not None:
            self.catalog.store_metadata(metadata)
            with self._lock:
                self.successes += 1
            peer = metadata.source_peer or ("?", 0)
            LOGGER.info("metadata stored: %s name=%r size=%d files=%d from %s:%d", info_hash.hex(), metadata.name, metadata.total_size, metadata.file_count, peer[0], peer[1])
        elif blamed:
            state = self.catalog.mark_fetch_failed(info_hash, blamed, reason)
            with self._lock:
                self.failures += 1
            LOGGER.debug("fetch failed for %s (%s), state now %s", info_hash.hex(), reason, state)
        else:
            self.catalog.release_fetch_claim(info_hash)
        assert self.successes >= 0 and self.failures >= 0

    # Parents: run_job
    # Keywords: attempt, connection slot, tcp then utp pool, verify
    async def attempt(self, info_hash: bytes, peer: Address, slots: asyncio.Semaphore) -> AttemptOutcome:
        assert len(peer) == 2
        self.waiting += 1
        try:
            await slots.acquire()
        finally:
            self.waiting -= 1
        self.active += 1
        with self._lock:
            self.attempts += 1
            self.max_active = max(self.max_active, self.active)
        try:
            result = await self.verified(info_hash, peer, self.fetch_with_retry(info_hash, peer), "tcp")
        finally:
            self.active -= 1
            slots.release()
        utp_free = self.utp_slots is not None and not self.utp_slots.locked() and ":" not in peer[0]
        if result[0] is None and result[1] in self.utp_after_reasons and self.utp_function is not None and utp_free:
            async with self.utp_slots:
                with self._lock:
                    self.utp_attempts += 1
                utp_result = await self.verified(info_hash, peer, self.fetch_over_utp(info_hash, peer), "utp")
            if utp_result[0] is not None:
                self.unreachable.pop(peer, None)
            if utp_result[0] is not None or utp_result[1] != "utp_connect_timeout":
                result = utp_result
        assert result[0] is not None or result[1] != "ok"
        return result

    # Parents: attempt
    # Keywords: verify sha1, summarize, reason, route counters, reachability
    async def verified(self, info_hash: bytes, peer: Address, fetch: "Awaitable[Tuple[bytes, str]]", transport: str) -> AttemptOutcome:
        assert len(info_hash) == NODE_ID_LENGTH and transport in ("tcp", "utp")
        try:
            raw, route = await fetch
            if hashlib.sha1(raw).digest() != info_hash:
                raise MetadataFetchError("sha1_mismatch")
            result: AttemptOutcome = (summarize_info_dict(info_hash, decode_bencode(raw), time.time(), peer), "ok")
            with self._lock:
                self.encrypted_successes += 1 if route == "encrypted" else 0
                self.utp_successes += 1 if route == "utp" else 0
        except MetadataFetchError as error:
            if transport == "tcp":
                self.remember_unreachable(peer, error.reason)
            result = (None, self.count_reason(error.reason if transport == "tcp" else "utp_" + error.reason))
        except (ValueError, RecursionError):
            result = (None, self.count_reason("bad_info_dict"))
        except asyncio.CancelledError:
            raise
        except Exception:
            LOGGER.exception("unexpected error while fetching %s from %s", info_hash.hex(), peer)
            result = (None, self.count_reason("unexpected"))
        assert result[0] is not None or result[1] != "ok"
        return result

    # Parents: attempt
    # Keywords: utp, fetch, route tag
    async def fetch_over_utp(self, info_hash: bytes, peer: Address) -> Tuple[bytes, str]:
        assert self.utp_function is not None and self.utp_socket is not None
        raw = await self.utp_function(info_hash, peer, self.utp_socket)
        assert isinstance(raw, bytes)
        return raw, "utp"

    # Parents: attempt
    # Keywords: tcp first, encrypted retry, closed on handshake, same slot
    async def fetch_with_retry(self, info_hash: bytes, peer: Address) -> Tuple[bytes, str]:
        assert len(peer) == 2
        try:
            return await self.fetch_function(info_hash, peer), "tcp"
        except MetadataFetchError as error:
            if error.reason != "closed_on_handshake" or self.retry_function is None:
                raise
        with self._lock:
            self.encrypted_attempts += 1
        result = await self.retry_function(info_hash, peer)
        assert isinstance(result, bytes)
        return result, "encrypted"

    # Parents: attempt
    # Keywords: reachability cache, timeout by address, refusal by address and port
    def remember_unreachable(self, peer: Address, reason: str) -> None:
        assert len(peer) == 2
        expiry = asyncio.get_running_loop().time() + UNREACHABLE_SECONDS
        if reason == "connect_timeout":
            remember(self.unreachable, peer[0], expiry, UNREACHABLE_ENTRIES)
        elif reason == "connect":
            remember(self.unreachable, peer, expiry, UNREACHABLE_ENTRIES)
        assert len(self.unreachable) <= UNREACHABLE_ENTRIES

    # Parents: run_job
    # Keywords: reachability cache, skip, expiry
    def is_known_unreachable(self, peer: Address, now: float) -> bool:
        assert len(peer) == 2
        result = self.unreachable.get(peer[0], 0.0) > now or self.unreachable.get(peer, 0.0) > now
        assert isinstance(result, bool)
        return result

    # Parents: attempt, run_job
    # Keywords: failure reason, counter
    def count_reason(self, reason: str) -> str:
        assert isinstance(reason, str) and reason
        with self._lock:
            self.reasons[reason] = self.reasons.get(reason, 0) + 1
        return reason

    # Parents: ScraperRuntime.snapshot_stats, tests
    # Keywords: statistics, counters, reasons, connections
    def snapshot_counts(self) -> Dict[str, int]:
        assert self.max_connections >= 1
        with self._lock:
            result = {
                "fetch_attempts": self.attempts,
                "fetch_successes": self.successes,
                "fetch_failures": self.failures,
                "fetch_encrypted_attempts": self.encrypted_attempts,
                "fetch_encrypted_successes": self.encrypted_successes,
                "fetch_utp_attempts": self.utp_attempts,
                "fetch_utp_successes": self.utp_successes,
                "utp_packets_sent": self.utp_socket.packets_sent if self.utp_socket is not None else 0,
                "fetch_workers": self.max_connections,
                "fetch_connections": self.active,
                "fetch_connections_max": self.max_active,
                "fetch_queue_size": self.waiting,
            }
            for reason, count in self.reasons.items():
                result["fetch_fail_" + reason] = count
        assert result["fetch_connections"] <= result["fetch_workers"]
        return result

    # Parents: ScraperRuntime.stop, tests
    # Keywords: join, shutdown, timeout
    def join(self, timeout: float) -> int:
        assert timeout >= 0
        if self.thread is not None:
            self.thread.join(timeout)
        alive = 1 if self.thread is not None and self.thread.is_alive() else 0
        if alive:
            LOGGER.warning("fetch engine still running after %.1fs", timeout)
        assert alive in (0, 1)
        return alive
