"""Fetch category: the asyncio fetch engine with fake fetch functions (F-004, FETCH-001 to FETCH-005, FETCH-010 to FETCH-012)."""
import asyncio
import resource
import threading
import time
import unittest
from unittest import mock

from dht_scraper.fetch_engine import FetchEngine, raise_open_file_limit
from dht_scraper.metadata_fetcher import MetadataFetchError
from dht_scraper.torrent_catalog import FETCH_DONE, FETCH_PENDING, SOURCE_SAMPLE, TorrentCatalog
from tests.fetch.fake_metadata_peer import build_info_dict
from tests.shared_fixtures import wait_until

PEER_1 = ("1.1.1.1", 1)
PEER_2 = ("2.2.2.2", 2)
PEER_3 = ("3.3.3.3", 3)


class ScriptedFetch:
    """Async fetch function whose outcome per peer is scripted; runs on the engine's loop."""

    def __init__(self, outcomes=None, raw=b""):
        self.outcomes = outcomes or {}
        self.raw = raw
        self.lock = threading.Lock()
        self.calls = []
        self.running = set()
        self.max_running = 0
        self.cancelled = []
        self.released = set()

    def release(self, peer):
        with self.lock:
            self.released.add(peer)

    async def __call__(self, info_hash, peer):
        with self.lock:
            self.calls.append(peer)
            self.running.add(peer)
            self.max_running = max(self.max_running, len(self.running))
        try:
            kind, value = self.outcomes.get(peer, ("hang", None))
            if kind == "fail":
                raise MetadataFetchError(value)
            if kind == "ok":
                await asyncio.sleep(value)
                return self.raw
            while kind == "wait" and peer not in self.released:
                await asyncio.sleep(0.01)
            if kind == "wait":
                raise MetadataFetchError(value)
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            with self.lock:
                self.cancelled.append(peer)
            raise
        finally:
            with self.lock:
                self.running.discard(peer)


class FetchEngineTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()
        self.stop_event = threading.Event()
        self.engine = None

    def tearDown(self):
        self.stop_event.set()
        if self.engine is not None:
            self.assertEqual(self.engine.join(3.0), 0)

    def start(self, fetch, **options):
        self.engine = FetchEngine(self.catalog, self.stop_event, fetch, **options)
        self.engine.start()
        return self.engine

    def test_engine_stores_metadata_f004_t2(self):
        raw, info_hash = build_info_dict(name=b"engine test")
        self.catalog.record_hash(info_hash, SOURCE_SAMPLE, peer=PEER_1)
        self.start(ScriptedFetch({PEER_1: ("ok", 0.0)}, raw), max_connections=4)
        self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1))
        detail = self.catalog.torrent_detail(info_hash)
        self.assertEqual((detail["fetch_state"], detail["metadata"]["name"], detail["metadata"]["source_peer"]), (FETCH_DONE, "engine test", {"ip": "1.1.1.1", "port": 1}))
        self.assertEqual(self.engine.snapshot_counts()["fetch_successes"], 1)

    def test_FETCH_001_connection_limit_is_respected_and_filled(self):
        peers = [("10.1.1.%d" % index, 1000 + index) for index in range(10)]
        for index, peer in enumerate(peers):
            self.catalog.record_hash(bytes([0x40 + index]) * 20, SOURCE_SAMPLE, peer=peer)
        fetch = ScriptedFetch({peer: ("wait", "closed") for peer in peers})
        self.start(fetch, max_connections=3)
        self.assertTrue(wait_until(lambda: len(fetch.running) == 3))
        self.assertEqual(fetch.max_running, 3)
        self.assertEqual(self.catalog.snapshot_counts()["fetch_in_progress"], 3)
        fetch.release(fetch.calls[0])
        self.assertTrue(wait_until(lambda: len(fetch.calls) == 4 and len(fetch.running) == 3))
        self.assertEqual(fetch.max_running, 3)

    def test_FETCH_002_first_verified_success_cancels_the_others(self):
        raw, info_hash = build_info_dict(name=b"parallel")
        self.catalog.record_hash(info_hash, SOURCE_SAMPLE, peer=PEER_1)
        self.catalog.add_peers(info_hash, [PEER_2, PEER_3])
        fetch = ScriptedFetch({PEER_2: ("ok", 0.3)}, raw)
        self.start(fetch, max_connections=8, stagger_seconds=0.05)
        self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1))
        self.assertTrue(wait_until(lambda: len(fetch.cancelled) == 2))
        self.assertEqual(fetch.calls, [PEER_3, PEER_2, PEER_1])
        self.assertEqual(self.catalog.torrent_detail(info_hash)["metadata"]["source_peer"], {"ip": "2.2.2.2", "port": 2})
        self.assertEqual(sorted(fetch.cancelled), [PEER_1, PEER_3])
        self.assertEqual(self.engine.snapshot_counts()["fetch_successes"], 1)

    def test_FETCH_003_failed_job_reports_blamed_peers(self):
        with self.subTest(case="peer failures"):
            catalog = TorrentCatalog()
            stop_event = threading.Event()
            info_hash = b"\x51" * 20
            catalog.record_hash(info_hash, SOURCE_SAMPLE, peer=PEER_1)
            catalog.add_peers(info_hash, [PEER_2])
            engine = FetchEngine(catalog, stop_event, ScriptedFetch({PEER_2: ("fail", "connect_timeout"), PEER_1: ("fail", "closed")}), max_connections=4)
            engine.start()
            try:
                self.assertTrue(wait_until(lambda: engine.snapshot_counts()["fetch_failures"] == 1))
                detail = catalog.torrent_detail(info_hash)
                self.assertEqual((detail["fetch_state"], detail["peers"], detail["last_error"]), (FETCH_PENDING, [], "closed"))
            finally:
                stop_event.set()
                self.assertEqual(engine.join(3.0), 0)
        with self.subTest(case="local error blames nobody"):
            catalog = TorrentCatalog()
            stop_event = threading.Event()
            info_hash = b"\x52" * 20
            catalog.record_hash(info_hash, SOURCE_SAMPLE, peer=PEER_1)
            fetch = ScriptedFetch({PEER_1: ("fail", "local_error")})
            engine = FetchEngine(catalog, stop_event, fetch, max_connections=4, local_error_pause=5.0)
            engine.start()
            try:
                self.assertTrue(wait_until(lambda: len(fetch.calls) == 1 and catalog.snapshot_counts()["fetch_in_progress"] == 0))
                detail = catalog.torrent_detail(info_hash)
                self.assertEqual((detail["fetch_state"], detail["peers"]), (FETCH_PENDING, [{"ip": "1.1.1.1", "port": 1}]))
                self.assertEqual(engine.snapshot_counts()["fetch_failures"], 0)
            finally:
                stop_event.set()
                self.assertEqual(engine.join(3.0), 0)

    def test_FETCH_004_stop_cancels_and_releases_claims(self):
        peers = [PEER_1, PEER_2, PEER_3]
        for index, peer in enumerate(peers):
            self.catalog.record_hash(bytes([0x60 + index]) * 20, SOURCE_SAMPLE, peer=peer)
        fetch = ScriptedFetch()
        engine = self.start(fetch, max_connections=8)
        self.assertTrue(wait_until(lambda: len(fetch.running) == 3))
        started = time.monotonic()
        self.stop_event.set()
        self.assertEqual(engine.join(2.0), 0)
        self.assertLess(time.monotonic() - started, 2.0)
        for index, peer in enumerate(peers):
            detail = self.catalog.torrent_detail(bytes([0x60 + index]) * 20)
            self.assertEqual((detail["fetch_state"], detail["peers"]), (FETCH_PENDING, [{"ip": peer[0], "port": peer[1]}]))
        self.assertEqual(engine.snapshot_counts()["fetch_failures"], 0)

    def test_FETCH_005_failure_reasons_are_counted(self):
        reasons = {PEER_1: ("fail", "connect_timeout"), PEER_2: ("fail", "connect"), PEER_3: ("fail", "closed_on_handshake")}
        for index, peer in enumerate(reasons):
            self.catalog.record_hash(bytes([0x70 + index]) * 20, SOURCE_SAMPLE, peer=peer)
        engine = self.start(ScriptedFetch(reasons), max_connections=8)
        self.assertTrue(wait_until(lambda: engine.snapshot_counts()["fetch_failures"] == 3))
        counts = engine.snapshot_counts()
        self.assertEqual((counts["fetch_attempts"], counts["fetch_fail_connect_timeout"], counts["fetch_fail_connect"], counts["fetch_fail_closed_on_handshake"]), (3, 1, 1, 1))

    def test_FETCH_011_known_unreachable_peers_are_skipped(self):
        raw, hash_b = build_info_dict(name=b"reachable")
        timed_out, refused, alternative, same_ip_other_port = ("5.5.5.5", 1), ("6.6.6.6", 1), ("6.6.6.6", 2), ("5.5.5.5", 2)
        fetch = ScriptedFetch({timed_out: ("fail", "connect_timeout"), refused: ("fail", "connect"), alternative: ("ok", 0.0)}, raw)
        self.catalog.record_hash(b"\x81" * 20, SOURCE_SAMPLE, peer=timed_out)
        self.catalog.record_hash(b"\x82" * 20, SOURCE_SAMPLE, peer=refused)
        engine = self.start(fetch, max_connections=8)
        self.assertTrue(wait_until(lambda: engine.snapshot_counts()["fetch_failures"] == 2))
        calls_before = len(fetch.calls)
        self.catalog.record_hash(hash_b, SOURCE_SAMPLE, peer=alternative)
        self.catalog.add_peers(hash_b, [refused, same_ip_other_port])
        self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1))
        self.assertEqual(fetch.calls[calls_before:], [alternative])
        self.assertEqual(self.catalog.torrent_detail(hash_b)["fetch_state"], FETCH_DONE)
        self.assertEqual(engine.snapshot_counts()["fetch_fail_skipped_unreachable"], 2)

    def test_MSE_003_close_on_handshake_is_retried_encrypted(self):
        raw, hash_h = build_info_dict(name=b"retried")
        plain = ScriptedFetch({PEER_1: ("fail", "closed_on_handshake"), PEER_2: ("fail", "connect")})
        encrypted = ScriptedFetch({PEER_1: ("ok", 0.0)}, raw)
        self.catalog.record_hash(hash_h, SOURCE_SAMPLE, peer=PEER_1)
        self.catalog.record_hash(b"\x91" * 20, SOURCE_SAMPLE, peer=PEER_2)
        engine = self.start(plain, max_connections=8, retry_function=encrypted)
        self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1 and engine.snapshot_counts()["fetch_failures"] == 1))
        self.assertEqual(encrypted.calls, [PEER_1])
        counts = engine.snapshot_counts()
        self.assertEqual((counts["fetch_encrypted_attempts"], counts["fetch_encrypted_successes"]), (1, 1))

    def test_UTP_005_unreachable_tcp_peer_is_tried_over_utp(self):
        raw, hash_h = build_info_dict(name=b"utp only")
        tcp = ScriptedFetch({PEER_1: ("fail", "connect"), PEER_2: ("fail", "closed")})
        utp = ScriptedFetch({PEER_1: ("ok", 0.0)}, raw)

        async def over_utp(info_hash, peer, utp_socket):
            self.assertIsNotNone(utp_socket)
            return await utp(info_hash, peer)

        self.catalog.record_hash(hash_h, SOURCE_SAMPLE, peer=PEER_1)
        self.catalog.record_hash(b"\x92" * 20, SOURCE_SAMPLE, peer=PEER_2)
        engine = self.start(tcp, max_connections=8, utp_function=over_utp)
        self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1 and engine.snapshot_counts()["fetch_failures"] == 1))
        self.assertEqual(utp.calls, [PEER_1])
        counts = engine.snapshot_counts()
        self.assertEqual((counts["fetch_utp_attempts"], counts["fetch_utp_successes"]), (1, 1))

    def test_UTP_007_utp_peer_is_reused_by_later_jobs(self):
        raw_h, hash_h = build_info_dict(name=b"utp first")
        raw_k, hash_k = build_info_dict(name=b"utp second")
        raws = {hash_h: raw_h, hash_k: raw_k}
        utp_calls = []

        async def over_utp(info_hash, peer, utp_socket):
            utp_calls.append((info_hash, peer))
            return raws[info_hash]

        self.catalog.record_hash(hash_h, SOURCE_SAMPLE, peer=PEER_1)
        engine = self.start(ScriptedFetch({PEER_1: ("fail", "connect")}), max_connections=8, utp_function=over_utp)
        self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1))
        self.catalog.record_hash(hash_k, SOURCE_SAMPLE, peer=PEER_1)
        self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 2))
        self.assertEqual(self.catalog.torrent_detail(hash_k)["fetch_state"], FETCH_DONE)
        self.assertEqual(utp_calls, [(hash_h, PEER_1), (hash_k, PEER_1)])
        self.assertEqual(engine.snapshot_counts().get("fetch_fail_skipped_unreachable", 0), 0)

    def test_FETCH_012_engine_runs_without_utp_socket(self):
        raw, hash_h = build_info_dict(name=b"tcp without utp")
        utp_calls = []

        async def over_utp(info_hash, peer, utp_socket):
            utp_calls.append(peer)
            return raw

        async def no_socket():
            raise OSError(24, "Too many open files")

        self.catalog.record_hash(hash_h, SOURCE_SAMPLE, peer=PEER_1)
        with mock.patch("dht_scraper.fetch_engine.open_utp_socket", no_socket), self.assertLogs("dht_scraper", level="WARNING") as logs:
            engine = self.start(ScriptedFetch({PEER_1: ("ok", 0.0)}, raw), max_connections=8, utp_function=over_utp)
            self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1))
        self.assertTrue(any("uTP" in line for line in logs.output))
        self.assertEqual(self.catalog.torrent_detail(hash_h)["fetch_state"], FETCH_DONE)
        self.assertEqual((utp_calls, engine.snapshot_counts()["fetch_utp_attempts"]), ([], 0))

    def test_FETCH_010_open_file_limit_is_raised(self):
        infinite = resource.RLIM_INFINITY
        with self.subTest(case="raised"):
            applied = []
            self.assertEqual(raise_open_file_limit(512, lambda: (256, infinite), applied.append), 512)
            self.assertEqual(applied, [(512, infinite)])
        with self.subTest(case="already enough"):
            applied = []
            self.assertEqual(raise_open_file_limit(512, lambda: (4096, 8192), applied.append), 4096)
            self.assertEqual(applied, [])
        with self.subTest(case="refused"):
            def refuse(limits):
                raise ValueError("not allowed")

            with self.assertLogs("dht_scraper", level="WARNING"):
                self.assertEqual(raise_open_file_limit(512, lambda: (256, infinite), refuse), 256)


if __name__ == "__main__":
    unittest.main()
