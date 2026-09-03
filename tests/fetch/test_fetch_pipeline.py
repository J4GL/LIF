"""Fetch category: scheduler and worker pool with a fake fetch function (F-003, F-004)."""
import queue
import threading
import unittest

from dht_scraper.fetch_scheduler import FetchScheduler
from dht_scraper.fetch_worker_pool import FetchWorkerPool
from dht_scraper.metadata_fetcher import MetadataFetchError
from dht_scraper.torrent_catalog import FETCH_DONE, FETCH_IN_PROGRESS, FETCH_PENDING, SOURCE_SAMPLE, TorrentCatalog
from tests.fetch.fake_metadata_peer import build_info_dict
from tests.shared_fixtures import wait_until

PEER_1 = ("1.1.1.1", 1)
PEER_2 = ("2.2.2.2", 2)


class FetchSchedulerTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()
        self.queue = queue.Queue(maxsize=2)
        self.stop_event = threading.Event()
        self.scheduler = FetchScheduler(self.catalog, self.queue, self.stop_event, idle_seconds=0.05, busy_seconds=0.01)
        for index in range(4):
            info_hash = bytes([index]) * 20
            for _ in range(index + 1):
                self.catalog.record_hash(info_hash, SOURCE_SAMPLE, peer=PEER_1)

    def test_schedule_round_fills_free_slots_only(self):
        self.assertEqual(self.scheduler.schedule_round(), 2)
        self.assertEqual(self.scheduler.schedule_round(), 0)
        first = self.queue.get_nowait()
        self.assertEqual(first[0], bytes([3]) * 20)
        self.assertEqual(self.catalog.snapshot_counts()["fetch_in_progress"], 2)

    def test_drain_releases_claims_and_run_stops(self):
        self.scheduler.schedule_round()
        self.assertEqual(self.scheduler.drain_queue(), 2)
        self.assertEqual(self.catalog.snapshot_counts()["fetch_in_progress"], 0)
        thread = threading.Thread(target=self.scheduler.run)
        thread.start()
        self.assertTrue(wait_until(lambda: self.queue.qsize() == 2))
        self.stop_event.set()
        thread.join(2.0)
        self.assertFalse(thread.is_alive())
        self.assertTrue(self.queue.empty())
        self.assertEqual(self.catalog.snapshot_counts()["fetch_in_progress"], 0)


class FetchWorkerPoolTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog(max_fetch_attempts=2)
        self.queue = queue.Queue(maxsize=4)
        self.stop_event = threading.Event()
        self.raw, self.info_hash = build_info_dict(name=b"pool test")

    def start_pool(self, fetch_function):
        self.pool = FetchWorkerPool(self.catalog, self.queue, self.stop_event, fetch_function, 2)
        self.pool.start()

    def tearDown(self):
        self.stop_event.set()
        if hasattr(self, "pool"):
            self.assertEqual(self.pool.join(3.0), 0)

    def test_worker_stores_metadata_f004_t2(self):
        self.catalog.record_hash(self.info_hash, SOURCE_SAMPLE, peer=PEER_1)
        self.start_pool(lambda info_hash, peer, stop_event: self.raw)
        self.queue.put(self.catalog.next_fetch_candidates(1)[0])
        self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1))
        detail = self.catalog.torrent_detail(self.info_hash)
        self.assertEqual((detail["fetch_state"], detail["metadata"]["name"], detail["metadata"]["source_peer"]), (FETCH_DONE, "pool test", {"ip": "1.1.1.1", "port": 1}))
        self.assertEqual(self.pool.snapshot_counts()["fetch_successes"], 1)

    def test_worker_tries_next_peer_then_marks_failed(self):
        calls = []

        def failing(info_hash, peer, stop_event):
            calls.append(peer)
            raise MetadataFetchError("closed")

        self.catalog.record_hash(self.info_hash, SOURCE_SAMPLE, peer=PEER_1)
        self.catalog.add_peers(self.info_hash, [PEER_2])
        self.start_pool(failing)
        self.queue.put(self.catalog.next_fetch_candidates(1)[0])
        self.assertTrue(wait_until(lambda: self.catalog.torrent_detail(self.info_hash)["fetch_state"] == FETCH_PENDING))
        detail = self.catalog.torrent_detail(self.info_hash)
        self.assertEqual((sorted(calls), detail["fetch_attempts"], detail["last_error"], detail["peers"]), ([PEER_1, PEER_2], 1, "closed", []))

    def test_untried_candidate_is_released_not_counted(self):
        self.catalog.record_hash(self.info_hash, SOURCE_SAMPLE, peer=PEER_1)
        candidate = self.catalog.next_fetch_candidates(1)[0]
        pool = FetchWorkerPool(self.catalog, self.queue, self.stop_event, lambda *_: self.raw, 1)
        self.stop_event.set()
        self.assertFalse(pool.process_candidate(candidate))
        self.assertEqual(pool.snapshot_counts()["fetch_failures"], 0)
        self.assertEqual(self.catalog.torrent_detail(self.info_hash)["fetch_state"], FETCH_PENDING)

    def test_worker_survives_bad_metadata_and_exceptions(self):
        self.catalog.record_hash(self.info_hash, SOURCE_SAMPLE, peer=PEER_1)
        self.catalog.add_peers(self.info_hash, [PEER_2])
        outcomes = iter([RuntimeError("boom"), b"not bencode"])

        def flaky(info_hash, peer, stop_event):
            outcome = next(outcomes)
            if isinstance(outcome, Exception):
                raise outcome
            return outcome

        self.start_pool(flaky)
        self.queue.put(self.catalog.next_fetch_candidates(1)[0])
        self.assertTrue(wait_until(lambda: self.pool.snapshot_counts()["fetch_failures"] == 1))
        self.assertEqual(self.pool.snapshot_counts()["fetch_workers_alive"], 2)
        self.assertNotEqual(self.catalog.torrent_detail(self.info_hash)["fetch_state"], FETCH_IN_PROGRESS)


if __name__ == "__main__":
    unittest.main()
