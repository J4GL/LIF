"""Catalog category: in-memory catalog, ranking, fetch and lookup state (F-003)."""
import threading
import unittest

from dht_scraper.torrent_catalog import (
    FETCH_DONE,
    FETCH_FAILED,
    FETCH_IN_PROGRESS,
    FETCH_PENDING,
    SOURCE_ANNOUNCE_PEER,
    SOURCE_GET_PEERS,
    SOURCE_SAMPLE,
    TorrentCatalog,
)
from dht_scraper.torrent_info_summary import TorrentFile, TorrentMetadata

HASH_A = b"\x0a" * 20
HASH_B = b"\x0b" * 20
HASH_C = b"\x0c" * 20
PEER_1 = ("1.1.1.1", 1)
PEER_2 = ("2.2.2.2", 2)


def make_metadata(info_hash, name="Ubuntu ISO", files=None):
    files = files or [TorrentFile(name, 100)]
    return TorrentMetadata(info_hash, name, sum(f.length for f in files), 16384, len(files), files, False, 50.0, PEER_1)


class CatalogCountersTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()

    def test_record_hash_creates_entry_and_counts_f003_t1(self):
        self.assertTrue(self.catalog.record_hash(HASH_A, SOURCE_GET_PEERS, now=100.0))
        self.assertFalse(self.catalog.record_hash(HASH_A, SOURCE_ANNOUNCE_PEER, peer=PEER_1, now=200.0))
        self.assertFalse(self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, now=150.0))
        detail = self.catalog.torrent_detail(HASH_A)
        self.assertEqual((detail["seen_count"], detail["announce_count"], detail["first_seen"], detail["last_seen"]), (3, 1, 100.0, 200.0))
        self.assertEqual(detail["peers"], [{"ip": "1.1.1.1", "port": 1}])
        self.assertEqual(detail["fetch_state"], FETCH_PENDING)
        counts = self.catalog.snapshot_counts()
        self.assertEqual((counts["hashes_seen"], counts["observations"], counts["fetch_pending"]), (1, 3, 1))

    def test_record_rejects_bad_input(self):
        with self.assertRaises(AssertionError):
            self.catalog.record_hash(b"short", SOURCE_GET_PEERS)
        with self.assertRaises(AssertionError):
            self.catalog.record_hash(HASH_A, "other")

    def test_add_peers_deduplicates_and_bounds(self):
        catalog = TorrentCatalog(max_peers_per_hash=2)
        catalog.record_hash(HASH_A, SOURCE_SAMPLE)
        self.assertEqual(catalog.add_peers(HASH_A, [PEER_1, PEER_1, PEER_2]), 2)
        self.assertEqual(catalog.add_peers(HASH_A, [("3.3.3.3", 3)]), 1)
        self.assertEqual(catalog.torrent_detail(HASH_A)["peers"], [{"ip": "2.2.2.2", "port": 2}, {"ip": "3.3.3.3", "port": 3}])
        self.assertEqual(catalog.add_peers(HASH_B, [PEER_1]), 0)


class CatalogFetchStateTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog(max_fetch_attempts=2, retry_seconds=10.0, max_lookups=2, lookup_retry_seconds=5.0)

    def test_candidates_ordered_by_seen_count_and_reranked_live(self):
        self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, peer=PEER_1, now=1.0)
        for _ in range(3):
            self.catalog.record_hash(HASH_B, SOURCE_SAMPLE, peer=PEER_2, now=1.0)
        self.catalog.record_hash(HASH_C, SOURCE_SAMPLE, now=1.0)
        self.assertEqual(self.catalog.next_fetch_candidates(1, now=2.0), [(HASH_B, [PEER_2])])
        for _ in range(5):
            self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, now=1.0)
        self.assertEqual(self.catalog.next_fetch_candidates(5, now=2.0), [(HASH_A, [PEER_1])])
        counts = self.catalog.snapshot_counts()
        self.assertEqual((counts["fetch_in_progress"], counts["fetch_pending"]), (2, 1))

    def test_store_metadata_marks_done_and_indexes_search(self):
        self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, peer=PEER_1)
        self.catalog.next_fetch_candidates(1)
        self.assertFalse(self.catalog.store_metadata(make_metadata(HASH_A)))
        self.assertTrue(self.catalog.store_metadata(make_metadata(HASH_B, "unknown before")))
        self.assertEqual(self.catalog.snapshot_counts()["with_metadata"], 2)
        self.assertEqual([row["name"] for row in self.catalog.search("ubuntu", 10)], ["Ubuntu ISO"])
        self.assertEqual(self.catalog.torrent_detail(HASH_A)["fetch_state"], FETCH_DONE)

    def test_mark_fetch_failed_retries_then_fails(self):
        self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, peer=PEER_1, now=0.0)
        self.catalog.add_peers(HASH_A, [PEER_2])
        self.catalog.next_fetch_candidates(1, now=1.0)
        self.assertEqual(self.catalog.mark_fetch_failed(HASH_A, [PEER_1], "closed", now=1.0), FETCH_PENDING)
        detail = self.catalog.torrent_detail(HASH_A)
        self.assertEqual((detail["fetch_attempts"], detail["retry_after"], detail["last_error"]), (1, 11.0, "closed"))
        self.assertEqual(detail["peers"], [{"ip": "2.2.2.2", "port": 2}])
        self.assertEqual(self.catalog.next_fetch_candidates(1, now=5.0), [])
        self.assertEqual(self.catalog.next_fetch_candidates(1, now=12.0), [(HASH_A, [PEER_2])])
        self.assertEqual(self.catalog.mark_fetch_failed(HASH_A, [PEER_2], "timeout", now=12.0), FETCH_FAILED)
        self.assertEqual(self.catalog.mark_fetch_failed(HASH_B, [], "x"), FETCH_FAILED)

    def test_release_fetch_claim(self):
        self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, peer=PEER_1)
        self.catalog.next_fetch_candidates(1)
        self.assertEqual(self.catalog.torrent_detail(HASH_A)["fetch_state"], FETCH_IN_PROGRESS)
        self.catalog.release_fetch_claim(HASH_A)
        self.assertEqual(self.catalog.torrent_detail(HASH_A)["fetch_state"], FETCH_PENDING)

    def test_lookup_lifecycle_with_backoff(self):
        self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, now=0.0)
        self.catalog.record_hash(HASH_B, SOURCE_SAMPLE, peer=PEER_1, now=0.0)
        self.assertEqual(self.catalog.hashes_needing_peers(5, now=0.0), [HASH_A])
        self.assertEqual(self.catalog.hashes_needing_peers(5, now=0.0), [])
        self.assertEqual(self.catalog.snapshot_counts()["lookups_in_progress"], 1)
        self.catalog.add_lookup_result(HASH_A, [], now=0.0)
        self.assertEqual(self.catalog.snapshot_counts()["lookups_in_progress"], 0)
        self.assertEqual(self.catalog.hashes_needing_peers(5, now=1.0), [])
        self.assertEqual(self.catalog.hashes_needing_peers(5, now=5.0), [HASH_A])
        self.catalog.add_lookup_result(HASH_A, [PEER_2], now=5.0)
        self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, now=5.0)
        self.assertEqual(self.catalog.next_fetch_candidates(1, now=6.0), [(HASH_A, [PEER_2])])

    def test_lookup_gives_up_after_max_attempts(self):
        self.catalog.record_hash(HASH_A, SOURCE_SAMPLE, now=0.0)
        self.catalog.hashes_needing_peers(1, now=0.0)
        self.catalog.add_lookup_result(HASH_A, [], now=0.0)
        self.catalog.hashes_needing_peers(1, now=100.0)
        self.catalog.add_lookup_result(HASH_A, [], now=100.0)
        detail = self.catalog.torrent_detail(HASH_A)
        self.assertEqual((detail["fetch_state"], detail["last_error"], detail["lookups_started"]), (FETCH_FAILED, "no_peers", 2))


class CatalogStrandedEntryTest(unittest.TestCase):
    def test_exhausted_lookups_and_dropped_peers_fail_instead_of_stranding(self):
        catalog = TorrentCatalog(max_lookups=1)
        catalog.record_hash(HASH_A, SOURCE_SAMPLE, now=0.0)
        catalog.hashes_needing_peers(1, now=0.0)
        catalog.add_lookup_result(HASH_A, [PEER_1], now=0.0)
        catalog.next_fetch_candidates(1, now=1.0)
        self.assertEqual(catalog.mark_fetch_failed(HASH_A, [PEER_1], "closed", now=1.0), FETCH_FAILED)
        self.assertEqual(catalog.torrent_detail(HASH_A)["last_error"], "no_peers")


class CatalogEvictionAndSearchTest(unittest.TestCase):
    def test_eviction_keeps_size_bounded_and_prefers_low_seen_without_metadata(self):
        catalog = TorrentCatalog(max_entries=40)
        catalog.store_metadata(make_metadata(HASH_A, "kept"))
        for _ in range(3):
            catalog.record_hash(HASH_B, SOURCE_SAMPLE, peer=PEER_1)
        catalog.next_fetch_candidates(1)
        for index in range(60):
            catalog.record_hash(bytes([index]) * 20, SOURCE_SAMPLE)
        self.assertLessEqual(catalog.snapshot_counts()["hashes_seen"], 40)
        self.assertIsNotNone(catalog.torrent_detail(HASH_A))
        self.assertIsNotNone(catalog.torrent_detail(HASH_B))
        counts = catalog.snapshot_counts()
        self.assertGreater(counts["evicted"], 0)
        self.assertEqual(counts["fetch_pending"] + counts["fetch_in_progress"] + counts["fetch_done"] + counts["fetch_failed"], counts["hashes_seen"])

    def test_search_is_case_insensitive_ranked_and_limited(self):
        catalog = TorrentCatalog()
        catalog.store_metadata(make_metadata(HASH_A, "Alpha Pack", [TorrentFile("Docs/README.md", 1)]))
        catalog.store_metadata(make_metadata(HASH_B, "beta", [TorrentFile("x", 1)]))
        for _ in range(3):
            catalog.record_hash(HASH_B, SOURCE_SAMPLE)
        self.assertEqual([row["info_hash"] for row in catalog.search("", 10)], [HASH_B.hex(), HASH_A.hex()])
        self.assertEqual([row["name"] for row in catalog.search("readme", 10)], ["Alpha Pack"])
        self.assertEqual(catalog.search("nothing", 10), [])
        self.assertEqual(len(catalog.search("", 1)), 1)
        self.assertIsNone(catalog.torrent_detail(HASH_C))
        row = catalog.search("alpha", 1)[0]
        self.assertEqual(set(row), {"info_hash", "name", "size", "file_count", "seen_count", "announce_count", "first_seen", "last_seen"})

    def test_concurrent_writers_keep_invariants(self):
        catalog = TorrentCatalog(max_entries=500)

        def writer(seed):
            for index in range(2000):
                catalog.record_hash(bytes([seed, index % 256]) + b"\x00" * 18, SOURCE_SAMPLE, peer=PEER_1)
                if index % 50 == 0:
                    catalog.next_fetch_candidates(3)
                    catalog.hashes_needing_peers(2)

        threads = [threading.Thread(target=writer, args=(seed,)) for seed in range(4)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        counts = catalog.snapshot_counts()
        self.assertEqual(counts["fetch_pending"] + counts["fetch_in_progress"] + counts["fetch_done"] + counts["fetch_failed"], counts["hashes_seen"])
        self.assertLessEqual(counts["hashes_seen"], 500 + counts["fetch_in_progress"] + counts["lookups_in_progress"])


if __name__ == "__main__":
    unittest.main()
