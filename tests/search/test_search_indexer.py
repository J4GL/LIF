"""Search category: the indexer thread between the catalog and the SQLite database (SEARCH-002 to SEARCH-006)."""
import sqlite3
import time
import unittest

from dht_scraper.search_indexer import SearchIndexer
from dht_scraper.torrent_catalog import SOURCE_SAMPLE, TorrentCatalog
from dht_scraper.torrent_info_summary import TorrentFile, TorrentMetadata
from tests.search.database_fixtures import FailingDatabase, document, info_hash_hex, open_database
from tests.shared_fixtures import wait_until

STAT_KEYS = ["index_backlog", "index_documents", "index_dropped", "index_errors", "index_updates", "index_upserts"]
FAST = {"document_seconds": 0.05, "counter_seconds": 0.05, "count_seconds": 0.05}


def metadata(number, name, files=None):
    listed = [TorrentFile(path, 5) for path in (files or [name])]
    return TorrentMetadata(bytes.fromhex(info_hash_hex(number)), name, 5 * len(listed), 16384, len(listed), listed, False, 9.0, ("1.2.3.4", 1))


class IndexerTestCase(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()
        self.catalog.enable_index_tracking()

    def make_indexer(self, database, **options):
        indexer = SearchIndexer(self.catalog, database, **options)

        def stop():
            indexer.stop()
            indexer.join(5.0)

        self.addCleanup(stop)
        return indexer


class SearchIndexerSpecTest(IndexerTestCase):
    def test_SEARCH_002_full_document_write(self):
        with self.subTest(case="cumulative"):
            database = open_database(self, [document(1, "old A", seen_count=10, announce_count=2, first_seen=5.0)])
            self.catalog.store_metadata(metadata(1, "A", ["d/a.txt", "d/b.txt"]), now=100.0)
            self.catalog.store_metadata(metadata(2, "B"), now=100.0)
            for number in (1, 2):
                for moment in (101.0, 102.0, 103.0):
                    self.catalog.record_hash(bytes.fromhex(info_hash_hex(number)), SOURCE_SAMPLE, now=moment)
            indexer = self.make_indexer(database)
            indexer.flush_documents()
            row_a, row_b = database.get_document(info_hash_hex(1)), database.get_document(info_hash_hex(2))
            self.assertEqual((row_a["seen_count"], row_a["announce_count"], row_a["first_seen"]), (13, 2, 5.0))
            self.assertEqual(row_a["name"], "old A")
            self.assertEqual((row_b["seen_count"], row_b["first_seen"]), (3, 100.0))
            self.assertEqual(indexer.snapshot_counts()["index_upserts"], 2)
            self.assertEqual(self.catalog.torrent_detail(bytes.fromhex(info_hash_hex(1)))["seen_count"], 13)
        with self.subTest(case="30 files"):
            database = open_database(self)
            paths = ["f%02d.bin" % index for index in range(1, 31)]
            paths[19], paths[24] = "giraffe.bin", "zebra.bin"
            self.catalog.store_metadata(metadata(3, "Thirty", paths), now=100.0)
            self.make_indexer(database).flush_documents()
            self.assertEqual(len(database.get_document(info_hash_hex(3))["files"]), 30)
            self.assertEqual([record["name"] for record in database.search("giraffe", 10)], ["Thirty"])
            self.assertEqual(database.search("zebra", 10), [])

    def test_SEARCH_003_counter_updates(self):
        database = open_database(self)
        hash_a = bytes.fromhex(info_hash_hex(1))
        self.catalog.store_metadata(metadata(1, "A"), now=100.0)
        self.catalog.store_metadata(metadata(2, "B"), now=100.0)
        indexer = self.make_indexer(database, document_seconds=0.05, counter_seconds=0.2, count_seconds=0.2)
        indexer.start()
        self.assertTrue(wait_until(lambda: database.count_documents() == 2, timeout=2.0))
        row_b = database.get_document(info_hash_hex(2))
        for moment in (110.0, 111.0, 112.0):
            self.catalog.record_hash(hash_a, SOURCE_SAMPLE, now=moment)
        self.assertTrue(wait_until(lambda: database.get_document(info_hash_hex(1))["seen_count"] == 3, timeout=2.0))
        self.assertEqual(database.get_document(info_hash_hex(1))["name"], "A")
        self.assertEqual(database.get_document(info_hash_hex(2)), row_b)
        self.assertEqual(indexer.snapshot_counts()["index_updates"], 1)
        connection = sqlite3.connect(database.path)
        connection.execute("DELETE FROM torrents WHERE info_hash = ?", (info_hash_hex(1),))
        connection.commit()
        connection.close()
        self.catalog.record_hash(hash_a, SOURCE_SAMPLE, now=113.0)
        self.assertTrue(wait_until(lambda: (database.get_document(info_hash_hex(1)) or {}).get("seen_count") == 4, timeout=2.0))

    def test_SEARCH_004_failures_retry_with_backoff(self):
        database = FailingDatabase(open_database(self), failing=True)
        indexer = self.make_indexer(database, backoff_start=0.1, backoff_max=0.4, **FAST)
        with self.assertLogs("dht_scraper", level="ERROR") as logs:
            indexer.start()
            self.catalog.store_metadata(metadata(1, "A"))
            started = time.monotonic()
            time.sleep(1.5)
            times = [moment for moment in database.call_times() if moment >= started - 0.5]
        counts = indexer.snapshot_counts()
        self.assertGreaterEqual(counts["index_errors"], 3)
        gaps = [later - earlier for earlier, later in zip(times, times[1:])]
        self.assertTrue(all(later >= earlier - 0.02 for earlier, later in zip(gaps, gaps[1:])), gaps)
        self.assertTrue(all(gap <= 0.6 for gap in gaps), gaps)
        self.assertEqual(counts["index_backlog"], 1)
        self.assertTrue(any("disk I/O error" in line for line in logs.output), logs.output)
        for number in range(2, 52):
            before = time.monotonic()
            self.catalog.store_metadata(metadata(number, "T%d" % number))
            self.assertLess(time.monotonic() - before, 0.1)
        database.failing = False
        self.assertTrue(wait_until(lambda: database.database.count_documents() == 51, timeout=2.0))
        self.assertTrue(wait_until(lambda: indexer.snapshot_counts()["index_backlog"] == 0, timeout=2.0))

    def test_SEARCH_005_final_flush_on_stop(self):
        with self.subTest(case="flush"):
            database = open_database(self)
            indexer = self.make_indexer(database, document_seconds=60.0, counter_seconds=60.0, count_seconds=60.0)
            indexer.start()
            time.sleep(0.1)
            self.catalog.store_metadata(metadata(1, "A"))
            started = time.monotonic()
            indexer.stop()
            self.assertTrue(indexer.join(5.0))
            self.assertLess(time.monotonic() - started, 1.0)
            self.assertIsNotNone(database.get_document(info_hash_hex(1)))
        with self.subTest(case="failing"):
            database = FailingDatabase(open_database(self))
            indexer = self.make_indexer(database, document_seconds=60.0, counter_seconds=60.0, count_seconds=60.0)
            indexer.start()
            time.sleep(0.1)
            database.failing = True
            self.catalog.store_metadata(metadata(2, "B"))
            started = time.monotonic()
            with self.assertLogs("dht_scraper", level="ERROR"):
                indexer.stop()
                self.assertTrue(indexer.join(5.0))
            self.assertLess(time.monotonic() - started, 4.0)

    def test_SEARCH_006_statistics(self):
        database = open_database(self, [document(number, "n%d" % number) for number in range(1, 8)])
        indexer = self.make_indexer(database, document_seconds=0.2, counter_seconds=0.2, count_seconds=0.2)
        indexer.start()
        counts = indexer.snapshot_counts()
        self.assertEqual(sorted(counts), STAT_KEYS)
        self.assertEqual(counts["index_documents"], 7)
        writer = open_database(self, path=database.path)
        writer.write_documents([document(number, "n%d" % number) for number in range(8, 11)])
        self.assertTrue(wait_until(lambda: indexer.snapshot_counts()["index_documents"] == 10, timeout=1.0))


if __name__ == "__main__":
    unittest.main()
