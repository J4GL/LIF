"""Search category: the SQLite database, its terms and its typo-tolerant queries (SEARCH-020)."""
import os
import sqlite3
import unittest

from dht_scraper.torrent_database import SchemaMismatch, TorrentDatabase, document_detail_record, text_terms
from tests.search.database_fixtures import document, info_hash_hex, open_database, temporary_directory, user_version

SEARCH_RECORD_KEYS = ["announce_count", "file_count", "first_seen", "info_hash", "last_seen", "name", "seen_count", "size"]
DETAIL_KEYS = ["announce_count", "fetch_attempts", "fetch_state", "first_seen", "info_hash", "last_error", "last_seen", "lookups_started", "metadata", "peers", "seen_count"]
METADATA_KEYS = ["fetched_at", "file_count", "files", "is_private", "name", "piece_length", "size", "source_peer"]


def names(database, query, limit=10):
    return [record["name"] for record in database.search(query, limit)]


class TextTermsTest(unittest.TestCase):
    def test_terms_follow_unicode61_with_diacritics_removed(self):
        self.assertEqual(text_terms("Élan_Vital-2.0"), ["elan", "vital", "2", "0"])
        self.assertEqual(text_terms("dir/b.txt"), ["dir", "b", "txt"])
        self.assertEqual(text_terms("  "), [])


class TorrentDatabaseSpecTest(unittest.TestCase):
    def test_SEARCH_020_database_scenarios(self):
        for scenario in (self.case_schema, self.case_tokens, self.case_typos, self.case_order, self.case_writes, self.case_lookup, self.case_detail):
            with self.subTest(case=scenario.__name__):
                scenario()

    def case_schema(self):
        directory = temporary_directory(self)
        path = os.path.join(directory, "new.sqlite3")
        database = TorrentDatabase(path)
        self.assertTrue(database.open_writer())
        database.write_documents([document(1, "kept")])
        database.close()
        connection = sqlite3.connect(path)
        tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
        connection.close()
        self.assertTrue({"torrents", "torrents_fts", "terms"} <= tables, tables)
        self.assertEqual(user_version(path), 1)
        again = TorrentDatabase(path)
        self.assertFalse(again.open_writer())
        self.assertEqual(again.count_documents(), 1)
        again.close()
        foreign = os.path.join(directory, "foreign.sqlite3")
        connection = sqlite3.connect(foreign)
        connection.execute("PRAGMA user_version = 7")
        connection.close()
        with self.assertRaises(SchemaMismatch):
            TorrentDatabase(foreign).open_writer()
        self.assertEqual(user_version(foreign), 7)

    def case_tokens(self):
        database = open_database(self, [document(1, "ubuntu-24.04-desktop-amd64.iso"), document(2, "Misc", files=["dir/a.txt", "dir/b.txt"]), document(3, "Debian"), document(4, "Élan Vital")])
        expected = {
            "ubuntu": ["ubuntu-24.04-desktop-amd64.iso"], "b.txt": ["Misc"], "ubun": ["ubuntu-24.04-desktop-amd64.iso"], "ubuntu zzz": [],
            "elan": ["Élan Vital"], "ÉLAN": ["Élan Vital"],
        }
        for query, wanted in expected.items():
            self.assertEqual(names(database, query), wanted, query)

    def case_typos(self):
        database = open_database(self, [document(1, "ubuntu desktop", 1), document(2, "ubunto mirror", 9), document(3, "Debian"), document(4, "Fedora 2024"), document(5, "Arch 2023")])
        expected = {
            "ubunut": ["ubuntu desktop"], "debain": ["Debian"], "deban": ["Debian"], "debiian": ["Debian"], "fedira": ["Fedora 2024"],
            "ubutnu desk": ["ubuntu desktop"], "dbn": [], "2025": [], "ubuntu zzzz": [],
        }
        for query, wanted in expected.items():
            self.assertEqual(names(database, query), wanted, query)
        self.assertEqual(names(database, "ubunto", 1), ["ubunto mirror"])
        self.assertEqual(names(database, "ubunto", 10), ["ubunto mirror", "ubuntu desktop"])

    def case_order(self):
        database = open_database(self, [
            document(1, "low", 5, 9.0), document(2, "high", 9, 1.0), document(3, "mid old", 7, 1.0), document(4, "mid new", 7, 3.0),
            document(5, "other", 99, 1.0, files=["docs/mid.txt"]),
        ])
        self.assertEqual(names(database, ""), ["other", "high", "mid new", "mid old", "low"])
        self.assertEqual(names(database, "", 2), ["other", "high"])
        self.assertEqual(names(database, "mid"), ["mid new", "mid old", "other"])
        self.assertTrue(all(sorted(record) == SEARCH_RECORD_KEYS for record in database.search("", 10)))

    def case_writes(self):
        database = open_database(self, [document(1, "kept name", 1)])
        database.write_documents([document(1, "changed", 5)])
        unknown = info_hash_hex(99)
        missing = database.update_counters([
            {"info_hash": info_hash_hex(1), "seen_count": 42, "announce_count": 3, "last_seen": 5.0},
            {"info_hash": unknown, "seen_count": 1, "announce_count": 0, "last_seen": 5.0},
        ])
        self.assertEqual(missing, [unknown])
        stored = database.get_document(info_hash_hex(1))
        self.assertEqual((stored["name"], stored["seen_count"], stored["announce_count"], stored["last_seen"]), ("kept name", 42, 3, 5.0))
        self.assertEqual(names(database, "changed"), [])

    def case_lookup(self):
        database = open_database(self, [document(number, "item %d" % number, seen_count=number, announce_count=number % 3, first_seen=float(number)) for number in range(1, 301)])
        wanted = [info_hash_hex(number) for number in range(1, 301)] + [info_hash_hex(1000), info_hash_hex(1001)]
        found = database.lookup_counters(wanted)
        self.assertEqual(len(found), 300)
        self.assertEqual(found[info_hash_hex(250)], (250, 1, 250.0))
        self.assertNotIn(info_hash_hex(1000), found)
        self.assertEqual(database.count_documents(), 300)

    def case_detail(self):
        stored = document(7, "detail", files=["a/1.bin", "a/2.bin"], lengths=[7, 9])
        database = open_database(self, [stored])
        found = database.get_document(stored["info_hash"])
        self.assertEqual(found, stored)
        self.assertIsNone(database.get_document(info_hash_hex(8)))
        record = document_detail_record(found)
        self.assertEqual(sorted(record), DETAIL_KEYS)
        self.assertEqual(sorted(record["metadata"]), METADATA_KEYS)
        self.assertEqual((record["fetch_state"], record["fetch_attempts"], record["last_error"], record["lookups_started"], record["peers"]), ("done", 0, "", 0, []))
        self.assertEqual(record["metadata"]["files"], [{"path": "a/1.bin", "length": 7}, {"path": "a/2.bin", "length": 9}])
        self.assertEqual((record["metadata"]["size"], record["metadata"]["source_peer"], record["seen_count"]), (16, None, 1))


if __name__ == "__main__":
    unittest.main()
