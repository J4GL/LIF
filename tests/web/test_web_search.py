"""Web category: /api/search and /api/torrent with the SQLite database (SEARCH-010 to SEARCH-012, SEARCH-014)."""
import json
import sqlite3
import threading
import unittest
from urllib.parse import parse_qs, urlsplit

from dht_scraper.magnet_link import MAGNET_TRACKERS
from dht_scraper.torrent_catalog import SOURCE_SAMPLE, TorrentCatalog
from dht_scraper.torrent_info_summary import TorrentFile, TorrentMetadata
from dht_scraper.web_interface import CatalogWebServer, serve_web_server
from tests.search.database_fixtures import FailingDatabase, document, info_hash_hex, open_database
from tests.shared_fixtures import http_get

RECORD_KEYS = ["announce_count", "file_count", "first_seen", "info_hash", "last_seen", "magnet", "name", "seen_count", "size"]


def metadata(number, name):
    return TorrentMetadata(bytes.fromhex(info_hash_hex(number)), name, 5, 16384, 1, [TorrentFile(name, 5)], False, 9.0, ("1.2.3.4", 1))


class WebSearchTestCase(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()

    def start_server(self, documents=()):
        self.database = FailingDatabase(open_database(self, documents))
        self.server = CatalogWebServer(("127.0.0.1", 0), self.catalog, lambda: {}, database=self.database, retry_delay=0.01)
        thread = threading.Thread(target=serve_web_server, args=(self.server,), daemon=True)
        thread.start()

        def stop():
            self.server.shutdown()
            self.server.server_close()
            thread.join(2.0)

        self.addCleanup(stop)

    def get(self, path):
        response, body = http_get(self.server.server_address[1], path)
        return response.status, json.loads(body)

    def search_names(self, query):
        with self.assertLogs("dht_scraper", level="INFO"):
            status, payload = self.get("/api/search?q=" + query)
        self.assertEqual((status, payload["engine"]), (200, "sqlite"))
        return [record["name"] for record in payload["results"]]


class WebSearchSpecTest(WebSearchTestCase):
    def test_SEARCH_010_search_through_database(self):
        self.start_server([document(1, "ubuntu-24.04-desktop-amd64.iso", 5), document(2, "Ubuntu Server", 9), document(3, "Debian", 7, files=["extra/ubuntu-notes.txt"])])
        with self.assertLogs("dht_scraper", level="INFO"):
            status, payload = self.get("/api/search?q=ubuntu&limit=10")
        self.assertEqual((status, payload["engine"]), (200, "sqlite"))
        self.assertEqual([record["name"] for record in payload["results"]], ["Ubuntu Server", "ubuntu-24.04-desktop-amd64.iso", "Debian"])
        self.assertTrue(all(sorted(record) == RECORD_KEYS for record in payload["results"]))
        self.assertTrue(payload["results"][0]["magnet"].startswith("magnet:?xt=urn:btih:"))
        self.assertEqual(self.search_names("&limit=2"), ["Ubuntu Server", "Debian"])
        self.assertEqual(self.search_names("ubuntu%20zzz"), [])

    def test_SEARCH_011_retry_then_unavailable(self):
        self.catalog.store_metadata(metadata(1, "Alpha"))
        self.start_server()
        self.database.fail_next = 1
        with self.assertLogs("dht_scraper", level="INFO"):
            status, payload = self.get("/api/search?q=alpha")
        self.assertEqual((status, payload["engine"]), (200, "sqlite"))
        self.assertEqual(self.database.call_names().count("search"), 2)
        self.database.failing = True
        with self.assertLogs("dht_scraper", level="INFO") as logs:
            answers = [self.get("/api/search?q=alpha") for _ in range(2)]
        self.assertEqual(answers, [(503, {"error": "search database unavailable"})] * 2)
        self.assertEqual(self.database.call_names().count("search"), 6)
        warnings = [line for line in logs.output if line.startswith("WARNING") and "disk I/O error" in line]
        self.assertEqual(len(warnings), 2)
        self.database.failing = False
        self.assertEqual(self.search_names("alpha"), [])

    def test_SEARCH_012_detail_resolution(self):
        hash_a, hash_b, hash_d, hash_e = (info_hash_hex(number) for number in (1, 2, 4, 5))
        self.catalog.store_metadata(metadata(1, "catalog name"))
        self.catalog.record_hash(bytes.fromhex(hash_b), SOURCE_SAMPLE)
        self.start_server([document(1, "index name"), document(2, "B", seen_count=6), document(4, "D", seen_count=8, files=["d/1", "d/2", "d/3"])])
        with self.assertLogs("dht_scraper", level="INFO"):
            _, detail_a = self.get("/api/torrent/" + hash_a)
            _, detail_b = self.get("/api/torrent/" + hash_b)
            _, detail_d = self.get("/api/torrent/" + hash_d)
            status_e, _ = self.get("/api/torrent/" + hash_e)
        self.assertEqual(detail_a["metadata"]["name"], "catalog name")
        for detail, seen in ((detail_b, 6), (detail_d, 8)):
            self.assertEqual((detail["fetch_state"], detail["peers"], detail["metadata"]["source_peer"], detail["seen_count"]), ("done", [], None, seen))
            self.assertTrue(detail["magnet"].startswith("magnet:?xt=urn:btih:" + detail["info_hash"]))
        self.assertEqual(detail_d["metadata"]["files"], [{"path": "d/%d" % index, "length": 7} for index in (1, 2, 3)])
        self.assertEqual(status_e, 404)
        self.database.failing = True
        with self.assertLogs("dht_scraper", level="INFO"):
            status_b, detail_b = self.get("/api/torrent/" + hash_b)
            status_e, error_e = self.get("/api/torrent/" + hash_e)
        self.assertEqual((status_b, detail_b["metadata"]), (200, None))
        self.assertEqual((status_e, error_e), (503, {"error": "search database unavailable"}))
        self.assertEqual(self.database.call_names().count("get_document"), 3 + 2 * 2)

    def test_SEARCH_017_magnets_through_web(self):
        hash_a = info_hash_hex(1)
        self.start_server([document(1, "Alpha")])
        with self.assertLogs("dht_scraper", level="INFO"):
            _, search = self.get("/api/search?q=alpha")
            status, detail = self.get("/api/torrent/" + hash_a)
        self.assertEqual((len(search["results"]), status), (1, 200))
        for magnet in (search["results"][0]["magnet"], detail["magnet"]):
            query = parse_qs(urlsplit(magnet).query)
            self.assertEqual((query["xt"], query["tr"]), (["urn:btih:" + hash_a], list(MAGNET_TRACKERS)))

    def test_SEARCH_014_typo_through_web(self):
        self.start_server([document(1, "Big Buck Bunny 1080p"), document(2, "Sintel")])
        self.assertEqual(self.search_names("bunyn%20buck"), ["Big Buck Bunny 1080p"])
        self.assertEqual(self.search_names("sintle"), ["Sintel"])


    def test_SEARCH_015_sorted_results(self):
        rows = [("beta", 30, 2, 5, 1, 20.0), ("Alpha", 10, 3, 9, 0, 30.0), ("gamma", 20, 1, 7, 4, 10.0), ("alphb", 40, 1, 1, 0, 5.0)]
        expected = {
            "name": ("Alpha alphb beta gamma", "gamma beta alphb Alpha"),
            "size": ("Alpha gamma beta alphb", "alphb beta gamma Alpha"),
            "file_count": ("alphb gamma beta Alpha", "Alpha beta gamma alphb"),
            "seen_count": ("alphb beta gamma Alpha", "Alpha gamma beta alphb"),
            "announce_count": ("alphb Alpha beta gamma", "gamma beta Alpha alphb"),
            "last_seen": ("alphb gamma beta Alpha", "Alpha beta gamma alphb"),
        }

        def get(path):
            with self.assertLogs("dht_scraper", level="INFO"):
                status, payload = self.get(path)
            self.assertEqual(status, 200)
            return payload, " ".join(record["name"] for record in payload["results"])

        with self.subTest(case="sqlite"):
            documents = [document(number, name, seen, last_seen=last, announce_count=announces, files=["%s/%d" % (name, index) for index in range(files)], lengths=[size // files] * (files - 1) + [size - size // files * (files - 1)]) for number, (name, size, files, seen, announces, last) in enumerate(rows, 1)]
            self.start_server(documents)
            for column, (ascending, descending) in expected.items():
                for order, names in (("asc", ascending), ("desc", descending)):
                    payload, found = get("/api/search?q=&sort=%s&order=%s" % (column, order))
                    self.assertEqual((found, payload["sort"], payload["order"]), (names, column, order), column)
            self.assertEqual(get("/api/search?q=&sort=name")[0]["order"], "asc")
            self.assertEqual(get("/api/search?q=&sort=size")[0]["order"], "desc")
            self.assertEqual(get("/api/search?q=alpha&sort=size&order=asc")[1], "Alpha alphb")
            payload, found = get("/api/search?q=&sort=bogus&order=up")
            self.assertEqual((found, payload["sort"]), ("Alpha gamma beta alphb", None))
        with self.subTest(case="memory"):
            catalog = TorrentCatalog()
            for number, (name, size, files, seen, announces, last) in enumerate(rows, 1):
                catalog.store_metadata(TorrentMetadata(bytes.fromhex(info_hash_hex(number)), name, size, 16384, files, [TorrentFile(name, size)], False, 9.0, None))
            server = CatalogWebServer(("127.0.0.1", 0), catalog, lambda: {})
            thread = threading.Thread(target=serve_web_server, args=(server,), daemon=True)
            thread.start()
            self.addCleanup(thread.join, 2.0)
            self.addCleanup(server.server_close)
            self.addCleanup(server.shutdown)
            with self.assertLogs("dht_scraper", level="INFO"):
                response, body = http_get(server.server_address[1], "/api/search?q=&sort=size&order=asc")
            payload = json.loads(body)
            self.assertEqual((payload["engine"], " ".join(record["name"] for record in payload["results"])), ("memory", "Alpha gamma beta alphb"))


if __name__ == "__main__":
    unittest.main()
