"""Web category: HTTP routes, JSON shapes, headers and the page (F-005)."""
import html.parser
import json
import threading
import unittest

from dht_scraper.torrent_catalog import SOURCE_SAMPLE, TorrentCatalog
from dht_scraper.torrent_info_summary import TorrentFile, TorrentMetadata
from dht_scraper.web_interface import CatalogWebServer, parse_info_hash_hex, parse_search_query, serve_web_server
from tests.shared_fixtures import http_get

HASH_A = b"\x0a" * 20
HASH_B = b"\x0b" * 20
HOSTILE = "<script>alert(1)</script>"


class PageStructure(html.parser.HTMLParser):
    """Element ids with their attributes, direct text, and the data-stat keys inside each id."""

    def __init__(self, text):
        super().__init__()
        self.attributes, self.texts, self.stats, self.open = {}, {}, {}, []
        self.feed(text)

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if "data-stat" in values:
            for element_id, _ in self.open:
                self.stats.setdefault(element_id, []).append(values["data-stat"])
        if tag in ("input", "meta", "br"):
            if "id" in values:
                self.attributes[values["id"]] = values
            return
        self.open.append((values.get("id"), tag))
        if "id" in values:
            self.attributes[values["id"]] = values

    def handle_endtag(self, tag):
        while self.open:
            _, opened = self.open.pop()
            if opened == tag:
                break

    def handle_data(self, data):
        if self.open and self.open[-1][0] and data.strip():
            self.texts[self.open[-1][0]] = self.texts.get(self.open[-1][0], "") + data.strip()

    def stats_in(self, element_id):
        return self.stats.get(element_id, [])


class WebInterfaceTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()
        self.catalog.store_metadata(TorrentMetadata(HASH_A, HOSTILE, 30, 16384, 2, [TorrentFile("dir/a.txt", 10), TorrentFile("dir/b.txt", 20)], False, 5.0, ("1.2.3.4", 6881)))
        self.catalog.store_metadata(TorrentMetadata(HASH_B, "ubuntu", 1, 16384, 1, [TorrentFile("ubuntu", 1)], False, 5.0, None))
        for _ in range(3):
            self.catalog.record_hash(HASH_B, SOURCE_SAMPLE, peer=("5.5.5.5", 5))
        self.server = CatalogWebServer(("127.0.0.1", 0), self.catalog, lambda: {"hashes_seen": 2, "nodes": 4})
        self.thread = threading.Thread(target=serve_web_server, args=(self.server,), daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(2.0)

    def get(self, path):
        return http_get(self.server.server_address[1], path)

    def test_index_page_served_f005_t1(self):
        response, body = self.get("/")
        self.assertEqual(response.status, 200)
        self.assertTrue(response.getheader("Content-Type").startswith("text/html"))
        self.assertIn("default-src 'none'", response.getheader("Content-Security-Policy"))
        self.assertEqual(response.getheader("X-Content-Type-Options"), "nosniff")
        self.assertNotIn("Python", response.getheader("Server"))
        for marker in (b'id="stats"', b'id="results"', b'id="detail"'):
            self.assertIn(marker, body)
        for forbidden in (b"innerHTML", b"src=", b"<link"):
            self.assertNotIn(forbidden, body)

    def test_RUNTIME_002_page_shows_pipeline_counters(self):
        _, body = self.get("/")
        page = PageStructure(body.decode("utf-8"))
        self.assertEqual(page.stats_in("stats-summary"), ["uptime_seconds", "torrents", "torrents_size"])
        self.assertEqual((page.texts.get("stats-toggle"), page.attributes["stats-toggle"].get("aria-controls")), (">>", "stats-more"))
        self.assertIn("hidden", page.attributes["stats-more"])
        keys = ("hashes_seen", "with_metadata", "fetch_connections", "fetch_attempts", "fetch_in_progress", "fetch_failed", "lookups_started", "samples_received", "packets_sent", "packets_received", "index_documents")
        self.assertEqual([key for key in keys if key not in page.stats_in("stats-more")], [])
        self.assertNotIn(b'data-stat="hashes_discovered"', body)
        self.assertIn(b'key === "torrents_size" ? formatBytes(value)', body)
        self.assertIn(b'key === "uptime_seconds" ? formatDuration(value)', body)

    def test_RUNTIME_009_default_limit_20(self):
        for number in range(25):
            self.catalog.store_metadata(TorrentMetadata(bytes([0x40 + number]) * 20, "t%d" % number, 1, 16384, 1, [TorrentFile("t%d" % number, 1)], False, 1.0, None))
        _, raw = self.get("/api/search?q=")
        payload = json.loads(raw)
        self.assertEqual((payload["limit"], len(payload["results"])), (20, 20))
        _, body = self.get("/")
        page = PageStructure(body.decode("utf-8"))
        self.assertEqual(page.attributes["limit"].get("value"), "20")
        self.assertIn(b'document.getElementById("limit").value || "20"', body)

    def test_RUNTIME_004_detail_panel_hides_peers(self):
        _, body = self.get("/")
        for removed in (b"Candidate peers", b"detail-peers", b"fetched from"):
            self.assertNotIn(removed, body)
        for kept in (b'id="detail-fields"', b'id="detail-files"', b'"fetched at"'):
            self.assertIn(kept, body)

    def test_api_stats_returns_provider_payload(self):
        response, body = self.get("/api/stats")
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(body), {"hashes_seen": 2, "nodes": 4})

    def test_api_search_returns_ranked_results_with_magnet(self):
        with self.assertLogs("dht_scraper", level="INFO"):
            _, body = self.get("/api/search?q=&limit=999")
        payload = json.loads(body)
        self.assertEqual((payload["limit"], payload["count"]), (200, 2))
        self.assertEqual([row["name"] for row in payload["results"]], ["ubuntu", HOSTILE])
        self.assertTrue(payload["results"][0]["magnet"].startswith("magnet:?xt=urn:btih:" + HASH_B.hex()))
        response, body = self.get("/api/search?q=b.txt")
        self.assertEqual([row["info_hash"] for row in json.loads(body)["results"]], [HASH_A.hex()])
        self.assertEqual(json.loads(body)["results"][0]["name"], HOSTILE)
        self.assertTrue(response.getheader("Content-Type").startswith("application/json"))
        self.assertEqual(response.getheader("X-Content-Type-Options"), "nosniff")

    def test_malformed_request_line_gets_an_error_status(self):
        import socket
        raw = socket.create_connection(("127.0.0.1", self.server.server_address[1]), timeout=2.0)
        raw.sendall(b"GET / FOO/1.1\r\n\r\n")
        reply = b""
        while True:
            chunk = raw.recv(4096)
            if not chunk:
                break
            reply += chunk
        raw.close()
        self.assertIn(b"Error code: 400", reply)

    def test_api_torrent_detail_and_errors(self):
        response, body = self.get("/api/torrent/" + HASH_A.hex())
        detail = json.loads(body)
        self.assertEqual(response.status, 200)
        self.assertEqual(detail["metadata"]["files"], [{"path": "dir/a.txt", "length": 10}, {"path": "dir/b.txt", "length": 20}])
        self.assertEqual(detail["metadata"]["source_peer"], {"ip": "1.2.3.4", "port": 6881})
        self.assertEqual(detail["fetch_state"], "done")
        self.assertTrue(detail["magnet"].startswith("magnet:"))
        self.assertEqual(self.get("/api/torrent/" + "0c" * 20)[0].status, 404)
        response, body = self.get("/api/torrent/xyz")
        self.assertEqual((response.status, json.loads(body)), (400, {"error": "invalid info hash"}))
        response, body = self.get("/nothing")
        self.assertEqual((response.status, json.loads(body)), (404, {"error": "not found"}))


class ParsingTest(unittest.TestCase):
    def test_parse_search_query_defaults_and_clamps(self):
        self.assertEqual(parse_search_query(""), ("", 20))
        self.assertEqual(parse_search_query("q=+abc+&limit=3"), ("abc", 3))
        self.assertEqual(parse_search_query("limit=0"), ("", 1))
        self.assertEqual(parse_search_query("limit=abc"), ("", 20))
        self.assertEqual(len(parse_search_query("q=" + "x" * 500)[0]), 200)

    def test_parse_info_hash_hex(self):
        self.assertEqual(parse_info_hash_hex("AB" * 20), b"\xab" * 20)
        self.assertIsNone(parse_info_hash_hex("ab" * 19))
        self.assertIsNone(parse_info_hash_hex("zz" * 20))
        self.assertIsNone(parse_info_hash_hex("ab" * 19 + "  "))


if __name__ == "__main__":
    unittest.main()
