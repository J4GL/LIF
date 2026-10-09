"""Web category: the page as rendered by a headless browser (RUNTIME-002, RUNTIME-009)."""
import json
import threading
import unittest

from dht_scraper.torrent_catalog import TorrentCatalog
from dht_scraper.torrent_info_summary import TorrentFile, TorrentMetadata
from dht_scraper.web_interface import CatalogWebServer, serve_web_server
from tests.search.database_fixtures import FailingDatabase, open_database
from tests.shared_fixtures import http_get
from tests.web.browser_fixtures import OBSCURA_VERSION, evaluate, requires_browser

STATS = {"uptime_seconds": 93784.4, "torrents": 3, "torrents_size": 2376220366990871, "database_size": 5242880, "hashes_seen": 42}
MORE_KEYS = [
    "nodes", "hashes_seen", "with_metadata", "fetch_connections", "fetch_attempts", "fetch_in_progress", "fetch_failed", "lookups_started",
    "samples_received", "packets_sent", "packets_received", "index_documents",
]
HEADER_STATE = """
  function state() {
    var more = document.getElementById("stats-more"); var toggle = document.getElementById("stats-toggle");
    return {hidden: more.hidden, display: getComputedStyle(more).display, toggle: toggle.textContent, expanded: toggle.getAttribute("aria-expanded")};
  }
  function stats(selector) {
    return Array.prototype.map.call(document.querySelectorAll(selector + " [data-stat]"), function (e) { return [e.getAttribute("data-stat"), e.textContent]; });
  }
  var before = state(); var summary = stats("#stats-summary"); var more = stats("#stats-more");
  var discovered = document.querySelectorAll('[data-stat="hashes_discovered"]').length;
  document.getElementById("stats-toggle").click(); var opened = state();
  document.getElementById("stats-toggle").click(); var closed = state();
  return {before: before, summary: summary, more: more, discovered: discovered, opened: opened, closed: closed};
"""


@requires_browser
class PageInBrowserSpecTest(unittest.TestCase):
    def serve(self, catalog, stats_provider):
        server = CatalogWebServer(("127.0.0.1", 0), catalog, stats_provider)
        thread = threading.Thread(target=serve_web_server, args=(server,), daemon=True)
        thread.start()

        def stop():
            server.shutdown()
            server.server_close()
            thread.join(2.0)

        self.addCleanup(stop)
        return server

    def url(self, server):
        return "http://127.0.0.1:%d/" % server.server_address[1]

    def test_RUNTIME_002_page_shows_pipeline_counters(self):
        server = self.serve(TorrentCatalog(), lambda: dict(STATS))
        with self.assertLogs("dht_scraper", level="INFO"):
            page = evaluate(self.url(server), HEADER_STATE)
        self.assertEqual(page["summary"], [["uptime_seconds", "1d 2h 3m 4s"], ["torrents", "3"], ["torrents_size", "2.1 PiB"], ["database_size", "5.0 MiB"]], OBSCURA_VERSION)
        self.assertEqual(page["before"], {"hidden": True, "display": "none", "toggle": ">>", "expanded": "false"})
        self.assertEqual([key for key, _ in page["more"]], MORE_KEYS)
        self.assertEqual(dict(page["more"])["hashes_seen"], "42")
        self.assertEqual(page["discovered"], 0)
        self.assertEqual(page["opened"], {"hidden": False, "display": "flex", "toggle": "<<", "expanded": "true"})
        self.assertEqual(page["closed"], {"hidden": True, "display": "none", "toggle": ">>", "expanded": "false"})

    def test_RUNTIME_009_default_limit_20(self):
        catalog = TorrentCatalog()
        for number in range(25):
            catalog.store_metadata(TorrentMetadata(bytes([0x40 + number]) * 20, "t%d" % number, 1, 16384, 1, [TorrentFile("t%d" % number, 1)], False, 1.0, None))
        server = self.serve(catalog, lambda: {})
        with self.assertLogs("dht_scraper", level="INFO"):
            _, raw = http_get(server.server_address[1], "/api/search?q=")
            payload = json.loads(raw)
            page = evaluate(self.url(server), 'return [document.getElementById("limit").value, document.querySelectorAll("#results tbody tr").length];')
        self.assertEqual((payload["limit"], len(payload["results"])), (20, 20))
        self.assertEqual(page, ["20", 20])


    def test_RUNTIME_010_unavailable_message(self):
        for failing, expected in ((True, "Search database unavailable, retrying\u2026"), (False, "No fetched torrents match yet. Metadata arrives as peers answer.")):
            with self.subTest(failing=failing):
                database = FailingDatabase(open_database(self), failing=failing)
                server = CatalogWebServer(("127.0.0.1", 0), TorrentCatalog(), lambda: {}, database=database, retry_delay=0.01)
                thread = threading.Thread(target=serve_web_server, args=(server,), daemon=True)
                thread.start()
                self.addCleanup(thread.join, 2.0)
                self.addCleanup(server.server_close)
                self.addCleanup(server.shutdown)
                with self.assertLogs("dht_scraper", level="INFO"):
                    page = evaluate(self.url(server), 'var empty = document.getElementById("empty"); return [document.querySelectorAll("#results tbody tr").length, empty.hidden, getComputedStyle(empty).display, empty.textContent];')
                self.assertEqual(page[:2], [0, False])
                self.assertNotEqual(page[2], "none")
                self.assertEqual(page[3], expected)


    def test_RUNTIME_012_sort_by_column(self):
        catalog = TorrentCatalog()
        for byte, name, size in ((1, "beta", 30), (2, "Alpha", 10), (3, "gamma", 20)):
            catalog.store_metadata(TorrentMetadata(bytes([byte]) * 20, name, size, 16384, 1, [TorrentFile(name, size)], False, 1.0, None))
        server = self.serve(catalog, lambda: {})
        script = """
          function headers() {
            return Array.prototype.map.call(document.querySelectorAll("#results thead th"), function (th) {
              return [th.textContent.replace(/[\\u25b2\\u25bc]/g, "").trim(), th.querySelector("button") !== null, th.getAttribute("aria-sort"), th.textContent];
            });
          }
          function click(label) {
            var buttons = document.querySelectorAll("#results thead th button");
            for (var i = 0; i < buttons.length; i += 1) { if (buttons[i].textContent.indexOf(label) === 0) { buttons[i].click(); } }
            return [headers(), location.hash];
          }
          return {initial: headers(), first: click("Size"), second: click("Size"), name: click("Name")};
        """
        with self.assertLogs("dht_scraper", level="INFO"):
            page = evaluate(self.url(server), script)
            sorted_page = evaluate(self.url(server) + "#sort=size&order=asc", 'return [Array.prototype.map.call(document.querySelectorAll("#results tbody td.name"), function (td) { return td.textContent; }), document.querySelector("th[aria-sort]") && document.querySelector("th[aria-sort]").getAttribute("aria-sort")];')
        labels = ["Name", "Size", "Files", "Seen", "Announces", "Last seen", "Magnet"]
        self.assertEqual([row[:3] for row in page["initial"]], [[label, label != "Magnet", None] for label in labels])

        def sort_state(state):
            headers, fragment = state
            marked = [(row[0], row[2], row[3]) for row in headers if row[2] is not None]
            return marked, fragment

        self.assertEqual(sort_state(page["first"]), ([("Size", "descending", "Size \u25bc")], "#sort=size&order=desc"))
        self.assertEqual(sort_state(page["second"]), ([("Size", "ascending", "Size \u25b2")], "#sort=size&order=asc"))
        self.assertEqual(sort_state(page["name"]), ([("Name", "ascending", "Name \u25b2")], "#sort=name&order=asc"))
        self.assertEqual(sorted_page, [["Alpha", "gamma", "beta"], "ascending"])

    def test_RUNTIME_013_abbreviated_counts_with_tooltip(self):
        catalog = TorrentCatalog()
        files = [TorrentFile("f%d" % number, 1) for number in range(200)]
        catalog.store_metadata(TorrentMetadata(bytes([0x77]) * 20, "many files", 2000, 16384, 2000, files, False, 1.0, None))
        stats = {"torrents": 2000, "nodes": 999, "hashes_seen": 1234, "with_metadata": 999999, "fetch_failed": 2000000, "packets_sent": 1500000000}
        server = self.serve(catalog, lambda: dict(stats))
        script = """
          function read(element) { return [element.textContent, element.getAttribute("data-full")]; }
          function stat(key) { return document.querySelector('[data-stat="' + key + '"]'); }
          function tip() { var t = document.getElementById("tooltip"); return [t.hidden, t.textContent]; }
          function hover(element) { element.dispatchEvent(new MouseEvent("mouseover", {bubbles: true})); return tip(); }
          function leave(element) { element.dispatchEvent(new MouseEvent("mouseout", {bubbles: true})); return tip(); }
          var filesCell = document.querySelector("#results tbody tr td:nth-child(3)");
          var texts = {torrents: read(stat("torrents")), nodes: read(stat("nodes")), hashes_seen: read(stat("hashes_seen")), with_metadata: read(stat("with_metadata")), fetch_failed: read(stat("fetch_failed")), packets_sent: read(stat("packets_sent")), files: read(filesCell)};
          var over = hover(stat("hashes_seen")); var out = leave(stat("hashes_seen"));
          var overNodes = hover(stat("nodes"))[0]; leave(stat("nodes"));
          return {texts: texts, over: over, out: out, overNodes: overNodes};
        """
        with self.assertLogs("dht_scraper", level="INFO"):
            page = evaluate(self.url(server), script)
        self.assertEqual(page["texts"], {
            "torrents": ["2k", "2 000"], "nodes": ["999", None], "hashes_seen": ["1.2k", "1 234"], "with_metadata": ["1M", "999 999"],
            "fetch_failed": ["2M", "2 000 000"], "packets_sent": ["1.5B", "1 500 000 000"], "files": ["2k", "2 000"],
        })
        self.assertEqual(page["over"], [False, "1 234"])
        self.assertEqual(page["out"], [True, "1 234"])
        self.assertIs(page["overNodes"], True)


if __name__ == "__main__":
    unittest.main()
