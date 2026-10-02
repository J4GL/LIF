"""Web category: the page as rendered by a headless browser (RUNTIME-002, RUNTIME-009)."""
import json
import threading
import unittest

from dht_scraper.torrent_catalog import TorrentCatalog
from dht_scraper.torrent_info_summary import TorrentFile, TorrentMetadata
from dht_scraper.web_interface import CatalogWebServer, serve_web_server
from tests.shared_fixtures import http_get
from tests.web.browser_fixtures import OBSCURA_VERSION, evaluate, requires_browser

STATS = {"uptime_seconds": 93784.4, "torrents": 3, "torrents_size": 2376220366990871, "hashes_seen": 42}
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
        self.assertEqual(page["summary"], [["uptime_seconds", "1d 2h 3m 4s"], ["torrents", "3"], ["torrents_size", "2.1 PiB"]], OBSCURA_VERSION)
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


if __name__ == "__main__":
    unittest.main()
