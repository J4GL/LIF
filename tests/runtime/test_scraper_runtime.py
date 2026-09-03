"""Runtime category: thread wiring, shutdown and the hash-to-search end to end path (F-001, F-005, F-006)."""
import socket
import threading
import unittest

from dht_scraper.bencode_codec import encode_bencode
from dht_scraper.krpc_messages import make_token
from dht_scraper.scraper_runtime import RuntimeSettings, ScraperRuntime
from dht_scraper.torrent_catalog import TorrentCatalog
from tests.fetch.fake_metadata_peer import build_info_dict
from tests.shared_fixtures import http_get_json, wait_until

BOOTSTRAP = [("127.0.0.1", 1)]
THREAD_NAMES = ("crawler", "scheduler", "web", "fetch-")


def project_threads():
    return [thread.name for thread in threading.enumerate() if thread.name.startswith(THREAD_NAMES)]


def settings(**overrides):
    values = dict(port=0, nodes=2, web_host="127.0.0.1", web_port=0, duration=None, fetch_workers=2, batch_size=1, interval=0.05, fetch_enabled=True, open_browser=False)
    values.update(overrides)
    return RuntimeSettings(**values)


class RuntimeSettingsTest(unittest.TestCase):
    def test_rejects_invalid_values(self):
        for bad in [dict(nodes=0), dict(nodes=65), dict(port=65530, nodes=8), dict(fetch_workers=0), dict(interval=0), dict(web_host=""), dict(duration=-1)]:
            with self.assertRaises(AssertionError, msg=repr(bad)):
                settings(**bad)


class ScraperRuntimeTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()
        self.opened = []

    def make_runtime(self, run_settings=None, fetch_function=None, **kwargs):
        fetch = fetch_function or (lambda info_hash, peer, stop_event: b"")
        return ScraperRuntime(run_settings or settings(), self.catalog, fetch, BOOTSTRAP, browser_opener=self.opened.append, **kwargs)

    def get_json(self, runtime, path):
        return http_get_json(runtime.web_server.server_address[1], path)

    def test_start_and_stop_join_all_threads_f001_t2(self):
        runtime = self.make_runtime(settings(open_browser=True))
        runtime.start()
        try:
            self.assertTrue(wait_until(lambda: len(self.opened) == 1))
            self.assertEqual(self.opened[0], "http://127.0.0.1:%d/" % runtime.web_server.server_address[1])
            status, stats = self.get_json(runtime, "/api/stats")
            self.assertEqual((status, stats["nodes"], stats["fetch_enabled"]), (200, 2, True))
            self.assertGreaterEqual(len(project_threads()), 5)
            sockets = [node.udp_socket for node in runtime.node_sockets]
        finally:
            runtime.stop()
        self.assertTrue(wait_until(lambda: project_threads() == []))
        self.assertEqual(runtime.node_sockets, [])
        self.assertTrue(all(sock.fileno() == -1 for sock in sockets))
        runtime.stop()

    def test_duration_stops_by_itself_and_no_fetch_starts_no_scheduler(self):
        runtime = self.make_runtime(settings(fetch_enabled=False))
        stats = runtime.run(0.3)
        self.assertTrue(runtime.stopped)
        self.assertIsNone(runtime.scheduler)
        self.assertFalse(stats["fetch_enabled"])
        self.assertEqual(self.opened, [])
        self.assertTrue(wait_until(lambda: project_threads() == []))

    def test_socket_failure_cleans_up(self):
        def failing_factory(port, count):
            raise OSError("bind failed")

        runtime = self.make_runtime(socket_factory=failing_factory)
        with self.assertRaises(OSError):
            runtime.start()
        self.assertTrue(runtime.stopped)
        self.assertIsNone(runtime.crawler_thread)
        self.assertEqual(project_threads(), [])

    def test_web_port_in_use_fails_before_crawler_starts(self):
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        try:
            runtime = self.make_runtime(settings(web_port=blocker.getsockname()[1]))
            with self.assertRaises(OSError):
                runtime.start()
            self.assertIsNone(runtime.crawler_thread)
            self.assertEqual(runtime.node_sockets, [])
        finally:
            blocker.close()

    def test_end_to_end_announce_to_search_f005_t2(self):
        raw, info_hash = build_info_dict(name=b"End To End Torrent")
        fetched_from = []

        def fake_fetch(hash_value, peer, stop_event):
            fetched_from.append(peer)
            return raw

        runtime = self.make_runtime(settings(nodes=1, interval=0.05), fetch_function=fake_fetch)
        runtime.start()
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            node = runtime.node_sockets[0]
            token = make_token(runtime.crawler.token_secret, "127.0.0.1")
            announce = encode_bencode({b"t": b"tt", b"y": b"q", b"q": b"announce_peer", b"a": {b"id": b"p" * 20, b"info_hash": info_hash, b"port": 6881, b"token": token, b"implied_port": 0}})
            sender.sendto(announce, ("127.0.0.1", node.port))
            self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["hashes_seen"] == 1))
            # A loopback announcer is not a routable peer, so the peer is injected the way a lookup result would be.
            self.assertEqual(self.catalog.add_peers(info_hash, [("127.0.0.1", 6881)]), 1)
            self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1))
            self.assertEqual(fetched_from, [("127.0.0.1", 6881)])
            status, payload = self.get_json(runtime, "/api/search?q=end%20to%20end")
            self.assertEqual((status, payload["count"], payload["results"][0]["info_hash"]), (200, 1, info_hash.hex()))
            status, detail = self.get_json(runtime, "/api/torrent/" + info_hash.hex())
            self.assertEqual(detail["metadata"]["files"], [{"path": "End To End Torrent", "length": 1234}])
            self.assertEqual(detail["announce_count"], 1)
        finally:
            sender.close()
            runtime.stop()
        self.assertTrue(wait_until(lambda: project_threads() == []))


if __name__ == "__main__":
    unittest.main()
