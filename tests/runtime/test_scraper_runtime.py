"""Runtime category: thread wiring, shutdown and the hash-to-search end to end path (F-001, F-005, F-006)."""
import socket
import threading
import unittest

from dht_scraper.bencode_codec import encode_bencode
from dht_scraper.krpc_messages import make_token
from dht_scraper.metadata_fetcher import MetadataFetchError
from dht_scraper.scraper_runtime import RuntimeSettings, ScraperRuntime
from dht_scraper.torrent_catalog import TorrentCatalog
from tests.fetch.fake_metadata_peer import build_info_dict
from tests.shared_fixtures import http_get_json, wait_until

BOOTSTRAP = [("127.0.0.1", 1)]
THREAD_NAMES = ("crawler", "fetch", "web")


def project_threads():
    return [thread.name for thread in threading.enumerate() if thread.name.startswith(THREAD_NAMES)]


async def never_fetch(info_hash, peer):
    raise MetadataFetchError("connect")


def settings(**overrides):
    values = dict(port=0, nodes=2, web_host="127.0.0.1", web_port=0, duration=None, fetch_workers=2, batch_size=1, interval=0.05, fetch_enabled=True, open_browser=False, ipv6_enabled=False)
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
        fetch = fetch_function or never_fetch
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
            self.assertEqual(sorted(project_threads()), ["crawler", "fetch", "web"])
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
        self.assertIsNone(runtime.fetch_engine)
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

    def test_V6_005_runtime_opens_ipv6_sockets(self):
        with self.subTest(case="enabled"):
            runtime = self.make_runtime(settings(fetch_enabled=False, ipv6_enabled=True))
            runtime.start()
            sender = socket.socket(socket.AF_INET6, socket.SOCK_DGRAM)
            sender.settimeout(2.0)
            try:
                by_family = {}
                for node in runtime.node_sockets:
                    by_family.setdefault(node.udp_socket.family, []).append(node)
                self.assertEqual((len(by_family[socket.AF_INET]), len(by_family[socket.AF_INET6])), (2, 2))
                self.assertEqual({node.own_id for node in by_family[socket.AF_INET]}, {node.own_id for node in by_family[socket.AF_INET6]})
                node6 = by_family[socket.AF_INET6][0]
                sender.sendto(encode_bencode({b"t": b"tt", b"y": b"q", b"q": b"ping", b"a": {b"id": b"p" * 20}}), ("::1", node6.port))
                data, address = sender.recvfrom(65535)
                self.assertEqual(address[1], node6.port)
            finally:
                sender.close()
                runtime.stop()
        with self.subTest(case="disabled"):
            runtime = self.make_runtime(settings(fetch_enabled=False, ipv6_enabled=False))
            runtime.start()
            try:
                self.assertEqual([node.udp_socket.family for node in runtime.node_sockets], [socket.AF_INET, socket.AF_INET])
            finally:
                runtime.stop()
        self.assertTrue(wait_until(lambda: project_threads() == []))

    def test_RUNTIME_001_stats_expose_pipeline_counters(self):
        raw, info_hash = build_info_dict(name=b"Counted")

        async def fake_fetch(hash_value, peer):
            return raw

        runtime = self.make_runtime(settings(nodes=1), fetch_function=fake_fetch)
        runtime.start()
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            token = make_token(runtime.crawler.token_secret, "127.0.0.1")
            announce = encode_bencode({b"t": b"tt", b"y": b"q", b"q": b"announce_peer", b"a": {b"id": b"p" * 20, b"info_hash": info_hash, b"port": 6881, b"token": token}})
            sender.sendto(announce, ("127.0.0.1", runtime.node_sockets[0].port))
            self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["hashes_seen"] == 1))
            self.catalog.add_peers(info_hash, [("127.0.0.1", 6881)])
            self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["with_metadata"] == 1))
            self.assertTrue(wait_until(lambda: self.get_json(runtime, "/api/stats")[1]["fetch_connections"] == 0))
            status, stats = self.get_json(runtime, "/api/stats")
            self.assertEqual(status, 200)
            self.assertEqual({key: stats[key] for key in ("hashes_discovered", "with_metadata", "fetch_attempts", "fetch_successes", "fetch_connections")},
                             {"hashes_discovered": 1, "with_metadata": 1, "fetch_attempts": 1, "fetch_successes": 1, "fetch_connections": 0})
            for key in ("fetch_workers", "fetch_queue_size", "lookups_started", "lookup_queries_sent", "lookup_peers_found", "sample_responses", "packets_throttled", "queue_size"):
                self.assertIn(key, stats)
        finally:
            sender.close()
            runtime.stop()
        self.assertTrue(wait_until(lambda: project_threads() == []))

    def test_end_to_end_announce_to_search_f005_t2(self):
        raw, info_hash = build_info_dict(name=b"End To End Torrent")
        fetched_from = []

        async def fake_fetch(hash_value, peer):
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
