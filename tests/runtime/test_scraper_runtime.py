"""Runtime category: thread wiring, shutdown and the hash-to-search end to end path (F-001, F-005, F-006)."""
import os
import signal
import socket
import sqlite3
import threading
import time
import unittest

from dht_scraper.bencode_codec import encode_bencode
from dht_scraper.krpc_messages import make_token
from dht_scraper.metadata_fetcher import MetadataFetchError
from dht_scraper.__main__ import main
from dht_scraper.scraper_runtime import RuntimeSettings, ScraperRuntime
from dht_scraper.torrent_catalog import SOURCE_SAMPLE, TorrentCatalog
from dht_scraper.torrent_database import TorrentDatabase
from dht_scraper.torrent_info_summary import TorrentFile, TorrentMetadata
from tests.fetch.fake_metadata_peer import build_info_dict
from tests.search.database_fixtures import temporary_directory, user_version
from tests.shared_fixtures import http_get_json, wait_until

BOOTSTRAP = [("127.0.0.1", 1)]
THREAD_NAMES = ("crawler", "fetch", "web", "index")
FAST_INDEX = {"document_seconds": 0.1, "counter_seconds": 0.1, "count_seconds": 0.2, "check_seconds": 0.05}


def project_threads():
    return [thread.name for thread in threading.enumerate() if thread.name.startswith(THREAD_NAMES)]


async def never_fetch(info_hash, peer):
    raise MetadataFetchError("connect")


def settings(**overrides):
    values = dict(port=0, nodes=2, web_host="127.0.0.1", web_port=0, duration=None, fetch_workers=2, batch_size=1, interval=0.05, fetch_enabled=True, open_browser=False, ipv6_enabled=False, database_path=None)
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

    def announce(self, runtime, info_hash):
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            token = make_token(runtime.crawler.token_secret, "127.0.0.1")
            message = {b"t": b"tt", b"y": b"q", b"q": b"announce_peer", b"a": {b"id": b"p" * 20, b"info_hash": info_hash, b"port": 6881, b"token": token}}
            sender.sendto(encode_bencode(message), ("127.0.0.1", runtime.node_sockets[0].port))
        finally:
            sender.close()

    def test_SEARCH_013_no_index_configured(self):
        runtime = self.make_runtime(settings(nodes=1, fetch_enabled=False))
        runtime.start()
        try:
            self.catalog.store_metadata(TorrentMetadata(b"\x0c" * 20, "Gamma", 1, 16384, 1, [TorrentFile("Gamma", 1)], False, 1.0, None))
            self.assertNotIn("index", project_threads())
            _, stats = self.get_json(runtime, "/api/stats")
            self.assertEqual([key for key in stats if key.startswith("index_")], [])
            _, payload = self.get_json(runtime, "/api/search?q=gamma")
            self.assertEqual((payload["engine"], [record["name"] for record in payload["results"]]), ("memory", ["Gamma"]))
        finally:
            runtime.stop()
        self.assertTrue(wait_until(lambda: project_threads() == []))

    def test_RUNTIME_005_sigterm_stops_cleanly(self):
        original = signal.getsignal(signal.SIGTERM)
        self.addCleanup(signal.signal, signal.SIGTERM, original)
        received = []

        def previous_handler(signum, frame):
            received.append(signum)

        signal.signal(signal.SIGTERM, previous_handler)
        runtime = self.make_runtime(settings(nodes=1, fetch_enabled=False))
        timers = [threading.Timer(delay, os.kill, (os.getpid(), signal.SIGTERM)) for delay in (0.5, 0.6)]
        for timer in timers:
            timer.start()
        started = time.monotonic()
        with self.assertLogs("dht_scraper", level="INFO") as logs:
            runtime.run(None)
        for timer in timers:
            timer.join()
        self.assertLess(time.monotonic() - started, 8.0)
        output = "\n".join(logs.output)
        self.assertEqual(output.count("terminated by signal"), 1)
        self.assertIn("finished: reason=terminate", output)
        self.assertTrue(wait_until(lambda: project_threads() == []))
        self.assertIs(signal.getsignal(signal.SIGTERM), previous_handler)

    def test_RUNTIME_006_fetched_torrent_indexed(self):
        path = os.path.join(temporary_directory(self), "torrents.sqlite3")
        raw, info_hash = build_info_dict(name=b"Indexed Torrent")

        async def fake_fetch(hash_value, peer):
            return raw

        runtime = self.make_runtime(settings(nodes=1, database_path=path), fetch_function=fake_fetch, indexer_options=FAST_INDEX)
        runtime.start()
        try:
            self.announce(runtime, info_hash)
            self.assertTrue(wait_until(lambda: self.catalog.snapshot_counts()["hashes_seen"] == 1))
            self.catalog.add_peers(info_hash, [("127.0.0.1", 6881)])
            reader = TorrentDatabase(path)
            self.assertTrue(wait_until(lambda: reader.get_document(info_hash.hex()) is not None))
            _, payload = self.get_json(runtime, "/api/search?q=indexed")
            self.assertEqual((payload["engine"], [record["info_hash"] for record in payload["results"]]), ("sqlite", [info_hash.hex()]))
            self.assertTrue(wait_until(lambda: self.get_json(runtime, "/api/stats")[1].get("index_documents", 0) >= 1))
            self.assertIn("index", project_threads())
        finally:
            runtime.stop()
        self.assertTrue(wait_until(lambda: project_threads() == []))

    def test_RUNTIME_007_database_survives_restart(self):
        path = os.path.join(temporary_directory(self), "torrents.sqlite3")
        info_hash = b"\x0d" * 20
        stored = TorrentMetadata(info_hash, "Persistent One", 1, 16384, 1, [TorrentFile("Persistent One", 1)], False, 1.0, None)
        first = ScraperRuntime(settings(nodes=1, fetch_enabled=False, database_path=path), self.catalog, never_fetch, BOOTSTRAP, browser_opener=self.opened.append, indexer_options=FAST_INDEX)
        first.start()
        try:
            first.catalog.store_metadata(stored)
            first.catalog.record_hash(info_hash, SOURCE_SAMPLE)
            first.catalog.record_hash(info_hash, SOURCE_SAMPLE)
        finally:
            first.stop()
        calls = []

        async def counting_fetch(hash_value, peer):
            calls.append(hash_value)
            raise MetadataFetchError("connect")

        second_catalog = TorrentCatalog()
        second = ScraperRuntime(settings(nodes=1, database_path=path), second_catalog, counting_fetch, BOOTSTRAP, browser_opener=self.opened.append, indexer_options=FAST_INDEX)
        second.start()
        try:
            _, payload = self.get_json(second, "/api/search?q=persistent")
            self.assertEqual(payload["engine"], "sqlite")
            self.assertEqual([(record["info_hash"], record["seen_count"]) for record in payload["results"]], [(info_hash.hex(), 2)])
            second_catalog.record_hash(info_hash, SOURCE_SAMPLE, ("127.0.0.1", 6881))
            done = lambda: self.get_json(second, "/api/torrent/" + info_hash.hex())[1]
            self.assertTrue(wait_until(lambda: done()["fetch_state"] == "done", timeout=2.0))
            self.assertEqual(done()["metadata"]["name"], "Persistent One")
            time.sleep(0.3)
            self.assertEqual(calls, [])
        finally:
            second.stop()
        reader = TorrentDatabase(path)
        self.assertEqual(reader.get_document(info_hash.hex())["seen_count"], 3)
        self.assertEqual(len(reader.search("", 10)), 1)

    def test_SEARCH_001_database_opened_at_start(self):
        with self.subTest(case="new"):
            path = os.path.join(temporary_directory(self), "torrents.sqlite3")
            runtime = self.make_runtime(settings(nodes=1, fetch_enabled=False, database_path=path))
            runtime.start()
            try:
                self.assertEqual(user_version(path), 1)
                connection = sqlite3.connect(path)
                tables = {row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")}
                connection.close()
                self.assertTrue({"torrents", "torrents_fts", "terms"} <= tables, tables)
                self.assertEqual(self.get_json(runtime, "/api/stats")[1]["index_documents"], 0)
            finally:
                runtime.stop()
            self.assertTrue(wait_until(lambda: project_threads() == []))
        with self.subTest(case="foreign"):
            path = os.path.join(temporary_directory(self), "foreign.sqlite3")
            connection = sqlite3.connect(path)
            connection.execute("PRAGMA user_version = 7")
            connection.close()
            with self.assertLogs("dht_scraper", level="INFO") as logs:
                code = main(["--database", path, "--no-browser", "--web-port", "0", "--port", "0", "--nodes", "1"])
            self.assertEqual(code, 1)
            errors = [line for line in logs.output if line.startswith("ERROR")]
            self.assertTrue(any("cannot start" in line and "schema version 7" in line for line in errors), logs.output)
            self.assertTrue(wait_until(lambda: project_threads() == []))
            self.assertEqual(user_version(path), 7)

if __name__ == "__main__":
    unittest.main()
