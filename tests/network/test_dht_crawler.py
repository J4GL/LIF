"""Network category: multi-node crawler with fake sockets and loopback sockets (F-001, F-002)."""
import socket
import unittest

from dht_scraper.fetch_engine import raise_open_file_limit

from dht_scraper.bencode_codec import decode_bencode, encode_bencode
from dht_scraper.dht_crawler import DhtCrawler, next_batch_time
from dht_scraper.dht_node_sockets import NodeSocket, close_node_sockets, create_node_sockets, create_udp_socket
from dht_scraper.krpc_messages import decode_compact_nodes6, encode_compact_nodes, encode_compact_peers, make_token
from dht_scraper.node_identity import LOOKUP_TRANSACTION_ID_LENGTH, NEIGHBOR_PREFIX_LENGTH, generate_node_id
from dht_scraper.torrent_catalog import SOURCE_SAMPLE, TorrentCatalog
from tests.shared_fixtures import FakeClock

PEER = ("81.0.0.1", 6881)
PEER_ID = b"p" * 20
HASH_1 = b"\x11" * 20
HASH_2 = b"\x22" * 20
BOOTSTRAP = [("127.0.0.1", 6881)]


class FakeSocket:
    def __init__(self):
        self.sent = []

    def sendto(self, payload, address):
        self.sent.append((decode_bencode(payload), address))
        return len(payload)


def make_query(name, args, transaction_id=b"tt"):
    return encode_bencode({b"t": transaction_id, b"y": b"q", b"q": name, b"a": args})


def make_response(fields, transaction_id=b"tt"):
    return encode_bencode({b"t": transaction_id, b"y": b"r", b"r": fields})


class CrawlerHandlerTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()
        self.clock = FakeClock()
        self.nodes = [NodeSocket(FakeSocket(), generate_node_id(), 6881), NodeSocket(FakeSocket(), generate_node_id(), 6882)]
        self.crawler = DhtCrawler(self.nodes, self.catalog, BOOTSTRAP, batch_size=2, interval=0.1, max_queue=10, clock=self.clock)

    def sent_on(self, index):
        return self.nodes[index].udp_socket.sent

    def test_queries_are_answered_on_the_receiving_node_f001_t1(self):
        self.crawler.handle_datagram(make_query(b"ping", {b"id": PEER_ID}), PEER, self.nodes[1])
        self.assertEqual(self.sent_on(0), [])
        reply, address = self.sent_on(1)[0]
        self.assertEqual((reply[b"y"], address), (b"r", PEER))
        self.assertEqual(reply[b"r"][b"id"], PEER_ID[:NEIGHBOR_PREFIX_LENGTH] + self.nodes[1].own_id[NEIGHBOR_PREFIX_LENGTH:])

    def test_get_peers_and_announce_record_hashes_and_peers(self):
        self.crawler.handle_datagram(make_query(b"get_peers", {b"id": PEER_ID, b"info_hash": HASH_1}), PEER, self.nodes[0])
        token = make_token(self.crawler.token_secret, PEER[0])
        self.crawler.handle_datagram(make_query(b"announce_peer", {b"id": PEER_ID, b"info_hash": HASH_2, b"port": 51413, b"token": token}), PEER, self.nodes[0])
        self.crawler.handle_datagram(make_query(b"announce_peer", {b"id": PEER_ID, b"info_hash": HASH_2, b"port": 1, b"token": b"bad"}), PEER, self.nodes[0])
        self.assertEqual(self.catalog.snapshot_counts()["hashes_seen"], 2)
        detail = self.catalog.torrent_detail(HASH_2)
        self.assertEqual((detail["seen_count"], detail["announce_count"], detail["peers"]), (2, 2, [{"ip": "81.0.0.1", "port": 51413}]))
        replies = [message[b"y"] for message, _ in self.sent_on(0)]
        self.assertEqual(replies, [b"r", b"r", b"e"])
        self.assertEqual(self.sent_on(0)[0][0][b"r"][b"token"], token)
        self.assertEqual(self.crawler.stats["announced_peers"], 1)

    def test_implied_port_uses_source_port(self):
        token = make_token(self.crawler.token_secret, PEER[0])
        self.crawler.handle_datagram(make_query(b"announce_peer", {b"id": PEER_ID, b"info_hash": HASH_1, b"port": 9, b"implied_port": 1, b"token": token}), PEER, self.nodes[0])
        self.assertEqual(self.catalog.torrent_detail(HASH_1)["peers"], [{"ip": "81.0.0.1", "port": 6881}])

    def test_invalid_info_hash_and_unknown_query(self):
        self.crawler.handle_datagram(make_query(b"get_peers", {b"id": PEER_ID, b"info_hash": b"short"}), PEER, self.nodes[0])
        self.crawler.handle_datagram(make_query(b"vote", {b"id": PEER_ID}), PEER, self.nodes[0])
        self.assertEqual([message[b"e"][0] for message, _ in self.sent_on(0)], [203, 204])
        self.assertEqual(self.catalog.snapshot_counts()["hashes_seen"], 0)

    def test_sample_infohashes_response_records_hashes_f002_t1(self):
        nodes = [(b"a" * 20, "1.2.3.4", 1000), (b"b" * 20, "5.6.7.8", 2000)]
        response = make_response({b"id": PEER_ID, b"interval": 300, b"num": 3, b"nodes": encode_compact_nodes(nodes), b"samples": HASH_1 + HASH_2 + b"\x33" * 20 + b"trailing"})
        self.crawler.handle_datagram(response, PEER, self.nodes[0])
        self.assertEqual(self.catalog.snapshot_counts()["hashes_seen"], 3)
        self.assertEqual(self.crawler.stats["samples_received"], 3)
        self.assertEqual(list(self.crawler.node_queue), nodes)
        self.assertEqual(self.crawler.sample_backoff[PEER], self.clock.now + 300)

    def test_crawl_step_sends_sample_queries_round_robin_and_honours_backoff(self):
        for index in range(4):
            self.crawler.add_node((bytes([index + 1]) * 20, "1.2.3.%d" % index, 1000 + index))
        self.crawler.sample_backoff[("1.2.3.1", 1001)] = self.clock.now + 100
        self.assertEqual(self.crawler.crawl_step(), 4)
        queries = [message[b"q"] for message, _ in self.sent_on(0)] + [message[b"q"] for message, _ in self.sent_on(1)]
        self.assertEqual(len(self.sent_on(0)), 2)
        self.assertEqual(len(self.sent_on(1)), 2)
        self.assertEqual(sorted(queries), [b"find_node", b"sample_infohashes", b"sample_infohashes", b"sample_infohashes"])
        first_query, first_address = self.sent_on(0)[0]
        self.assertEqual(first_address, ("1.2.3.0", 1000))
        self.assertEqual(first_query[b"a"][b"id"][:NEIGHBOR_PREFIX_LENGTH], (b"\x01" * 20)[:NEIGHBOR_PREFIX_LENGTH])
        self.assertEqual(self.crawler.stats["sample_queries_sent"], 3)

    def test_error_204_triggers_one_find_node_fallback(self):
        error = encode_bencode({b"t": b"tt", b"y": b"e", b"e": [204, b"Method Unknown"]})
        self.crawler.handle_datagram(error, PEER, self.nodes[1])
        self.crawler.handle_datagram(error, PEER, self.nodes[1])
        self.assertEqual([message[b"q"] for message, _ in self.sent_on(1)], [b"find_node"])
        self.assertEqual(self.crawler.stats["errors_received"], 2)
        self.assertGreater(self.crawler.sample_backoff[PEER], self.clock.now)

    def test_incoming_sample_infohashes_query_is_answered_with_recent_hashes(self):
        self.crawler.handle_datagram(make_query(b"get_peers", {b"id": PEER_ID, b"info_hash": HASH_1}), PEER, self.nodes[0])
        self.crawler.handle_datagram(make_query(b"sample_infohashes", {b"id": PEER_ID, b"target": HASH_2}), PEER, self.nodes[0])
        reply = self.sent_on(0)[1][0]
        self.assertEqual((reply[b"r"][b"samples"], reply[b"r"][b"num"], reply[b"r"][b"interval"]), (HASH_1, 1, 300))

    def test_self_and_unroutable_nodes_are_filtered(self):
        self_like = b"z" * NEIGHBOR_PREFIX_LENGTH + self.nodes[1].own_id[NEIGHBOR_PREFIX_LENGTH:]
        nodes = [(self_like, "1.2.3.4", 1000), (b"c" * 20, "127.0.0.1", 3000), (b"d" * 20, "9.9.9.9", 0), (b"e" * 20, "2.2.2.2", 2)]
        self.crawler.handle_datagram(make_response({b"id": self_like, b"nodes": encode_compact_nodes(nodes)}), ("9.9.9.9", 6881), self.nodes[0])
        self.crawler.handle_datagram(make_response({b"id": PEER_ID}), ("2001:db8::1", 6881), self.nodes[0])
        self.assertEqual(list(self.crawler.node_queue), [nodes[3]])
        self.assertEqual(list(self.crawler.recent_nodes), [nodes[3]])

    def test_malformed_datagrams_are_ignored(self):
        for data in [b"", b"garbage", b"l" * 5000, encode_bencode({b"y": b"q"}), encode_bencode({b"y": b"r", b"r": b"x"}), encode_bencode({b"y": b"e"})]:
            self.crawler.handle_datagram(data, PEER, self.nodes[0])
        self.assertEqual(self.crawler.stats["packets_received"], 6)
        self.assertEqual(self.sent_on(0), [])

    def test_lookup_is_started_for_hashes_without_peers_and_routed_by_transaction_id(self):
        self.crawler.handle_datagram(make_query(b"get_peers", {b"id": PEER_ID, b"info_hash": HASH_1}), PEER, self.nodes[0])
        self.crawler.add_node((b"n" * 20, "3.3.3.3", 3333))
        self.assertEqual(self.crawler.start_pending_lookups(self.clock()), 1)
        _, lookup_query, address = self.lookup_queries()[0]
        self.assertEqual(lookup_query[b"a"][b"info_hash"], HASH_1)
        transaction_id = lookup_query[b"t"]
        self.assertEqual(len(transaction_id), LOOKUP_TRANSACTION_ID_LENGTH)
        self.crawler.handle_datagram(make_response({b"id": b"n" * 20, b"values": encode_compact_peers([("4.4.4.4", 4444)])}, transaction_id), address, self.nodes[0])
        self.assertEqual(self.catalog.torrent_detail(HASH_1)["peers"], [{"ip": "4.4.4.4", "port": 4444}])
        self.assertEqual(self.crawler.start_pending_lookups(self.clock()), 0)
        for message, _ in self.sent_on(0) + self.sent_on(1):
            if message.get(b"q") == b"get_peers" and self.crawler.lookup_manager.owns_transaction(message[b"t"]):
                self.crawler.handle_datagram(encode_bencode({b"t": message[b"t"], b"y": b"e", b"e": [201, b"x"]}), PEER, self.nodes[0])
        self.assertEqual(self.crawler.lookup_manager.active, {})

    def lookup_queries(self):
        return [(index, message, address) for index in (0, 1) for message, address in self.sent_on(index) if message.get(b"q") == b"get_peers"]

    def test_LOOKUP_001_sampled_hash_is_asked_to_its_sampling_node_first(self):
        for index in range(3):
            self.crawler.add_node((bytes([0x30 + index]) * 20, "3.3.3.%d" % index, 3000 + index))
        sampler_id = b"s" * 20
        sample = make_response({b"id": sampler_id, b"interval": 300, b"num": 1, b"samples": HASH_1})
        self.crawler.handle_datagram(sample, ("5.5.5.5", 5555), self.nodes[1])
        self.assertEqual(self.crawler.start_pending_lookups(self.clock()), 1)
        queries = self.lookup_queries()
        self.assertEqual(len(queries), 1)
        socket_index, message, address = queries[0]
        self.assertEqual((socket_index, message[b"a"][b"info_hash"], address), (1, HASH_1, ("5.5.5.5", 5555)))
        reply = make_response({b"id": sampler_id, b"values": encode_compact_peers([("8.8.8.8", 8888)])}, message[b"t"])
        self.crawler.handle_datagram(reply, ("5.5.5.5", 5555), self.nodes[1])
        self.assertEqual(self.catalog.torrent_detail(HASH_1)["peers"], [{"ip": "8.8.8.8", "port": 8888}])

    def test_V6_007_ipv4_only_lookup_ignores_ipv6_nodes(self):
        sampler_id = b"s" * 20
        self.crawler.handle_datagram(make_response({b"id": sampler_id, b"interval": 300, b"num": 1, b"samples": HASH_1}), ("5.5.5.5", 5555), self.nodes[0])
        self.assertEqual(self.crawler.start_pending_lookups(self.clock()), 1)
        hint_query = self.lookup_queries()[0][1]
        closest_v6 = (HASH_1, "2a01:e0a::11", 7001)
        ipv4_node = (b"\x10" * 20, "3.3.3.3", 3333)
        reply = make_response({b"id": sampler_id, b"nodes": encode_compact_nodes([ipv4_node]), b"nodes6": encode_compact_nodes([closest_v6])}, hint_query[b"t"])
        self.crawler.handle_datagram(reply, ("5.5.5.5", 5555), self.nodes[0])
        self.assertEqual([address for _, _, address in self.lookup_queries()], [("5.5.5.5", 5555), ("3.3.3.3", 3333)])
        self.assertEqual(self.crawler.snapshot_stats()["lookup_queries_sent"], 2)

    def test_LOOKUP_006_fetchable_backlog_pauses_new_lookups(self):
        crawler = DhtCrawler(self.nodes, self.catalog, BOOTSTRAP, batch_size=2, interval=0.1, max_queue=10, clock=self.clock, lookup_fetchable_target=2)
        for index in range(2):
            self.catalog.record_hash(bytes([0x50 + index]) * 20, SOURCE_SAMPLE, peer=("6.6.6.%d" % index, 6000 + index))
        for index in range(3):
            self.catalog.record_hash(bytes([0x60 + index]) * 20, SOURCE_SAMPLE)
        crawler.add_node((b"n" * 20, "3.3.3.3", 3333))
        self.assertEqual(crawler.start_pending_lookups(self.clock()), 0)
        self.catalog.next_fetch_candidates(1)
        self.assertEqual(crawler.start_pending_lookups(self.clock()), 1)

    def test_bootstrap_sends_from_every_node_and_caches_dns(self):
        crawler = DhtCrawler(self.nodes, self.catalog, [("127.0.0.1", 6881), ("no-such-host.invalid", 6881)], clock=self.clock)
        crawler.bootstrap()
        crawler.bootstrap()
        self.assertEqual(crawler.resolved_bootstrap_hosts[socket.AF_INET], {"127.0.0.1": ["127.0.0.1"], "no-such-host.invalid": []})
        self.assertEqual(len(self.sent_on(0)), 2)
        self.assertEqual(len(self.sent_on(1)), 2)
        self.assertEqual(self.sent_on(0)[0][0][b"a"][b"id"], self.nodes[0].own_id)

    def test_snapshot_stats_queue_bound_and_dedup(self):
        for index in range(20):
            self.crawler.add_node((bytes([index + 1]) * 20, "1.2.3.4", 1000 + index))
        self.crawler.add_node((b"x" * 20, "1.2.3.4", 1019))
        stats = self.crawler.snapshot_stats()
        self.assertEqual((stats["queue_size"], stats["active_lookups"]), (10, 0))
        self.assertEqual(self.crawler.node_queue[-1][0], bytes([20]) * 20)


class CrawlerPolicySpecTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()
        self.clock = FakeClock()
        self.nodes = [NodeSocket(FakeSocket(), generate_node_id(), 6881), NodeSocket(FakeSocket(), generate_node_id(), 6882)]
        self.crawler = DhtCrawler(self.nodes, self.catalog, BOOTSTRAP, batch_size=2, interval=0.1, max_queue=10, clock=self.clock)

    def sent_on(self, index):
        return self.nodes[index].udp_socket.sent

    def test_CRAWL_001_get_peers_reply_id_is_next_to_the_info_hash(self):
        own_suffix = self.nodes[1].own_id[NEIGHBOR_PREFIX_LENGTH:]
        self.crawler.handle_datagram(make_query(b"get_peers", {b"id": PEER_ID, b"info_hash": HASH_1}), PEER, self.nodes[1])
        reply = self.sent_on(1)[-1][0]
        self.assertEqual(reply[b"r"][b"id"], HASH_1[:NEIGHBOR_PREFIX_LENGTH] + own_suffix)
        announce = {b"id": PEER_ID, b"info_hash": HASH_1, b"port": 51413, b"token": reply[b"r"][b"token"]}
        self.crawler.handle_datagram(make_query(b"announce_peer", announce), PEER, self.nodes[1])
        self.assertEqual(self.sent_on(1)[-1][0][b"r"][b"id"], HASH_1[:NEIGHBOR_PREFIX_LENGTH] + own_suffix)
        self.crawler.handle_datagram(make_query(b"ping", {b"id": PEER_ID}), PEER, self.nodes[1])
        self.assertEqual(self.sent_on(1)[-1][0][b"r"][b"id"], PEER_ID[:NEIGHBOR_PREFIX_LENGTH] + own_suffix)

    def test_CRAWL_003_error_204_fallback_only_when_nodes_are_needed(self):
        error = encode_bencode({b"t": b"tt", b"y": b"e", b"e": [204, b"Method Unknown"]})
        with self.subTest(case="queue half full"):
            for index in range(5):
                self.crawler.add_node((bytes([index + 1]) * 20, "1.2.3.%d" % index, 1000 + index))
            self.crawler.handle_datagram(error, ("81.0.0.1", 6881), self.nodes[0])
            self.assertEqual([message.get(b"q") for message, _ in self.sent_on(0)], [])
            self.assertGreater(self.crawler.sample_backoff[("81.0.0.1", 6881)], self.clock.now)
        with self.subTest(case="queue needs nodes"):
            self.crawler.node_queue.popleft()
            self.crawler.handle_datagram(error, ("81.0.0.2", 6881), self.nodes[0])
            self.assertEqual([(message.get(b"q"), address) for message, address in self.sent_on(0)], [(b"find_node", ("81.0.0.2", 6881))])

    def test_CRAWL_005_udp_buffers_are_enlarged(self):
        udp_socket = create_udp_socket(0)
        try:
            self.assertGreaterEqual(udp_socket.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF), 4 * 1024 * 1024)
            self.assertGreaterEqual(udp_socket.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF), 1024 * 1024)
        finally:
            udp_socket.close()

    def test_CRAWL_006_batches_do_not_drift(self):
        self.assertAlmostEqual(next_batch_time(10.0, 10.03, 0.1), 10.1)
        self.assertAlmostEqual(next_batch_time(10.1, 10.35, 0.1), 10.45)

    def test_CRAWL_007_high_descriptors_are_served(self):
        self.assertGreaterEqual(raise_open_file_limit(2048), 2048)
        dummies = [socket.socket(socket.AF_INET, socket.SOCK_DGRAM) for _ in range(1100)]
        nodes = []
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            nodes = create_node_sockets(0, 1)
            self.assertGreater(nodes[0].udp_socket.fileno(), 1023)
            crawler = DhtCrawler(nodes, TorrentCatalog(), BOOTSTRAP)
            sender.settimeout(2.0)
            sender.sendto(make_query(b"ping", {b"id": PEER_ID}), ("127.0.0.1", nodes[0].port))
            self.assertEqual(crawler.receive_pending(2.0), 1)
            self.assertEqual(decode_bencode(sender.recvfrom(65535)[0])[b"y"], b"r")
            crawler.close()
        finally:
            sender.close()
            close_node_sockets(nodes)
            for dummy in dummies:
                dummy.close()

    def test_CRAWL_009_bootstrap_reaches_every_router_address(self):
        lookups = []

        def resolver(host, port, family):
            lookups.append(host)
            if host == "dead.example":
                raise OSError("no such host")
            return ["81.0.0.1", "81.0.0.2"]

        crawler = DhtCrawler(self.nodes, self.catalog, [("multi.example", 6881), ("dead.example", 6881)], clock=self.clock, resolver=resolver)
        crawler.bootstrap()
        crawler.bootstrap()
        sent = [(message[b"q"], address) for index in (0, 1) for message, address in self.sent_on(index)]
        self.assertEqual(sorted(sent), sorted([(b"find_node", ("81.0.0.1", 6881)), (b"find_node", ("81.0.0.2", 6881))] * 4))
        self.assertEqual(sorted(lookups), ["dead.example", "multi.example"])
        self.clock.now += 1.9
        self.assertEqual(crawler.crawl_step(), 0)
        self.assertEqual(len(self.sent_on(0)) + len(self.sent_on(1)), 8)
        self.clock.now += 0.1
        crawler.crawl_step()
        self.assertEqual(len(self.sent_on(0)) + len(self.sent_on(1)), 12)

    def test_CRAWL_008_per_address_send_cap(self):
        reply = {b"t": b"tt", b"y": b"r", b"r": {b"id": b"x" * 20}}
        results = [self.crawler.send_message(self.nodes[0], reply, ("81.0.0.1", 1 + index % 2)) for index in range(45)]
        self.assertEqual((results.count(True), results[-5:]), (40, [False] * 5))
        self.assertEqual((len(self.sent_on(0)), self.crawler.stats["packets_throttled"]), (40, 5))
        self.clock.now += 10.0
        self.assertTrue(self.crawler.send_message(self.nodes[0], reply, ("81.0.0.1", 1)))


class Ipv6CrawlerSpecTest(unittest.TestCase):
    def setUp(self):
        self.catalog = TorrentCatalog()
        self.clock = FakeClock()
        self.nodes = [NodeSocket(FakeSocket(), generate_node_id(), 6881), NodeSocket(FakeSocket(), generate_node_id(), 6881, socket.AF_INET6)]
        self.crawler = DhtCrawler(self.nodes, self.catalog, BOOTSTRAP, batch_size=2, interval=0.1, max_queue=10, clock=self.clock)
        self.v6_nodes = [(b"\x61" * 20, "2a01:e0a::11", 7001), (b"\x62" * 20, "2a01:e0a::12", 7002)]
        self.sampler = ("2a01:e0a::9", 6881)
        sample = make_response({b"id": b"s" * 20, b"interval": 300, b"num": 1, b"samples": HASH_1, b"nodes6": encode_compact_nodes(self.v6_nodes)})
        self.crawler.handle_datagram(sample, self.sampler, self.nodes[1])

    def sent(self, index):
        return self.nodes[index].udp_socket.sent

    def test_V6_002_ipv6_crawl_and_replies(self):
        self.assertIsNotNone(self.catalog.torrent_detail(HASH_1))
        self.crawler.crawl_step()
        self.assertEqual(sorted((message[b"q"], address) for message, address in self.sent(1)), sorted((b"sample_infohashes", (node[1], node[2])) for node in self.v6_nodes))
        self.assertTrue(all(":" not in address[0] for _, address in self.sent(0)))
        self.crawler.handle_datagram(make_query(b"find_node", {b"id": PEER_ID, b"target": HASH_2}), ("2a01:e0a::5", 6881), self.nodes[1])
        reply, address = self.sent(1)[-1]
        self.assertEqual(address, ("2a01:e0a::5", 6881))
        self.assertNotIn(b"nodes", reply[b"r"])
        self.assertTrue(set(self.v6_nodes) <= set(decode_compact_nodes6(reply[b"r"][b"nodes6"])))

    def test_V6_003_ipv6_lookup(self):
        self.assertEqual(self.crawler.start_pending_lookups(self.clock()), 1)
        queries = [(message, address) for message, address in self.sent(1) if message.get(b"q") == b"get_peers"]
        self.assertEqual([address for _, address in queries], [self.sampler])
        reply = make_response({b"id": b"s" * 20, b"values": encode_compact_peers([("2a01:e0a::7", 51413)])}, queries[0][0][b"t"])
        self.crawler.handle_datagram(reply, self.sampler, self.nodes[1])
        self.assertEqual(self.catalog.torrent_detail(HASH_1)["peers"], [{"ip": "2a01:e0a::7", "port": 51413}])

    def test_V6_004_ipv6_bootstrap(self):
        def resolver(host, port, family):
            return ["2a01:e0a::1"] if family == socket.AF_INET6 else ["81.0.0.1"]

        catalog = TorrentCatalog()
        nodes = [NodeSocket(FakeSocket(), generate_node_id(), 6881), NodeSocket(FakeSocket(), generate_node_id(), 6881, socket.AF_INET6)]
        crawler = DhtCrawler(nodes, catalog, [("router.example", 6881)], clock=self.clock, resolver=resolver)
        crawler.bootstrap()
        self.assertEqual([address for _, address in nodes[0].udp_socket.sent], [("81.0.0.1", 6881)])
        self.assertEqual([address for _, address in nodes[1].udp_socket.sent], [("2a01:e0a::1", 6881)])


class LoopbackNodesTest(unittest.TestCase):
    def test_three_nodes_answer_on_their_own_ports(self):
        nodes = create_node_sockets(0, 3)
        sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sender.settimeout(2.0)
        catalog = TorrentCatalog()
        try:
            self.assertEqual(len({node.port for node in nodes}), 3)
            crawler = DhtCrawler(nodes, catalog, BOOTSTRAP)
            for node in nodes:
                sender.sendto(make_query(b"ping", {b"id": PEER_ID}), ("127.0.0.1", node.port))
            handled = 0
            while handled < 3:
                got = crawler.receive_pending(2.0)
                self.assertGreater(got, 0)
                handled += got
            reply_ports = set()
            for _ in range(3):
                data, address = sender.recvfrom(65535)
                self.assertEqual(decode_bencode(data)[b"y"], b"r")
                reply_ports.add(address[1])
            self.assertEqual(reply_ports, {node.port for node in nodes})
            self.assertEqual(crawler.receive_pending(0.0), 0)
            crawler.close()
        finally:
            sender.close()
            close_node_sockets(nodes)


if __name__ == "__main__":
    unittest.main()
