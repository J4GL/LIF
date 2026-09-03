"""Network category: multi-node crawler with fake sockets and loopback sockets (F-001, F-002)."""
import socket
import unittest

from dht_scraper.bencode_codec import decode_bencode, encode_bencode
from dht_scraper.dht_crawler import DhtCrawler
from dht_scraper.dht_node_sockets import NodeSocket, close_node_sockets, create_node_sockets
from dht_scraper.krpc_messages import encode_compact_nodes, encode_compact_peers, make_token
from dht_scraper.node_identity import LOOKUP_TRANSACTION_ID_LENGTH, NEIGHBOR_PREFIX_LENGTH, generate_node_id
from dht_scraper.torrent_catalog import TorrentCatalog
from tests.shared_fixtures import FakeClock

PEER = ("10.0.0.1", 6881)
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
        self.assertEqual((detail["seen_count"], detail["announce_count"], detail["peers"]), (2, 2, [{"ip": "10.0.0.1", "port": 51413}]))
        replies = [message[b"y"] for message, _ in self.sent_on(0)]
        self.assertEqual(replies, [b"r", b"r", b"e"])
        self.assertEqual(self.sent_on(0)[0][0][b"r"][b"token"], token)
        self.assertEqual(self.crawler.stats["announced_peers"], 1)

    def test_implied_port_uses_source_port(self):
        token = make_token(self.crawler.token_secret, PEER[0])
        self.crawler.handle_datagram(make_query(b"announce_peer", {b"id": PEER_ID, b"info_hash": HASH_1, b"port": 9, b"implied_port": 1, b"token": token}), PEER, self.nodes[0])
        self.assertEqual(self.catalog.torrent_detail(HASH_1)["peers"], [{"ip": "10.0.0.1", "port": 6881}])

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
        lookup_query, address = [item for item in self.sent_on(1) if item[0][b"q"] == b"get_peers"][0]
        self.assertEqual(address, ("3.3.3.3", 3333))
        self.assertEqual(lookup_query[b"a"][b"info_hash"], HASH_1)
        transaction_id = lookup_query[b"t"]
        self.assertEqual(len(transaction_id), LOOKUP_TRANSACTION_ID_LENGTH)
        self.crawler.handle_datagram(make_response({b"id": b"n" * 20, b"values": encode_compact_peers([("4.4.4.4", 4444)])}, transaction_id), address, self.nodes[0])
        self.assertEqual(self.catalog.torrent_detail(HASH_1)["peers"], [{"ip": "4.4.4.4", "port": 4444}])
        self.assertEqual(self.crawler.lookup_manager.active, {})
        self.assertEqual(self.crawler.start_pending_lookups(self.clock()), 0)

    def test_bootstrap_sends_from_every_node_and_caches_dns(self):
        crawler = DhtCrawler(self.nodes, self.catalog, [("127.0.0.1", 6881), ("no-such-host.invalid", 6881)], clock=self.clock)
        crawler.bootstrap()
        crawler.bootstrap()
        self.assertEqual(crawler.resolved_bootstrap_hosts, {"127.0.0.1": "127.0.0.1"})
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
        finally:
            sender.close()
            close_node_sockets(nodes)


if __name__ == "__main__":
    unittest.main()
