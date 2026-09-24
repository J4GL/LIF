"""Protocol category: KRPC builders and compact node encoding (F-005)."""
import unittest

from dht_scraper.krpc_messages import (
    build_error_response,
    build_find_node_query,
    build_find_node_response,
    build_get_peers_query,
    build_get_peers_response,
    build_ping_response,
    build_sample_infohashes_query,
    build_sample_infohashes_response,
    decode_compact_nodes,
    decode_compact_nodes6,
    decode_compact_peers,
    decode_samples,
    encode_compact_nodes,
    encode_compact_peers,
    is_routable_address,
    make_token,
    read_announced_port,
    read_error_code,
)

NODE_A = (b"a" * 20, "1.2.3.4", 6881)
NODE_B = (b"b" * 20, "10.0.0.1", 51413)


class CompactNodesTest(unittest.TestCase):
    def test_round_trip_f005_t2(self):
        encoded = encode_compact_nodes([NODE_A, NODE_B])
        self.assertEqual(len(encoded), 52)
        self.assertEqual(decode_compact_nodes(encoded), [NODE_A, NODE_B])

    def test_decode_ignores_trailing_bytes(self):
        encoded = encode_compact_nodes([NODE_A]) + b"xyz"
        self.assertEqual(decode_compact_nodes(encoded), [NODE_A])
        self.assertEqual(decode_compact_nodes(b""), [])
        self.assertEqual(decode_compact_nodes(b"short"), [])


class RoutableAddressTest(unittest.TestCase):
    def test_CRAWL_004_only_global_ipv4_is_routable(self):
        routable = [("81.2.3.4", 6881), ("1.1.1.1", 1)]
        rejected = [
            "0.1.2.3", "10.0.0.1", "100.64.0.1", "127.0.0.1", "169.254.1.1", "172.16.0.1", "172.31.255.255", "192.0.2.1",
            "192.168.1.1", "198.18.0.1", "198.51.100.1", "203.0.113.1", "224.0.0.1", "240.0.0.1", "255.255.255.255", "not.an.ip.x", "1.2.3",
        ]
        cases = routable + [(ip, 6881) for ip in rejected] + [("81.2.3.4", 0)]
        results = [(ip, port) for ip, port in cases if is_routable_address(ip, port)]
        self.assertEqual(results, routable)

    def test_rejects_special_addresses_and_ports(self):
        for ip, port in [("0.0.0.0", 6881), ("127.0.0.1", 6881), ("224.0.0.1", 6881), ("169.254.1.1", 6881), ("1.2.3.4", 0), ("1.2.3.4", 65536), ("not-an-ip", 6881), ("2001:db8::1", 6881)]:
            self.assertFalse(is_routable_address(ip, port), msg="%s:%d" % (ip, port))


class Ipv6FormatsTest(unittest.TestCase):
    def test_V6_001_ipv6_compact_formats_and_routing(self):
        node = (b"i" * 20, "2a01:e0a::1", 6881)
        encoded = encode_compact_nodes([node])
        self.assertEqual(len(encoded), 38)
        self.assertEqual(decode_compact_nodes6(encoded), [node])
        peer6, peer4 = ("2a01:e0a::2", 51413), ("81.2.3.4", 6881)
        values = encode_compact_peers([peer6, peer4])
        self.assertEqual([len(item) for item in values], [18, 6])
        self.assertEqual(decode_compact_peers(values), [peer6, peer4])
        candidates = ("2a01:e0a::1", "::1", "fe80::1", "fd00::1", "2001:db8::1", "ff0e::1", "::ffff:81.2.3.4", "::")
        self.assertEqual([ip for ip in candidates if is_routable_address(ip, 6881)], ["2a01:e0a::1"])


class MessageBuildersTest(unittest.TestCase):
    def test_find_node_query(self):
        message = build_find_node_query(b"aa", b"n" * 20, b"t" * 20)
        self.assertEqual(message[b"y"], b"q")
        self.assertEqual(message[b"q"], b"find_node")
        self.assertEqual(len(message[b"a"][b"id"]), 20)
        self.assertEqual(len(message[b"a"][b"target"]), 20)

    def test_responses(self):
        ping = build_ping_response(b"aa", b"n" * 20)
        self.assertEqual(ping[b"y"], b"r")
        self.assertEqual(ping[b"r"][b"id"], b"n" * 20)
        nodes = encode_compact_nodes([NODE_A])
        find_node = build_find_node_response(b"aa", b"n" * 20, nodes)
        self.assertEqual(find_node[b"r"][b"nodes"], nodes)
        get_peers = build_get_peers_response(b"aa", b"n" * 20, b"tok", nodes)
        self.assertEqual(set(get_peers[b"r"]), {b"id", b"token", b"nodes"})

    def test_error_response(self):
        message = build_error_response(b"aa", 204, b"Method Unknown")
        self.assertEqual(message[b"y"], b"e")
        self.assertEqual(message[b"e"], [204, b"Method Unknown"])

    def test_make_token_is_deterministic_and_ip_bound(self):
        secret = b"s" * 16
        self.assertEqual(len(make_token(secret, "1.2.3.4")), 8)
        self.assertEqual(make_token(secret, "1.2.3.4"), make_token(secret, "1.2.3.4"))
        self.assertNotEqual(make_token(secret, "1.2.3.4"), make_token(secret, "1.2.3.5"))
        self.assertNotEqual(make_token(secret, "1.2.3.4"), make_token(b"other", "1.2.3.4"))



class Bep51AndLookupMessagesTest(unittest.TestCase):
    def test_get_peers_and_sample_queries(self):
        query = build_get_peers_query(b"L123", b"n" * 20, b"h" * 20)
        self.assertEqual((query[b"y"], query[b"q"], query[b"a"][b"info_hash"]), (b"q", b"get_peers", b"h" * 20))
        sample = build_sample_infohashes_query(b"aa", b"n" * 20, b"t" * 20)
        self.assertEqual((sample[b"q"], sample[b"a"][b"target"]), (b"sample_infohashes", b"t" * 20))

    def test_sample_response_f002_t2(self):
        samples = b"a" * 20 + b"b" * 20
        response = build_sample_infohashes_response(b"aa", b"n" * 20, 300, encode_compact_nodes([NODE_A]), 2, samples)
        self.assertEqual(response[b"r"][b"samples"], samples)
        self.assertEqual(response[b"r"][b"interval"], 300)
        self.assertEqual(decode_samples(samples + b"xyz"), [b"a" * 20, b"b" * 20])
        self.assertEqual(decode_samples(None), [])

    def test_compact_peers(self):
        values = encode_compact_peers([("1.2.3.4", 6881), ("5.6.7.8", 1)])
        self.assertEqual(decode_compact_peers(values + [b"short", 7]), [("1.2.3.4", 6881), ("5.6.7.8", 1)])
        self.assertEqual(decode_compact_peers(b"not a list"), [])

    def test_read_error_code(self):
        self.assertEqual(read_error_code({b"y": b"e", b"e": [204, b"Method Unknown"]}), 204)
        self.assertIsNone(read_error_code({b"y": b"e", b"e": b"bad"}))
        self.assertIsNone(read_error_code({b"y": b"e", b"e": [True, b"x"]}))

    def test_read_announced_port(self):
        self.assertEqual(read_announced_port({b"port": 51413}, 6881), 51413)
        self.assertEqual(read_announced_port({b"port": 51413, b"implied_port": 1}, 6881), 6881)
        self.assertIsNone(read_announced_port({b"port": 0}, 6881))
        self.assertIsNone(read_announced_port({b"port": 70000}, 6881))
        self.assertIsNone(read_announced_port({}, 6881))


if __name__ == "__main__":
    unittest.main()
