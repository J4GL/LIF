"""Protocol category: node identity helpers."""
import unittest

from dht_scraper.node_identity import (
    generate_lookup_transaction_id,
    generate_node_id,
    generate_peer_id,
    generate_transaction_id,
    is_valid_node_id,
    neighbor_node_id,
    select_closest_nodes,
    xor_distance,
)


class NodeIdentityTest(unittest.TestCase):
    def test_generate_lengths(self):
        self.assertEqual(len(generate_node_id()), 20)
        self.assertEqual(len(generate_transaction_id()), 2)
        self.assertNotEqual(generate_node_id(), generate_node_id())

    def test_neighbor_node_id(self):
        remote = b"r" * 20
        own = b"o" * 20
        result = neighbor_node_id(remote, own)
        self.assertEqual(result, b"r" * 15 + b"o" * 5)

    def test_is_valid_node_id(self):
        self.assertTrue(is_valid_node_id(b"x" * 20))
        self.assertFalse(is_valid_node_id(b"x" * 19))
        self.assertFalse(is_valid_node_id("x" * 20))
        self.assertFalse(is_valid_node_id(None))



class LookupIdentityTest(unittest.TestCase):
    def test_xor_distance(self):
        self.assertEqual(xor_distance(b"a" * 20, b"a" * 20), 0)
        self.assertEqual(xor_distance(b"\x00" * 20, b"\x00" * 19 + b"\x01"), 1)
        self.assertEqual(xor_distance(b"a" * 20, b"b" * 20), xor_distance(b"b" * 20, b"a" * 20))

    def test_select_closest_nodes(self):
        target = b"\x00" * 20
        far = (b"\xff" * 20, "1.1.1.1", 1)
        near = (b"\x00" * 19 + b"\x01", "2.2.2.2", 2)
        middle = (b"\x00" * 19 + b"\x10", "3.3.3.3", 3)
        self.assertEqual(select_closest_nodes([far, near, middle], target, 2), [near, middle])
        self.assertEqual(select_closest_nodes([], target, 5), [])

    def test_lookup_transaction_and_peer_id(self):
        transaction_id = generate_lookup_transaction_id()
        self.assertEqual(len(transaction_id), 4)
        self.assertEqual(transaction_id[:1], b"L")
        peer_id = generate_peer_id()
        self.assertEqual(len(peer_id), 20)
        self.assertTrue(peer_id.startswith(b"-DS0002-"))


if __name__ == "__main__":
    unittest.main()
