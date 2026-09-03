"""Network category: iterative get_peers lookup state machine driven by fake responses (F-004)."""
import unittest

from dht_scraper.dht_lookup import LOOKUP_ALPHA, LOOKUP_DEADLINE, LOOKUP_MAX_ROUNDS, LOOKUP_QUERY_TIMEOUT, LookupManager
from dht_scraper.krpc_messages import encode_compact_nodes, encode_compact_peers
from tests.shared_fixtures import FakeClock

TARGET = b"\x00" * 20


def node_at(distance_byte, index):
    return (bytes([distance_byte]) + bytes([index]) + b"\x00" * 18, "10.0.%d.%d" % (distance_byte, index), 1000 + index)


class LookupManagerTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock(100.0)
        self.sent = []
        self.finished = []
        self.manager = LookupManager(lambda tid, node, info_hash: self.sent.append((tid, node, info_hash)), lambda info_hash, peers: self.finished.append((info_hash, peers)), self.clock)

    def test_start_sends_first_round_to_closest_nodes(self):
        seeds = [node_at(0xf0, index) for index in range(12)] + [node_at(0x01, 0)]
        self.assertTrue(self.manager.start_lookup(TARGET, seeds, self.clock()))
        self.assertEqual(len(self.sent), LOOKUP_ALPHA)
        self.assertEqual(self.sent[0][1], node_at(0x01, 0))
        self.assertTrue(all(self.manager.owns_transaction(tid) for tid, _, _ in self.sent))
        self.assertFalse(self.manager.start_lookup(TARGET, seeds, self.clock()))
        self.assertFalse(self.manager.start_lookup(b"\x01" * 20, [], self.clock()))

    def test_values_finish_the_lookup(self):
        self.manager.start_lookup(TARGET, [node_at(0x10, 0)], self.clock())
        tid = self.sent[0][0]
        self.manager.handle_response(tid, {b"id": b"n" * 20, b"values": encode_compact_peers([("1.2.3.4", 6881), ("127.0.0.1", 1)])}, ("10.0.16.0", 1000))
        self.assertEqual(self.finished, [(TARGET, [("1.2.3.4", 6881)])])
        self.assertFalse(self.manager.owns_transaction(tid))
        self.assertEqual(self.manager.active, {})

    def test_nodes_lead_to_next_round_after_expiry(self):
        first = [node_at(0x80, index) for index in range(3)]
        self.manager.start_lookup(TARGET, first, self.clock())
        closer = node_at(0x02, 0)
        self.manager.handle_response(self.sent[0][0], {b"id": b"n" * 20, b"nodes": encode_compact_nodes([closer])}, ("10.0.128.0", 1000))
        self.assertEqual(len(self.sent), 3)
        self.clock.now += LOOKUP_QUERY_TIMEOUT + 0.1
        self.manager.tick(self.clock())
        self.assertEqual(self.sent[3][1], closer)
        self.assertEqual(self.manager.active[TARGET].rounds_sent, 2)

    def test_deadline_and_max_rounds_finish_without_peers(self):
        self.manager.start_lookup(TARGET, [node_at(0x80, 0)], self.clock())
        self.clock.now += LOOKUP_DEADLINE + 1
        self.manager.tick(self.clock())
        self.assertEqual(self.finished, [(TARGET, [])])
        self.finished.clear()
        many = [node_at(0x40, index) for index in range(64)]
        already_sent = len(self.sent)
        self.manager.start_lookup(TARGET, many, self.clock())
        handled = 0
        while TARGET in self.manager.active:
            transaction_id = self.sent[already_sent + handled][0]
            handled += 1
            self.manager.handle_error(transaction_id)
        self.assertEqual(handled, LOOKUP_ALPHA * LOOKUP_MAX_ROUNDS)
        self.assertEqual(self.finished, [(TARGET, [])])
        self.assertEqual(self.manager.finished_total, 2)

    def test_response_from_unexpected_sender_is_ignored(self):
        self.manager.start_lookup(TARGET, [node_at(0x10, 0)], self.clock())
        tid = self.sent[0][0]
        self.manager.handle_response(tid, {b"id": b"n" * 20, b"values": encode_compact_peers([("1.2.3.4", 6881)])}, ("9.9.9.9", 9))
        self.assertEqual(self.finished, [])
        self.assertTrue(self.manager.owns_transaction(tid))

    def test_error_and_abort(self):
        self.manager.start_lookup(TARGET, [node_at(0x80, 0)], self.clock())
        self.manager.handle_error(self.sent[0][0])
        self.assertEqual(self.finished, [(TARGET, [])])
        self.manager.start_lookup(b"\x05" * 20, [node_at(0x80, 1)], self.clock())
        self.manager.abort_all()
        self.assertEqual(self.finished[-1], (b"\x05" * 20, []))
        self.assertEqual(self.manager.transactions, {})


if __name__ == "__main__":
    unittest.main()
