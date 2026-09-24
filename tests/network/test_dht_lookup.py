"""Network category: continuous get_peers lookups driven by fake responses (F-004, LOOKUP-*)."""
import unittest

from dht_scraper.dht_lookup import LookupLimits, LookupManager
from dht_scraper.krpc_messages import encode_compact_nodes, encode_compact_peers
from tests.shared_fixtures import FakeClock

TARGET = b"\x00" * 20
DEFAULT = LookupLimits()


def node_at(distance_byte, index):
    return (bytes([distance_byte]) + bytes([index]) + b"\x00" * 18, "10.0.%d.%d" % (distance_byte, index), 1000 + index)


def nodes_at(distance_byte, count):
    return [node_at(distance_byte, index) for index in range(count)]


class Recorder:
    """A lookup manager whose callbacks record what they receive."""

    def __init__(self, clock, **limits):
        self.sent = []
        self.finished = []
        self.streamed = []
        self.manager = LookupManager(
            lambda transaction_id, node, info_hash: self.sent.append((transaction_id, node, info_hash)),
            lambda info_hash, peers: self.finished.append((info_hash, peers)),
            clock,
            on_peers=lambda info_hash, peers: self.streamed.append((info_hash, peers)),
            limits=LookupLimits(**limits),
        )

    def answer(self, index, fields):
        transaction_id, node, _ = self.sent[index]
        self.manager.handle_response(transaction_id, {b"id": b"n" * 20, **fields}, (node[1], node[2]))

    def fail(self, index):
        self.manager.handle_error(self.sent[index][0])

    def nodes_sent(self):
        return [node for _, node, _ in self.sent]


class LookupManagerTest(unittest.TestCase):
    def setUp(self):
        self.clock = FakeClock(100.0)
        self.recorder = Recorder(self.clock)
        self.manager = self.recorder.manager

    def test_RUNTIME_003_lookup_counts_tolerate_torn_reads(self):
        self.manager.started_total, self.manager.finished_total, self.manager.with_peers_total = 6, 7, 8
        counts = self.manager.snapshot_counts()
        self.assertEqual((counts["lookups_finished"], counts["lookups_with_peers"]), (7, 8))

    def test_start_sends_alpha_queries_to_closest_nodes(self):
        seeds = nodes_at(0xf0, 12) + [node_at(0x01, 0)]
        self.assertTrue(self.manager.start_lookup(TARGET, seeds, self.clock()))
        self.assertEqual(len(self.recorder.sent), DEFAULT.alpha)
        self.assertEqual(self.recorder.sent[0][1], node_at(0x01, 0))
        self.assertTrue(all(self.manager.owns_transaction(tid) for tid, _, _ in self.recorder.sent))
        self.assertFalse(self.manager.start_lookup(TARGET, seeds, self.clock()))
        self.assertFalse(self.manager.start_lookup(b"\x01" * 20, [], self.clock()))

    def test_values_from_the_last_node_finish_the_lookup(self):
        self.manager.start_lookup(TARGET, [node_at(0x10, 0)], self.clock())
        tid = self.recorder.sent[0][0]
        self.recorder.answer(0, {b"values": encode_compact_peers([("1.2.3.4", 6881), ("127.0.0.1", 1)])})
        self.assertEqual(self.recorder.finished, [(TARGET, [("1.2.3.4", 6881)])])
        self.assertFalse(self.manager.owns_transaction(tid))
        self.assertEqual(self.manager.active, {})

    def test_response_from_unexpected_sender_is_ignored(self):
        self.manager.start_lookup(TARGET, [node_at(0x10, 0)], self.clock())
        tid = self.recorder.sent[0][0]
        self.manager.handle_response(tid, {b"id": b"n" * 20, b"values": encode_compact_peers([("1.2.3.4", 6881)])}, ("9.9.9.9", 9))
        self.assertEqual(self.recorder.finished, [])
        self.assertTrue(self.manager.owns_transaction(tid))

    def test_error_and_abort(self):
        self.manager.start_lookup(TARGET, [node_at(0x80, 0)], self.clock())
        self.recorder.fail(0)
        self.assertEqual(self.recorder.finished, [(TARGET, [])])
        self.manager.start_lookup(b"\x05" * 20, [node_at(0x80, 1)], self.clock())
        self.manager.abort_all()
        self.assertEqual(self.recorder.finished[-1], (b"\x05" * 20, []))
        self.assertEqual(self.manager.transactions, {})

    def test_LOOKUP_002_lookup_widens_when_the_hint_has_no_values(self):
        seeds = nodes_at(0x40, 6)
        hint = node_at(0x80, 9)
        closer = [node_at(0x01, 0), node_at(0x02, 0)]
        with self.subTest(case="hint answers with closer nodes"):
            recorder = Recorder(self.clock, alpha=4)
            self.assertTrue(recorder.manager.start_lookup(TARGET, seeds, self.clock(), hint))
            self.assertEqual(recorder.nodes_sent(), [hint])
            recorder.answer(0, {b"nodes": encode_compact_nodes(closer)})
            self.assertEqual(recorder.nodes_sent()[1:], closer + seeds[:2])
        with self.subTest(case="hint times out"):
            recorder = Recorder(self.clock, alpha=4)
            recorder.manager.start_lookup(TARGET, seeds, self.clock(), hint)
            self.clock.now += DEFAULT.hint_timeout + 0.01
            recorder.manager.tick(self.clock())
            self.assertEqual(recorder.nodes_sent()[1:], seeds[:4])
            self.assertIn(TARGET, recorder.manager.active)

    def test_LOOKUP_003_alpha_queries_stay_in_flight(self):
        recorder = Recorder(self.clock, alpha=4)
        seeds = nodes_at(0x40, 10)
        recorder.manager.start_lookup(TARGET, seeds, self.clock())
        self.assertEqual(recorder.nodes_sent(), seeds[:4])
        recorder.answer(0, {b"nodes": b""})
        self.assertEqual(recorder.nodes_sent()[4:], [seeds[4]])
        self.assertEqual(len(recorder.manager.active[TARGET].pending), 4)
        recorder.fail(1)
        self.assertEqual(recorder.nodes_sent()[5:], [seeds[5]])
        self.clock.now += DEFAULT.query_timeout + 0.01
        recorder.manager.tick(self.clock())
        self.assertEqual(recorder.nodes_sent()[6:], seeds[6:10])
        self.assertEqual(len(recorder.manager.active[TARGET].pending), 4)

    def test_LOOKUP_004_peers_are_streamed_on_each_values_response(self):
        peer_1, peer_2, peer_3 = ("1.1.1.1", 1), ("2.2.2.2", 2), ("3.3.3.3", 3)
        self.manager.start_lookup(TARGET, nodes_at(0x40, 4), self.clock())
        self.recorder.answer(0, {b"values": encode_compact_peers([peer_1, peer_2])})
        self.assertEqual(self.recorder.streamed, [(TARGET, [peer_1, peer_2])])
        self.assertEqual(self.recorder.finished, [])
        self.assertIn(TARGET, self.manager.active)
        self.recorder.answer(1, {b"values": encode_compact_peers([peer_2, peer_3])})
        self.assertEqual(self.recorder.streamed[-1], (TARGET, [peer_3]))
        self.assertEqual(self.recorder.finished, [(TARGET, [peer_1, peer_2, peer_3])])
        self.assertNotIn(TARGET, self.manager.active)

    def test_LOOKUP_005_finish_conditions(self):
        eight = [("9.9.9.%d" % index, 1000 + index) for index in range(8)]
        with self.subTest(case="enough peers"):
            recorder = Recorder(self.clock)
            recorder.manager.start_lookup(TARGET, nodes_at(0x40, 4), self.clock())
            recorder.answer(0, {b"values": encode_compact_peers(eight)})
            self.assertEqual(recorder.finished, [(TARGET, sorted(eight))])
            self.assertEqual(recorder.manager.active, {})
        with self.subTest(case="converged"):
            recorder = Recorder(self.clock)
            recorder.manager.start_lookup(TARGET, nodes_at(0x40, 2), self.clock())
            recorder.fail(0)
            recorder.fail(1)
            self.assertEqual(recorder.finished, [(TARGET, [])])
        with self.subTest(case="query cap"):
            recorder = Recorder(self.clock)
            recorder.manager.start_lookup(TARGET, nodes_at(0x40, 64), self.clock())
            index = 0
            while TARGET in recorder.manager.active:
                recorder.fail(index)
                index += 1
            self.assertEqual(len(recorder.sent), DEFAULT.max_queries)
            self.assertEqual(recorder.finished, [(TARGET, [])])
        with self.subTest(case="deadline"):
            recorder = Recorder(self.clock)
            recorder.manager.start_lookup(TARGET, nodes_at(0x40, 4), self.clock())
            self.clock.now += DEFAULT.deadline + 0.01
            recorder.manager.tick(self.clock())
            self.assertEqual(recorder.finished, [(TARGET, [])])
            self.assertFalse(recorder.manager.owns_transaction(recorder.sent[0][0]))

    def test_LOOKUP_007_query_budget_and_per_node_cap(self):
        with self.subTest(case="token bucket"):
            recorder = Recorder(self.clock, queries_per_second=5, alpha=4)
            targets = [bytes([index + 1]) * 20 for index in range(3)]
            started = [recorder.manager.start_lookup(target, nodes_at(0x40 + index, 4), self.clock()) for index, target in enumerate(targets)]
            self.assertEqual(started, [True, True, False])
            self.assertEqual(len(recorder.sent), 5)
            self.assertEqual(len(recorder.manager.active), 2)
            self.clock.now += 0.4
            recorder.manager.tick(self.clock())
            self.assertEqual(len(recorder.sent), 7)
        with self.subTest(case="per-node cap"):
            recorder = Recorder(self.clock, max_per_node=2)
            hint = node_at(0x80, 1)
            seeds = nodes_at(0x40, 4)
            self.assertTrue(recorder.manager.start_lookup(b"\x01" * 20, seeds, self.clock(), hint))
            self.assertTrue(recorder.manager.start_lookup(b"\x02" * 20, seeds, self.clock(), hint))
            self.assertFalse(recorder.manager.start_lookup(b"\x03" * 20, seeds, self.clock(), hint))
            recorder.answer(0, {b"nodes": b""})
            self.assertTrue(recorder.manager.start_lookup(b"\x03" * 20, seeds, self.clock(), hint))
            self.assertEqual(recorder.sent[-1][1:], (hint, b"\x03" * 20))


    def test_LOOKUP_008_transaction_ids_are_unique_in_flight(self):
        recorder = Recorder(self.clock, max_active=1000, queries_per_second=100000, max_per_node=100000, alpha=4)
        for index in range(1000):
            recorder.manager.start_lookup(index.to_bytes(20, "big"), nodes_at(0x40, 4), self.clock())
        transaction_ids = [transaction_id for transaction_id, _, _ in recorder.sent]
        self.assertEqual(len(transaction_ids), 4000)
        self.assertEqual(len(set(transaction_ids)), 4000)
        self.assertTrue(all(transaction_id[:1] == b"L" and len(transaction_id) == 4 for transaction_id in transaction_ids))


if __name__ == "__main__":
    unittest.main()
