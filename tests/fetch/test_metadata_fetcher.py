"""Fetch category: BEP 9/10 metadata download against a loopback fake peer (F-004)."""
import socket
import threading
import unittest

from dht_scraper.metadata_fetcher import FetchTimeouts, MetadataFetchError, fetch_metadata
from tests.fetch.fake_metadata_peer import FakeMetadataPeer, build_info_dict

FAST = FetchTimeouts(connect=1.0, session=2.0)


class MetadataFetcherTest(unittest.TestCase):
    def test_fetches_single_piece_metadata_f004_t1(self):
        raw, info_hash = build_info_dict()
        peer = FakeMetadataPeer(info_hash, raw, "ok")
        try:
            self.assertEqual(fetch_metadata(info_hash, peer.address, None, FAST), raw)
            self.assertEqual(peer.requests, [0])
        finally:
            peer.close()

    def test_fetches_multi_piece_metadata_in_order(self):
        raw, info_hash = build_info_dict(extra_pieces=2000)
        self.assertGreater(len(raw), 2 * 16384)
        peer = FakeMetadataPeer(info_hash, raw, "ok", their_id=7, send_noise=False)
        try:
            self.assertEqual(fetch_metadata(info_hash, peer.address, None, FAST), raw)
            self.assertEqual(peer.requests, [0, 1, 2])
        finally:
            peer.close()

    def test_failure_modes_map_to_reasons(self):
        expected = {
            "no_extensions": "no_extensions",
            "no_ut_metadata": "no_ut_metadata",
            "too_large": "too_large",
            "reject": "reject",
            "corrupt": "sha1_mismatch",
            "close_after_handshake": "closed",
            "wrong_hash": "hash_mismatch",
            "huge_frame": "frame_too_large",
            "silent": "timeout",
        }
        for mode, reason in expected.items():
            raw, info_hash = build_info_dict()
            peer = FakeMetadataPeer(info_hash, raw, mode)
            try:
                with self.assertRaises(MetadataFetchError, msg=mode) as context:
                    fetch_metadata(info_hash, peer.address, None, FAST)
                self.assertEqual(context.exception.reason, reason, mode)
            finally:
                peer.close()

    def test_connection_refused_and_stop_event(self):
        raw, info_hash = build_info_dict()
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
        probe.close()
        with self.assertRaises(MetadataFetchError) as context:
            fetch_metadata(info_hash, ("127.0.0.1", closed_port), None, FAST)
        self.assertEqual(context.exception.reason, "connect")
        stop_event = threading.Event()
        stop_event.set()
        peer = FakeMetadataPeer(info_hash, raw, "silent")
        try:
            with self.assertRaises(MetadataFetchError) as context:
                fetch_metadata(info_hash, peer.address, stop_event, FAST)
            self.assertEqual(context.exception.reason, "stopped")
        finally:
            peer.close()


if __name__ == "__main__":
    unittest.main()
