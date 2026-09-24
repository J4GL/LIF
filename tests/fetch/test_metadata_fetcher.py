"""Fetch category: BEP 9/10 metadata download against a loopback fake peer (F-004, FETCH-006 to FETCH-009)."""
import asyncio
import socket
import time
import unittest

from dht_scraper.metadata_fetcher import FetchTimeouts, MetadataFetchError, fetch_metadata, open_tcp_stream
from tests.fetch.fake_metadata_peer import FakeMetadataPeer, build_info_dict

FAST = FetchTimeouts(connect=1.0, handshake=1.0, piece=1.0, session=2.0)
PHASES = FetchTimeouts(connect=0.3, handshake=0.5, piece=0.5, session=5.0)


def fetch(info_hash, address, timeouts=FAST):
    return asyncio.run(fetch_metadata(info_hash, address, timeouts))


def failure_reason(info_hash, address, timeouts=FAST):
    try:
        fetch(info_hash, address, timeouts)
    except MetadataFetchError as error:
        return error.reason
    return None


class MetadataFetcherTest(unittest.TestCase):
    def test_fetches_single_piece_metadata_f004_t1(self):
        raw, info_hash = build_info_dict()
        peer = FakeMetadataPeer(info_hash, raw, "ok")
        try:
            self.assertEqual(fetch(info_hash, peer.address), raw)
            self.assertEqual(peer.requests, [0])
        finally:
            peer.close()

    def test_fetches_multi_piece_metadata_in_order(self):
        raw, info_hash = build_info_dict(extra_pieces=2000)
        self.assertGreater(len(raw), 2 * 16384)
        peer = FakeMetadataPeer(info_hash, raw, "ok", their_id=7, send_noise=False)
        try:
            self.assertEqual(fetch(info_hash, peer.address), raw)
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
            "wrong_hash": "hash_mismatch",
            "huge_frame": "frame_too_large",
            "silent": "timeout",
        }
        for mode, reason in expected.items():
            raw, info_hash = build_info_dict()
            peer = FakeMetadataPeer(info_hash, raw, mode)
            try:
                self.assertEqual(failure_reason(info_hash, peer.address), reason, mode)
            finally:
                peer.close()

    def test_connection_refused_and_cancellation(self):
        raw, info_hash = build_info_dict()
        probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        probe.bind(("127.0.0.1", 0))
        closed_port = probe.getsockname()[1]
        probe.close()
        self.assertEqual(failure_reason(info_hash, ("127.0.0.1", closed_port)), "connect")
        peer = FakeMetadataPeer(info_hash, raw, "silent")

        async def cancel_while_waiting():
            task = asyncio.ensure_future(fetch_metadata(info_hash, peer.address, FAST))
            await asyncio.sleep(0.2)
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task

        try:
            asyncio.run(cancel_while_waiting())
        finally:
            peer.close()

    def test_FETCH_006_handshakes_are_pipelined(self):
        raw, info_hash = build_info_dict()
        peer = FakeMetadataPeer(info_hash, raw, "expect_pipelined")
        try:
            self.assertEqual(fetch(info_hash, peer.address), raw)
            self.assertTrue(peer.saw_pipelined_handshake)
        finally:
            peer.close()

    def test_FETCH_007_pieces_are_pipelined(self):
        raw, info_hash = build_info_dict(extra_pieces=2000)
        peer = FakeMetadataPeer(info_hash, raw, "wait_for_all_requests", send_noise=False)
        try:
            self.assertEqual(fetch(info_hash, peer.address), raw)
            self.assertEqual(peer.requests, [0, 1, 2])
        finally:
            peer.close()

    def test_FETCH_008_phase_timeouts(self):
        with self.subTest(case="connect"):
            captured = []

            async def never_connects(sock, address):
                captured.append(sock)
                await asyncio.sleep(3600)

            started = time.monotonic()
            with self.assertRaises(MetadataFetchError) as context:
                asyncio.run(open_tcp_stream(("192.0.2.1", 6881), PHASES.connect, never_connects))
            elapsed = time.monotonic() - started
            self.assertEqual(context.exception.reason, "connect_timeout")
            self.assertTrue(0.3 <= elapsed < 0.8, elapsed)
            self.assertEqual(captured[0].fileno(), -1)
        for mode in ("silent_before_handshake", "silent"):
            with self.subTest(case=mode):
                raw, info_hash = build_info_dict()
                peer = FakeMetadataPeer(info_hash, raw, mode)
                try:
                    started = time.monotonic()
                    self.assertEqual(failure_reason(info_hash, peer.address, PHASES), "timeout")
                    elapsed = time.monotonic() - started
                    self.assertTrue(0.5 <= elapsed < 1.0, elapsed)
                finally:
                    peer.close()

    def test_V6_006_fetch_over_ipv6(self):
        raw, info_hash = build_info_dict(name=b"over ipv6")
        peer = FakeMetadataPeer(info_hash, raw, "ok", host="::1")
        try:
            self.assertEqual(fetch(info_hash, peer.address), raw)
        finally:
            peer.close()

    def test_FETCH_009_close_before_handshake(self):
        raw, info_hash = build_info_dict()
        peer = FakeMetadataPeer(info_hash, raw, "close_after_handshake")
        try:
            self.assertEqual(failure_reason(info_hash, peer.address), "closed_on_handshake")
        finally:
            peer.close()


if __name__ == "__main__":
    unittest.main()
