"""Fetch category: uTP client against a loopback uTP responder (UTP-001 to UTP-004, UTP-006)."""
import asyncio
import socket
import struct
import time
import unittest

from dht_scraper.metadata_fetcher import FetchTimeouts, MetadataFetchError
from dht_scraper.utp_transport import ST_DATA, ST_FIN, ST_STATE, UtpConnection, UtpPacket, decode_utp_packet, encode_utp_packet, fetch_metadata_utp
from tests.fetch.fake_metadata_peer import build_info_dict
from tests.fetch.fake_utp_peer import FakeUtpPeer
from tests.shared_fixtures import wait_until

TIMEOUTS = FetchTimeouts(connect=1.0, handshake=4.0, piece=4.0, session=8.0, utp_connect=3.0)


def fetch(info_hash, address, timeouts=TIMEOUTS):
    return asyncio.run(fetch_metadata_utp(info_hash, address, timeouts=timeouts))


class RecordingSocket:
    """Stands in for UtpSocket: records the decoded packets a connection sends."""

    def __init__(self):
        self.sent = []

    def send(self, packet, address):
        self.sent.append(decode_utp_packet(packet))

    def forget(self, connection):
        pass


def peer_packet(packet_type, seq_nr, payload=b""):
    return UtpPacket(packet_type, 100, 0, 0, 1 << 20, seq_nr, 1, payload)


class UtpTransportTest(unittest.TestCase):
    def test_UTP_001_header_codec(self):
        encoded = encode_utp_packet(ST_DATA, 0x1234, 7, 9, 65536, 65535, 2, b"hello")
        self.assertEqual(len(encoded), 25)
        packet = decode_utp_packet(encoded)
        self.assertEqual(tuple(packet), (ST_DATA, 0x1234, 7, 9, 65536, 65535, 2, b"hello"))
        with_extension = bytes([encoded[0], 1]) + encoded[2:20] + bytes([0, 4]) + b"\xff\x00\x00\x00" + b"hello"
        self.assertEqual(decode_utp_packet(with_extension).payload, b"hello")
        bad_version = bytes([(ST_DATA << 4) | 2]) + encoded[1:]
        bad_type = bytes([(5 << 4) | 1]) + encoded[1:]
        truncated_extension = bytes([encoded[0], 1]) + encoded[2:20] + bytes([0, 30]) + b"abc"
        for data in (encoded[:19], bad_version, bad_type, truncated_extension):
            self.assertIsNone(decode_utp_packet(data))

    def test_UTP_002_metadata_over_utp(self):
        raw, info_hash = build_info_dict(name=b"over utp", extra_pieces=2000)
        peer = FakeUtpPeer(info_hash, raw)
        try:
            self.assertEqual(fetch(info_hash, peer.address), raw)
            self.assertEqual(peer.requests, [0, 1, 2])
            self.assertTrue(wait_until(lambda: peer.unacked_count() == 0, timeout=2.0))
        finally:
            peer.close()

    def test_UTP_003_reordered_and_duplicated_packets(self):
        raw, info_hash = build_info_dict(name=b"reordered", extra_pieces=2000)
        peer = FakeUtpPeer(info_hash, raw, "reorder")
        try:
            self.assertEqual(fetch(info_hash, peer.address), raw)
        finally:
            peer.close()

    def test_UTP_004_resend_and_give_up(self):
        with self.subTest(case="lost syn and lost data"):
            raw, info_hash = build_info_dict(name=b"lossy")
            peer = FakeUtpPeer(info_hash, raw, "lossy")
            try:
                started = time.monotonic()
                self.assertEqual(fetch(info_hash, peer.address), raw)
                self.assertTrue(1.5 <= time.monotonic() - started < 4.0)
            finally:
                peer.close()
        with self.subTest(case="nobody answers"):
            silent = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            silent.bind(("127.0.0.1", 0))
            try:
                started = time.monotonic()
                with self.assertRaises(MetadataFetchError) as context:
                    fetch(b"\x01" * 20, silent.getsockname(), TIMEOUTS._replace(utp_connect=0.5))
                self.assertEqual(context.exception.reason, "connect_timeout")
                self.assertLess(time.monotonic() - started, 1.0)
            finally:
                silent.close()

    def test_UTP_006_fin_ends_the_stream(self):
        async def run(arrivals, reads):
            recording = RecordingSocket()
            connection = UtpConnection(recording, ("1.2.3.4", 1), 100)
            connection.send_syn()
            connection.packet_received(peer_packet(ST_STATE, 500))
            for packet in arrivals:
                connection.packet_received(packet)
            await asyncio.sleep(0)
            loop = asyncio.get_running_loop()
            results = []
            for length, closed_reason in reads:
                started = loop.time()
                try:
                    results.append(await connection.read_exactly(length, started + 2.0, closed_reason))
                except MetadataFetchError as error:
                    results.append((error.reason, loop.time() - started < 0.5))
            acks = [packet.ack_nr for packet in recording.sent if packet.packet_type == ST_STATE]
            connection.close(send_reset=False)
            return results, acks[-1]

        cases = {
            "in order": [peer_packet(ST_DATA, 500, b"abc"), peer_packet(ST_FIN, 501)],
            "fin before data": [peer_packet(ST_FIN, 501), peer_packet(ST_DATA, 500, b"abc")],
        }
        for case, arrivals in cases.items():
            with self.subTest(case=case):
                results, last_ack = asyncio.run(run(arrivals, [(3, "closed"), (1, "closed")]))
                self.assertEqual(results, [b"abc", ("closed", True)])
                self.assertEqual(last_ack, 501)
        with self.subTest(case="fin before any data"):
            results, last_ack = asyncio.run(run([peer_packet(ST_FIN, 500)], [(1, "closed_on_handshake")]))
            self.assertEqual(results, [("closed_on_handshake", True)])
            self.assertEqual(last_ack, 500)


if __name__ == "__main__":
    unittest.main()
