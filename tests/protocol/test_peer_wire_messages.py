"""Protocol category: peer wire handshake, extension protocol and ut_metadata encoding (F-004)."""
import unittest

from dht_scraper.bencode_codec import decode_bencode
from dht_scraper.peer_wire_messages import (
    MAX_METADATA_SIZE,
    decode_extended_message,
    decode_frame_length,
    decode_handshake,
    decode_metadata_message,
    encode_extended_handshake,
    encode_extended_message,
    encode_frame,
    encode_handshake,
    encode_metadata_request,
    expected_piece_length,
    piece_count,
    read_extended_handshake,
)

INFO_HASH = b"h" * 20
PEER_ID = b"-DS0002-" + b"x" * 12


class HandshakeTest(unittest.TestCase):
    def test_handshake_layout_and_round_trip(self):
        data = encode_handshake(INFO_HASH, PEER_ID)
        self.assertEqual(len(data), 68)
        self.assertEqual(data[0], 19)
        self.assertEqual(data[1:20], b"BitTorrent protocol")
        self.assertTrue(data[25] & 0x10)
        self.assertEqual(data[28:48], INFO_HASH)
        self.assertEqual(decode_handshake(data), (INFO_HASH, PEER_ID, True))

    def test_handshake_without_extension_bit_and_bad_input(self):
        plain = bytearray(encode_handshake(INFO_HASH, PEER_ID))
        plain[25] = 0
        self.assertEqual(decode_handshake(bytes(plain))[2], False)
        with self.assertRaises(ValueError):
            decode_handshake(b"\x13NotBitTorrent protocol" + b"\x00" * 48)
        with self.assertRaises(ValueError):
            decode_handshake(b"\x13")


class FramesAndExtensionsTest(unittest.TestCase):
    def test_frame_and_extended_message_round_trip(self):
        frame = encode_frame(20, b"\x01abc")
        self.assertEqual(decode_frame_length(frame[:4]), 5)
        self.assertEqual(frame[4], 20)
        message = encode_extended_message(3, {b"msg_type": 1, b"piece": 0, b"total_size": 5}, b"hello")
        extension_id, payload, trailing = decode_extended_message(message[5:])
        self.assertEqual((extension_id, payload[b"piece"], trailing), (3, 0, b"hello"))
        self.assertEqual(decode_metadata_message(payload), (1, 0, 5))
        with self.assertRaises(ValueError):
            decode_extended_message(b"\x03")
        with self.assertRaises(ValueError):
            decode_extended_message(b"\x03i1e")

    def test_extended_handshake(self):
        ours = encode_extended_handshake()
        extension_id, payload, _ = decode_extended_message(ours[5:])
        self.assertEqual(extension_id, 0)
        self.assertEqual(payload[b"m"], {b"ut_metadata": 1})
        self.assertEqual(read_extended_handshake({b"m": {b"ut_metadata": 3}, b"metadata_size": 1000}), (3, 1000))
        for bad in [{}, {b"m": {}}, {b"m": {b"ut_metadata": 3}}, {b"m": {b"ut_metadata": 3}, b"metadata_size": 0}]:
            with self.assertRaises(ValueError, msg=repr(bad)):
                read_extended_handshake(bad)
        with self.assertRaisesRegex(ValueError, "too_large"):
            read_extended_handshake({b"m": {b"ut_metadata": 3}, b"metadata_size": MAX_METADATA_SIZE + 1})

    def test_metadata_request_and_piece_math(self):
        request = encode_metadata_request(3, 2)
        _, payload, _ = decode_extended_message(request[5:])
        self.assertEqual(payload, {b"msg_type": 0, b"piece": 2})
        self.assertEqual(piece_count(16384), 1)
        self.assertEqual(piece_count(16385), 2)
        self.assertEqual(piece_count(40000), 3)
        self.assertEqual(expected_piece_length(40000, 0), 16384)
        self.assertEqual(expected_piece_length(40000, 2), 40000 - 2 * 16384)
        with self.assertRaises(ValueError):
            decode_metadata_message({b"msg_type": 1})


if __name__ == "__main__":
    unittest.main()
