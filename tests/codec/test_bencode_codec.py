"""Codec category: bencode encode and decode (F-005)."""
import unittest

from dht_scraper.bencode_codec import decode_bencode, encode_bencode


class BencodeCodecTest(unittest.TestCase):
    def test_round_trip_f005_t1(self):
        value = {"a": 1, "b": [b"x", {"c": -2}]}
        encoded = encode_bencode(value)
        self.assertEqual(encoded, b"d1:ai1e1:bl1:xd1:ci-2eeee")
        self.assertEqual(decode_bencode(encoded), {b"a": 1, b"b": [b"x", {b"c": -2}]})

    def test_dict_keys_are_sorted(self):
        self.assertEqual(encode_bencode({"b": 1, "a": 2}), b"d1:ai2e1:bi1ee")

    def test_encode_rejects_unsupported_types(self):
        with self.assertRaises(TypeError):
            encode_bencode(True)
        with self.assertRaises(TypeError):
            encode_bencode(1.5)
        with self.assertRaises(TypeError):
            encode_bencode([object()])

    def test_decode_rejects_malformed_input(self):
        malformed = [
            b"",           # empty
            b"i1ee",       # trailing data
            b"i01e",       # leading zero
            b"i-0e",       # negative zero
            b"ie",         # no digits
            b"i12",        # unterminated integer
            b"-1:x",       # negative string length
            b"5:ab",       # truncated string
            b"3ab",        # missing colon
            b"l",          # unterminated list
            b"li1e",       # unterminated list with item
            b"d1:a",       # dict value missing
            b"di1ei2ee",   # non string key
            b"x",          # unknown prefix
        ]
        for data in malformed:
            with self.assertRaises(ValueError, msg=repr(data)):
                decode_bencode(data)

    def test_deep_nesting_does_not_crash_with_value_error(self):
        with self.assertRaises((ValueError, RecursionError)):
            decode_bencode(b"l" * 5000)

    def test_decode_binary_string_and_zero_length(self):
        self.assertEqual(decode_bencode(b"0:"), b"")
        self.assertEqual(decode_bencode(b"3:\x00\xff\x01"), b"\x00\xff\x01")
        self.assertEqual(decode_bencode(b"le"), [])
        self.assertEqual(decode_bencode(b"de"), {})


if __name__ == "__main__":
    unittest.main()
