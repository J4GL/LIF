"""Fetch category: message stream encryption against a loopback MSE responder (MSE-001, MSE-002)."""
import asyncio
import unittest

from dht_scraper.metadata_fetcher import FetchTimeouts, MetadataFetchError, fetch_metadata
from dht_scraper.stream_encryption import CRYPTO_PLAINTEXT, CRYPTO_RC4, Rc4, fetch_metadata_encrypted, mse_cipher
from tests.fetch.fake_metadata_peer import build_info_dict
from tests.fetch.fake_mse_peer import FakeMsePeer

FAST = FetchTimeouts(connect=1.0, handshake=1.0, piece=1.0, session=3.0)


class StreamEncryptionTest(unittest.TestCase):
    def test_MSE_001_rc4_vectors(self):
        vectors = [(b"Key", b"Plaintext", "bbf316e8d940af0ad3"), (b"Wiki", b"pedia", "1021bf0420"), (b"Secret", b"Attack at dawn", "45a01f645fc35b383552544b9bf5")]
        for key, plaintext, expected in vectors:
            with self.subTest(key=key):
                self.assertEqual(Rc4(key).crypt(plaintext).hex(), expected)
        keystream = Rc4(b"k" * 20).crypt(bytes(1024 + 16))[1024:]
        self.assertEqual(mse_cipher(b"k" * 20).crypt(bytes(16)), keystream)

    def test_MSE_002_encrypted_session_downloads_metadata(self):
        raw, info_hash = build_info_dict(name=b"encrypted", extra_pieces=900)
        with self.subTest(case="rc4 selected"):
            peer = FakeMsePeer(info_hash, raw, CRYPTO_RC4)
            try:
                with self.assertRaises(MetadataFetchError) as context:
                    asyncio.run(fetch_metadata(info_hash, peer.address, FAST))
                self.assertEqual(context.exception.reason, "closed_on_handshake")
                self.assertEqual(asyncio.run(fetch_metadata_encrypted(info_hash, peer.address, FAST)), raw)
                self.assertTrue(peer.verified_sync)
            finally:
                peer.close()
        with self.subTest(case="plaintext selected"):
            peer = FakeMsePeer(info_hash, raw, CRYPTO_PLAINTEXT)
            try:
                self.assertEqual(asyncio.run(fetch_metadata_encrypted(info_hash, peer.address, FAST)), raw)
            finally:
                peer.close()


if __name__ == "__main__":
    unittest.main()
