"""Web category: magnet link building (F-005)."""
import unittest

from dht_scraper.magnet_link import build_magnet_link


class MagnetLinkTest(unittest.TestCase):
    def test_build_magnet_link_encodes_name(self):
        link = build_magnet_link(b"\xab" * 20, "a b&c")
        self.assertEqual(link, "magnet:?xt=urn:btih:" + "ab" * 20 + "&dn=a%20b%26c")

    def test_build_magnet_link_without_name(self):
        self.assertEqual(build_magnet_link(b"\x01" * 20, ""), "magnet:?xt=urn:btih:" + "01" * 20)


if __name__ == "__main__":
    unittest.main()
