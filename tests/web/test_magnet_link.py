"""Web category: magnet link building (F-005, SEARCH-016)."""
import unittest
from urllib.parse import parse_qs, urlsplit

from dht_scraper.magnet_link import MAGNET_TRACKERS, build_magnet_link


class MagnetLinkTest(unittest.TestCase):
    def test_SEARCH_016_magnet_carries_trackers(self):
        self.assertEqual(len(MAGNET_TRACKERS), 20)
        link = build_magnet_link(b"\xab" * 20, "a b&c")
        self.assertTrue(link.startswith("magnet:?xt=urn:btih:" + "ab" * 20 + "&dn=a%20b%26c&tr="))
        self.assertTrue(link.endswith("&tr=udp%3A%2F%2F65.109.28.33%3A6969%2Fannounce"))
        query = parse_qs(urlsplit(link).query)
        self.assertEqual((query["xt"], query["dn"], query["tr"]), (["urn:btih:" + "ab" * 20], ["a b&c"], list(MAGNET_TRACKERS)))
        self.assertEqual(MAGNET_TRACKERS[0], "udp://93.158.213.92:1337/announce")

        link = build_magnet_link(b"\x01" * 20, "")
        self.assertTrue(link.startswith("magnet:?xt=urn:btih:" + "01" * 20 + "&tr="))
        query = parse_qs(urlsplit(link).query)
        self.assertNotIn("dn", query)
        self.assertEqual(query["tr"], list(MAGNET_TRACKERS))


if __name__ == "__main__":
    unittest.main()
