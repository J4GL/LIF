"""Magnet link construction."""
import urllib.parse

from dht_scraper.node_identity import NODE_ID_LENGTH

MAGNET_PREFIX = "magnet:?xt=urn:btih:"

# https://github.com/ngosang/trackerslist/blob/master/trackers_best_ip.txt, in file order (2026-10-09)
MAGNET_TRACKERS = (
    "udp://93.158.213.92:1337/announce",
    "udp://151.242.104.187:80/announce",
    "udp://91.216.110.53:451/announce",
    "udp://185.121.168.96:1337/announce",
    "udp://43.250.54.126:6969/announce",
    "udp://34.66.57.33:80/announce",
    "udp://34.66.57.33:1337/announce",
    "udp://109.201.134.183:80/announce",
    "udp://45.137.199.107:6969/announce",
    "udp://135.125.198.235:1984/announce",
    "udp://211.75.210.221:80/announce",
    "udp://209.141.59.25:6969/announce",
    "udp://211.75.210.221:6969/announce",
    "http://211.75.210.221:6969/announce",
    "udp://83.102.180.21:80/announce",
    "udp://31.56.179.159:6969/announce",
    "udp://65.109.28.17:6969/announce",
    "udp://95.217.80.20:6969/announce",
    "udp://95.217.80.22:6969/announce",
    "udp://65.109.28.33:6969/announce",
)


# Parents: format_search_result, format_torrent_detail
# Keywords: magnet, link, info hash, display name
def build_magnet_link(info_hash: bytes, name: str) -> str:
    assert len(info_hash) == NODE_ID_LENGTH and isinstance(name, str)
    result = MAGNET_PREFIX + info_hash.hex()
    if name:
        result += "&dn=" + urllib.parse.quote(name, safe="")
    result += "".join("&tr=" + urllib.parse.quote(tracker, safe="") for tracker in MAGNET_TRACKERS)
    assert result.startswith(MAGNET_PREFIX) and len(result) >= len(MAGNET_PREFIX) + NODE_ID_LENGTH * 2
    assert result.count("&tr=") == len(MAGNET_TRACKERS)
    return result
