"""Magnet link construction."""
import urllib.parse

from dht_scraper.node_identity import NODE_ID_LENGTH

MAGNET_PREFIX = "magnet:?xt=urn:btih:"


# Parents: format_search_result, format_torrent_detail
# Keywords: magnet, link, info hash, display name
def build_magnet_link(info_hash: bytes, name: str) -> str:
    assert len(info_hash) == NODE_ID_LENGTH and isinstance(name, str)
    result = MAGNET_PREFIX + info_hash.hex()
    if name:
        result += "&dn=" + urllib.parse.quote(name, safe="")
    assert result.startswith(MAGNET_PREFIX) and len(result) >= len(MAGNET_PREFIX) + NODE_ID_LENGTH * 2
    return result
