"""Insertion-ordered mapping with a size cap: remembering a key evicts the oldest keys."""
import collections
from typing import Any, Hashable


# Parents: DhtCrawler.add_node, DhtCrawler.handle_samples, DhtCrawler.handle_error, TorrentCatalog._add_peer_locked
# Keywords: bounded, ordered dict, evict oldest, remember
def remember(mapping: "collections.OrderedDict[Any, Any]", key: Hashable, value: Any, limit: int) -> bool:
    assert limit >= 1 and isinstance(mapping, collections.OrderedDict)
    is_new = key not in mapping
    mapping[key] = value
    mapping.move_to_end(key)
    while len(mapping) > limit:
        mapping.popitem(last=False)
    assert len(mapping) <= limit and key in mapping or not is_new
    return is_new
