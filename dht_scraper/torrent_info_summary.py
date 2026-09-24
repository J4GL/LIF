"""Turn a verified torrent info dictionary into a small metadata record."""
from typing import Any, Dict, List, NamedTuple, Optional, Sequence, Tuple

from dht_scraper.bencode_codec import is_plain_int
from dht_scraper.node_identity import NODE_ID_LENGTH

MAX_LISTED_FILES = 200
Peer = Tuple[str, int]


class TorrentFile(NamedTuple):
    path: str
    length: int


class TorrentMetadata:
    """Immutable summary of one torrent's info dictionary. No piece hashes, no content."""

    __slots__ = ("info_hash", "name", "total_size", "piece_length", "file_count", "files", "is_private", "fetched_at", "source_peer", "search_text")

    # Parents: summarize_info_dict, tests
    # Keywords: metadata, record, invariants, immutable
    def __init__(
        self,
        info_hash: bytes,
        name: str,
        total_size: int,
        piece_length: int,
        file_count: int,
        files: Sequence[TorrentFile],
        is_private: bool,
        fetched_at: float,
        source_peer: Optional[Peer],
    ) -> None:
        assert len(info_hash) == NODE_ID_LENGTH and isinstance(name, str)
        assert total_size >= 0 and piece_length > 0 and file_count >= 1 and 1 <= len(files) <= min(file_count, MAX_LISTED_FILES)
        self.info_hash = info_hash
        self.name = name
        self.total_size = total_size
        self.piece_length = piece_length
        self.file_count = file_count
        self.files = tuple(files)
        self.is_private = is_private
        self.fetched_at = fetched_at
        self.source_peer = source_peer
        self.search_text = build_search_text(name, self.files)
        assert isinstance(self.files, tuple) and self.search_text == self.search_text.lower()


# Parents: decode_utf8_name, read_file_entries
# Keywords: utf-8, decode, replace, text
def decode_text(value: Any) -> str:
    assert value is None or isinstance(value, (bytes, str, int, list, dict))
    if isinstance(value, bytes):
        result = value.decode("utf-8", errors="replace")
    elif isinstance(value, str):
        result = value
    else:
        result = ""
    assert isinstance(result, str)
    return result


# Parents: summarize_info_dict
# Keywords: name, utf-8 variant, fallback
def decode_utf8_name(info: Dict[bytes, Any]) -> str:
    assert isinstance(info, dict)
    result = decode_text(info.get(b"name.utf-8")) or decode_text(info.get(b"name"))
    assert isinstance(result, str)
    return result


# Parents: summarize_info_dict
# Keywords: files, paths, single file, multi file
def read_file_entries(info: Dict[bytes, Any], name: str) -> List[TorrentFile]:
    assert isinstance(info, dict) and isinstance(name, str)
    files = info.get(b"files")
    entries: List[TorrentFile] = []
    if files is None:
        length = info.get(b"length")
        if not is_plain_int(length) or length < 0:
            raise ValueError("bad_info_dict")
        entries.append(TorrentFile(name, length))
    else:
        if not isinstance(files, list) or not files:
            raise ValueError("bad_info_dict")
        for item in files:
            if not isinstance(item, dict):
                raise ValueError("bad_info_dict")
            length = item.get(b"length")
            path_parts = item.get(b"path.utf-8") or item.get(b"path")
            if not is_plain_int(length) or length < 0 or not isinstance(path_parts, list):
                raise ValueError("bad_info_dict")
            entries.append(TorrentFile("/".join(decode_text(part) for part in path_parts), length))
    assert len(entries) >= 1 and all(entry.length >= 0 for entry in entries)
    return entries


# Parents: TorrentMetadata.__init__
# Keywords: search, lower case, name, paths
def build_search_text(name: str, files: Sequence[TorrentFile]) -> str:
    assert isinstance(name, str)
    result = (name + "\n" + "\n".join(entry.path for entry in files)).lower()
    assert name.lower() in result
    return result


# Parents: FetchEngine.attempt
# Keywords: info dict, summarize, metadata, validation
def summarize_info_dict(info_hash: bytes, info: Any, fetched_at: float, source_peer: Optional[Peer]) -> TorrentMetadata:
    assert len(info_hash) == NODE_ID_LENGTH and fetched_at >= 0
    if not isinstance(info, dict):
        raise ValueError("bad_info_dict")
    name = decode_utf8_name(info)
    piece_length = info.get(b"piece length")
    if not name or not is_plain_int(piece_length) or piece_length <= 0:
        raise ValueError("bad_info_dict")
    entries = read_file_entries(info, name)
    total_size = sum(entry.length for entry in entries)
    is_private = info.get(b"private") == 1
    result = TorrentMetadata(info_hash, name, total_size, piece_length, len(entries), entries[:MAX_LISTED_FILES], is_private, fetched_at, source_peer)
    assert result.total_size == total_size and result.file_count == len(entries)
    return result
