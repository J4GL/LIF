"""Protocol category: info dictionary summary (F-004)."""
import unittest

from dht_scraper.torrent_info_summary import MAX_LISTED_FILES, TorrentFile, TorrentMetadata, build_search_text, summarize_info_dict

INFO_HASH = b"h" * 20


class TorrentInfoSummaryTest(unittest.TestCase):
    def test_single_file(self):
        info = {b"name": b"ubuntu.iso", b"piece length": 262144, b"length": 1000, b"pieces": b"x" * 20}
        metadata = summarize_info_dict(INFO_HASH, info, 5.0, ("1.2.3.4", 6881))
        self.assertEqual((metadata.name, metadata.total_size, metadata.file_count, metadata.piece_length), ("ubuntu.iso", 1000, 1, 262144))
        self.assertEqual(metadata.files, (TorrentFile("ubuntu.iso", 1000),))
        self.assertFalse(metadata.is_private)
        self.assertIn("ubuntu.iso", metadata.search_text)

    def test_multi_file_with_utf8_name_and_private(self):
        info = {
            b"name": b"fallback", b"name.utf-8": "café".encode("utf-8"), b"piece length": 16384, b"private": 1,
            b"files": [{b"length": 5, b"path": [b"dir", b"A.TXT"]}, {b"length": 7, b"path.utf-8": [b"b.txt"]}],
        }
        metadata = summarize_info_dict(INFO_HASH, info, 1.0, None)
        self.assertEqual(metadata.name, "café")
        self.assertEqual(metadata.files, (TorrentFile("dir/A.TXT", 5), TorrentFile("b.txt", 7)))
        self.assertEqual(metadata.total_size, 12)
        self.assertTrue(metadata.is_private)
        self.assertEqual(metadata.search_text, "café\ndir/a.txt\nb.txt")

    def test_invalid_utf8_is_replaced_and_files_truncated(self):
        files = [{b"length": 1, b"path": [b"f%d" % index]} for index in range(MAX_LISTED_FILES + 50)]
        info = {b"name": b"\xff\xfe", b"piece length": 1, b"files": files}
        metadata = summarize_info_dict(INFO_HASH, info, 1.0, None)
        self.assertEqual(metadata.name, "��")
        self.assertEqual(len(metadata.files), MAX_LISTED_FILES)
        self.assertEqual((metadata.file_count, metadata.total_size), (MAX_LISTED_FILES + 50, MAX_LISTED_FILES + 50))

    def test_rejects_bad_info_dicts(self):
        bad = [
            b"not a dict",
            {b"piece length": 1, b"length": 1},
            {b"name": b"x", b"length": 1},
            {b"name": b"x", b"piece length": 0, b"length": 1},
            {b"name": b"x", b"piece length": 1, b"length": -1},
            {b"name": b"x", b"piece length": 1, b"files": []},
            {b"name": b"x", b"piece length": 1, b"files": [{b"length": 1}]},
        ]
        for info in bad:
            with self.assertRaises(ValueError, msg=repr(info)):
                summarize_info_dict(INFO_HASH, info, 1.0, None)

    def test_metadata_record_invariants(self):
        with self.assertRaises(AssertionError):
            TorrentMetadata(INFO_HASH, "x", 1, 1, 1, [], False, 0.0, None)
        self.assertEqual(build_search_text("Name", [TorrentFile("Dir/File", 1)]), "name\ndir/file")


if __name__ == "__main__":
    unittest.main()
