"""SQLite search database: torrents with metadata, an FTS5 index and a typo index of their terms."""
import contextlib
import json
import os
import re
import sqlite3
import unicodedata
import urllib.parse
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

SCHEMA_VERSION = 1
READ_TIMEOUT_SECONDS = 2.0
WRITE_TIMEOUT_SECONDS = 5.0
MAX_SEARCHABLE_FILES = 20
MIN_TERM_LENGTH = 3
MAX_TERM_LENGTH = 32
MIN_TYPO_LENGTH = 4
MAX_TYPO_TERMS = 20
MAX_QUERY_TERMS = 16
MAX_LOOKUP_PARAMETERS = 500
TERM_PATTERN = re.compile(r"[^\W_]+")
DOCUMENT_COLUMNS = ("info_hash", "name", "size", "file_count", "piece_length", "is_private", "fetched_at", "seen_count", "announce_count", "first_seen", "last_seen")
SEARCH_COLUMNS = ("info_hash", "name", "size", "file_count", "seen_count", "announce_count", "first_seen", "last_seen")
RANK_ORDER = "t.seen_count DESC, t.last_seen DESC"
SCHEMA_SQL = """
BEGIN;
CREATE TABLE torrents (
    id INTEGER PRIMARY KEY,
    info_hash TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    size INTEGER NOT NULL,
    file_count INTEGER NOT NULL,
    piece_length INTEGER NOT NULL,
    is_private INTEGER NOT NULL,
    fetched_at REAL NOT NULL,
    seen_count INTEGER NOT NULL,
    announce_count INTEGER NOT NULL,
    first_seen REAL NOT NULL,
    last_seen REAL NOT NULL,
    files_json TEXT NOT NULL
);
CREATE INDEX torrents_rank ON torrents (seen_count DESC, last_seen DESC);
CREATE VIRTUAL TABLE torrents_fts USING fts5(name, files, content='', tokenize='unicode61 remove_diacritics 2');
CREATE TABLE terms (variant TEXT NOT NULL, term TEXT NOT NULL, PRIMARY KEY (variant, term)) WITHOUT ROWID;
PRAGMA user_version = 1;
COMMIT;
"""
Document = Dict[str, Any]
Counters = Tuple[int, int, float]


class SchemaMismatch(sqlite3.DatabaseError):
    """The file is not a search database of this schema version."""


# Parents: TorrentDatabase.write_documents, TorrentDatabase.search, tests
# Keywords: tokenizer, unicode61, diacritics, case folding
def text_terms(text: str) -> List[str]:
    assert isinstance(text, str)
    decomposed = unicodedata.normalize("NFKD", text)
    stripped = "".join(character for character in decomposed if not unicodedata.combining(character))
    result = TERM_PATTERN.findall(stripped.casefold())
    assert all(term == term.casefold() for term in result)
    return result


# Parents: term_rows, typo_terms
# Keywords: typo index, deletion neighborhood, symspell
def deletions(term: str) -> Set[str]:
    assert len(term) >= 1
    result = {term[:index] + term[index + 1:] for index in range(len(term))}
    assert all(len(variant) == len(term) - 1 for variant in result)
    return result


# Parents: typo_terms
# Keywords: damerau levenshtein, one edit, swap, verify candidate
def within_one_edit(first: str, second: str) -> bool:
    assert isinstance(first, str) and isinstance(second, str)
    if abs(len(first) - len(second)) > 1:
        return False
    if len(first) == len(second):
        differences = [index for index in range(len(first)) if first[index] != second[index]]
        swapped = len(differences) == 2 and differences[1] == differences[0] + 1 and first[differences[0]] == second[differences[1]] and first[differences[1]] == second[differences[0]]
        return len(differences) <= 1 or swapped
    shorter, longer = (first, second) if len(first) < len(second) else (second, first)
    index = 0
    while index < len(shorter) and shorter[index] == longer[index]:
        index += 1
    result = shorter[index:] == longer[index + 1:]
    assert isinstance(result, bool)
    return result


# Parents: TorrentDatabase.insert_document
# Keywords: typo index, rows, term length bounds
def term_rows(terms: Iterable[str]) -> List[Tuple[str, str]]:
    result = []
    for term in sorted(set(terms)):
        if MIN_TERM_LENGTH <= len(term) <= MAX_TERM_LENGTH:
            result.append((term, term))
            result.extend((variant, term) for variant in sorted(deletions(term)))
    assert all(len(row) == 2 for row in result)
    return result


# Parents: TorrentDatabase.search
# Keywords: typo, candidates, terms table, verified
def typo_terms(connection: sqlite3.Connection, term: str) -> List[str]:
    assert isinstance(term, str)
    if len(term) < MIN_TYPO_LENGTH or term.isdigit():
        return []
    variants = sorted({term} | deletions(term))
    rows = connection.execute("SELECT DISTINCT term FROM terms WHERE variant IN (%s) ORDER BY term" % ",".join("?" * len(variants)), variants).fetchall()
    result = [row[0] for row in rows if row[0] != term and within_one_edit(term, row[0])][:MAX_TYPO_TERMS]
    assert len(result) <= MAX_TYPO_TERMS
    return result


# Parents: TorrentDatabase.search
# Keywords: fts5 query, quoted terms, prefix, alternatives
def match_expression(terms: Sequence[str], alternatives: Optional[Sequence[Sequence[str]]] = None) -> str:
    assert len(terms) >= 1 and (alternatives is None or len(alternatives) == len(terms))
    groups = []
    for position, term in enumerate(terms):
        options = ['"%s"%s' % (term, "*" if position == len(terms) - 1 else "")]
        options.extend('"%s"' % other for other in (alternatives[position] if alternatives else ()))
        groups.append("(%s)" % " OR ".join(options))
    result = " AND ".join(groups)
    assert result.count("(") == len(terms)
    return result


# Parents: TorrentDatabase.search
# Keywords: search record, row, api shape
def search_record(row: Sequence[Any]) -> Document:
    assert len(row) == len(SEARCH_COLUMNS)
    result = dict(zip(SEARCH_COLUMNS, row))
    assert len(result["info_hash"]) == 40
    return result


# Parents: CatalogRequestHandler.render_torrent, tests
# Keywords: detail record, document, api shape, no peers
def document_detail_record(document: Document) -> Document:
    assert "info_hash" in document and "files" in document
    metadata = {
        "name": document["name"],
        "size": document["size"],
        "piece_length": document["piece_length"],
        "file_count": document["file_count"],
        "files": [{"path": path, "length": length} for path, length in document["files"]],
        "is_private": document["is_private"],
        "fetched_at": document["fetched_at"],
        "source_peer": None,
    }
    result = {
        "info_hash": document["info_hash"],
        "seen_count": document["seen_count"],
        "announce_count": document["announce_count"],
        "first_seen": document["first_seen"],
        "last_seen": document["last_seen"],
        "fetch_state": "done",
        "fetch_attempts": 0,
        "last_error": "",
        "lookups_started": 0,
        "peers": [],
        "metadata": metadata,
    }
    assert result["metadata"]["source_peer"] is None
    return result


class TorrentDatabase:
    """One SQLite file: a writer connection for the index thread, a short reader connection per query."""

    # Parents: ScraperRuntime.start_indexer, tests
    # Keywords: database, path, lazy writer
    def __init__(self, path: str) -> None:
        assert isinstance(path, str) and path
        self.path = path
        self.writer: Optional[sqlite3.Connection] = None
        assert self.writer is None

    # Parents: ScraperRuntime.start_indexer, tests
    # Keywords: open, create schema, wal, schema version, refuse foreign file
    def open_writer(self) -> bool:
        assert self.writer is None
        connection = sqlite3.connect(self.path, timeout=WRITE_TIMEOUT_SECONDS, check_same_thread=False, isolation_level=None)
        try:
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            objects = connection.execute("SELECT count(*) FROM sqlite_master").fetchone()[0]
            if version not in (0, SCHEMA_VERSION) or (version == 0 and objects > 0):
                raise SchemaMismatch("%s has schema version %d, expected %d" % (self.path, version, SCHEMA_VERSION))
            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            created = version == 0
            if created:
                connection.executescript(SCHEMA_SQL)
        except BaseException:
            connection.close()
            raise
        self.writer = connection
        assert self.writer is not None
        return created

    # Parents: ScraperRuntime.stop, tests
    # Keywords: close, writer, idempotent
    def close(self) -> None:
        if self.writer is not None:
            self.writer.close()
            self.writer = None
        assert self.writer is None

    # Parents: search, get_document, lookup_counters, count_documents
    # Keywords: reader, never creates, query only, busy timeout
    def connect_reader(self) -> sqlite3.Connection:
        assert self.path
        uri = "file:%s?mode=rw" % urllib.parse.quote(os.path.abspath(self.path))
        result = sqlite3.connect(uri, uri=True, timeout=READ_TIMEOUT_SECONDS)
        try:
            result.execute("PRAGMA query_only = ON")
        except BaseException:
            result.close()
            raise
        assert result is not None
        return result

    # Parents: write_documents, update_counters
    # Keywords: transaction, begin immediate, commit, rollback
    @contextlib.contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        assert self.writer is not None and not self.writer.in_transaction
        self.writer.execute("BEGIN IMMEDIATE")
        try:
            yield self.writer
        except BaseException:
            self.writer.execute("ROLLBACK")
            raise
        self.writer.execute("COMMIT")
        assert not self.writer.in_transaction

    # Parents: SearchIndexer.flush_documents, tests
    # Keywords: write, insert text once, update counters, one transaction
    def write_documents(self, documents: Sequence[Document]) -> None:
        assert self.writer is not None and all(len(document["info_hash"]) == 40 for document in documents)
        with self.transaction():
            for document in documents:
                row = self.writer.execute("SELECT id FROM torrents WHERE info_hash = ?", (document["info_hash"],)).fetchone()
                if row is None:
                    self.insert_document(document)
                else:
                    self.writer.execute(
                        "UPDATE torrents SET seen_count = ?, announce_count = ?, first_seen = ?, last_seen = ? WHERE id = ?",
                        (document["seen_count"], document["announce_count"], document["first_seen"], document["last_seen"], row[0]),
                    )
        assert self.writer is not None

    # Parents: write_documents
    # Keywords: insert, fts row, typo terms, first 20 files
    def insert_document(self, document: Document) -> None:
        assert self.writer is not None and self.writer.in_transaction
        values = [document[column] for column in DOCUMENT_COLUMNS] + [json.dumps(document["files"], ensure_ascii=False)]
        values[DOCUMENT_COLUMNS.index("is_private")] = int(bool(document["is_private"]))
        cursor = self.writer.execute("INSERT INTO torrents (%s, files_json) VALUES (%s)" % (", ".join(DOCUMENT_COLUMNS), ", ".join("?" * len(values))), values)
        paths = [path for path, _ in document["files"][:MAX_SEARCHABLE_FILES]]
        self.writer.execute("INSERT INTO torrents_fts (rowid, name, files) VALUES (?, ?, ?)", (cursor.lastrowid, document["name"], "\n".join(paths)))
        terms = text_terms(document["name"]) + [term for path in paths for term in text_terms(path)]
        self.writer.executemany("INSERT OR IGNORE INTO terms (variant, term) VALUES (?, ?)", term_rows(terms))
        assert cursor.lastrowid is not None

    # Parents: SearchIndexer.flush_counters, tests
    # Keywords: counter update, missing rows, one transaction
    def update_counters(self, updates: Sequence[Document]) -> List[str]:
        assert self.writer is not None
        missing = []
        with self.transaction():
            for update in updates:
                cursor = self.writer.execute(
                    "UPDATE torrents SET seen_count = ?, announce_count = ?, last_seen = ? WHERE info_hash = ?",
                    (update["seen_count"], update["announce_count"], update["last_seen"], update["info_hash"]),
                )
                if cursor.rowcount == 0:
                    missing.append(update["info_hash"])
        assert len(missing) <= len(updates)
        return missing

    # Parents: SearchIndexer.flush_documents, tests
    # Keywords: index base, previous runs, chunks
    def lookup_counters(self, info_hashes: Sequence[str]) -> Dict[str, Counters]:
        assert all(len(info_hash) == 40 for info_hash in info_hashes)
        result: Dict[str, Counters] = {}
        connection = self.connect_reader()
        try:
            for start in range(0, len(info_hashes), MAX_LOOKUP_PARAMETERS):
                chunk = list(info_hashes[start:start + MAX_LOOKUP_PARAMETERS])
                query = "SELECT info_hash, seen_count, announce_count, first_seen FROM torrents WHERE info_hash IN (%s)" % ",".join("?" * len(chunk))
                for info_hash, seen_count, announce_count, first_seen in connection.execute(query, chunk):
                    result[info_hash] = (seen_count, announce_count, first_seen)
        finally:
            connection.close()
        assert len(result) <= len(info_hashes)
        return result

    # Parents: SearchIndexer.refresh_count, tests
    # Keywords: count, documents, statistics
    def count_documents(self) -> int:
        connection = self.connect_reader()
        try:
            result = connection.execute("SELECT count(*) FROM torrents").fetchone()[0]
        finally:
            connection.close()
        assert result >= 0
        return result

    # Parents: CatalogRequestHandler.render_torrent, tests
    # Keywords: detail, document, files json
    def get_document(self, info_hash: str) -> Optional[Document]:
        assert len(info_hash) == 40
        connection = self.connect_reader()
        try:
            row = connection.execute("SELECT %s, files_json FROM torrents WHERE info_hash = ?" % ", ".join(DOCUMENT_COLUMNS), (info_hash,)).fetchone()
        finally:
            connection.close()
        if row is None:
            return None
        result = dict(zip(DOCUMENT_COLUMNS, row[:-1]))
        result["is_private"] = bool(result["is_private"])
        result["files"] = json.loads(row[-1])
        assert result["info_hash"] == info_hash
        return result

    # Parents: CatalogRequestHandler.search_records, tests
    # Keywords: search, tiers, typo tolerance, name first, popularity
    def search(self, query: str, limit: int) -> List[Document]:
        assert isinstance(query, str) and limit >= 1
        terms = text_terms(query)[:MAX_QUERY_TERMS]
        connection = self.connect_reader()
        try:
            if not terms:
                rows = connection.execute("SELECT %s FROM torrents t ORDER BY %s LIMIT ?" % (", ".join("t." + column for column in SEARCH_COLUMNS), RANK_ORDER), (limit,)).fetchall()
                found = [search_record(row) for row in rows]
            else:
                found = self.run_match(connection, match_expression(terms), limit)
            if terms and len(found) < limit:
                alternatives = [typo_terms(connection, term) for term in terms]
                if any(alternatives):
                    known = {record["info_hash"] for record in found}
                    extra = self.run_match(connection, match_expression(terms, alternatives), limit)
                    found += [record for record in extra if record["info_hash"] not in known][:limit - len(found)]
        finally:
            connection.close()
        assert len(found) <= limit
        return found

    # Parents: search
    # Keywords: fts5 match, name first, ranking, limit
    def run_match(self, connection: sqlite3.Connection, expression: str, limit: int) -> List[Document]:
        assert expression and limit >= 1
        query = (
            "SELECT %s FROM torrents t WHERE t.id IN (SELECT rowid FROM torrents_fts WHERE torrents_fts MATCH ?) "
            "ORDER BY (t.id IN (SELECT rowid FROM torrents_fts WHERE torrents_fts MATCH ?)) DESC, %s LIMIT ?"
        ) % (", ".join("t." + column for column in SEARCH_COLUMNS), RANK_ORDER)
        rows = connection.execute(query, (expression, "name : (%s)" % expression, limit)).fetchall()
        result = [search_record(row) for row in rows]
        assert len(result) <= limit
        return result
