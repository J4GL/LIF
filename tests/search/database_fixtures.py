"""Helpers shared by the search, web and runtime tests: documents, temporary databases, failure injection."""
import os
import shutil
import sqlite3
import tempfile
import threading
import time

from dht_scraper.torrent_database import TorrentDatabase


def info_hash_hex(number):
    return "%040x" % number


def document(number, name, seen_count=1, last_seen=1.0, files=None, announce_count=0, first_seen=1.0, lengths=None):
    paths = files or [name]
    sizes = lengths or [7] * len(paths)
    return {
        "info_hash": info_hash_hex(number), "name": name, "size": sum(sizes), "file_count": len(paths), "piece_length": 16384,
        "is_private": False, "fetched_at": 2.0, "seen_count": seen_count, "announce_count": announce_count, "first_seen": first_seen,
        "last_seen": last_seen, "files": [[path, size] for path, size in zip(paths, sizes)],
    }


def temporary_directory(test):
    directory = tempfile.mkdtemp(prefix="dht-db-")
    test.addCleanup(shutil.rmtree, directory, True)
    return directory


def open_database(test, documents=(), path=None):
    """A TorrentDatabase opened for writing in a temporary directory, holding the documents."""
    database = TorrentDatabase(path or os.path.join(temporary_directory(test), "torrents.sqlite3"))
    database.open_writer()
    test.addCleanup(database.close)
    if documents:
        database.write_documents(list(documents))
    return database


def user_version(path):
    connection = sqlite3.connect(path)
    try:
        return connection.execute("PRAGMA user_version").fetchone()[0]
    finally:
        connection.close()


class FailingDatabase:
    """Wraps a TorrentDatabase: records every method call and raises OperationalError while `failing`."""

    def __init__(self, database, failing=False):
        self.database = database
        self.failing = failing
        self.calls = []
        self.lock = threading.Lock()

    def __getattr__(self, name):
        target = getattr(self.database, name)
        if not callable(target):
            return target

        def call(*args, **kwargs):
            with self.lock:
                self.calls.append((name, time.monotonic()))
            if self.failing:
                raise sqlite3.OperationalError("disk I/O error (injected)")
            return target(*args, **kwargs)

        return call

    def call_names(self):
        with self.lock:
            return [name for name, _ in self.calls]

    def call_times(self):
        with self.lock:
            return [moment for _, moment in self.calls]
