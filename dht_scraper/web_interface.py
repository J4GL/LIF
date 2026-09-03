"""Minimal HTTP interface: the page, live stats, search and torrent detail as JSON."""
import http.server
import json
import logging
import string
import urllib.parse
from typing import Any, Callable, Dict, Optional, Tuple

from dht_scraper.event_log import LOGGER_NAME
from dht_scraper.magnet_link import build_magnet_link
from dht_scraper.node_identity import NODE_ID_LENGTH, is_valid_node_id
from dht_scraper.torrent_catalog import TorrentCatalog
from dht_scraper.web_page import render_index_page

LOGGER = logging.getLogger(LOGGER_NAME)
DEFAULT_WEB_HOST = "127.0.0.1"
DEFAULT_WEB_PORT = 8080
DEFAULT_SEARCH_LIMIT = 50
MAX_SEARCH_LIMIT = 200
MAX_QUERY_LENGTH = 200
ROUTE_TORRENT_PREFIX = "/api/torrent/"
SERVER_POLL_SECONDS = 0.5
CONTENT_JSON = "application/json; charset=utf-8"
CONTENT_HTML = "text/html; charset=utf-8"
CONTENT_SECURITY_POLICY = "default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'"
StatsProvider = Callable[[], Dict[str, Any]]
Response = Tuple[int, str, bytes]


# Parents: CatalogRequestHandler.render_search
# Keywords: query string, limit, clamp, parse
def parse_search_query(query_string: str) -> Tuple[str, int]:
    assert isinstance(query_string, str)
    fields = urllib.parse.parse_qs(query_string, keep_blank_values=True)
    query = fields.get("q", [""])[0].strip()[:MAX_QUERY_LENGTH]
    try:
        limit = int(fields.get("limit", [str(DEFAULT_SEARCH_LIMIT)])[0])
    except ValueError:
        limit = DEFAULT_SEARCH_LIMIT
    limit = max(1, min(limit, MAX_SEARCH_LIMIT))
    assert 1 <= limit <= MAX_SEARCH_LIMIT and len(query) <= MAX_QUERY_LENGTH
    return query, limit


# Parents: CatalogRequestHandler.render_torrent
# Keywords: hex, info hash, validate, parse
def parse_info_hash_hex(text: str) -> Optional[bytes]:
    assert isinstance(text, str)
    result: Optional[bytes] = None
    if len(text) == NODE_ID_LENGTH * 2 and all(character in string.hexdigits for character in text):
        result = bytes.fromhex(text)
    assert result is None or is_valid_node_id(result)
    return result


# Parents: CatalogRequestHandler.render_search, CatalogRequestHandler.render_torrent
# Keywords: magnet, attach, record, format
def attach_magnet(record: Dict[str, Any], name: str) -> Dict[str, Any]:
    assert "info_hash" in record and isinstance(name, str)
    result = dict(record)
    result["magnet"] = build_magnet_link(bytes.fromhex(record["info_hash"]), name)
    assert result["magnet"].startswith("magnet:")
    return result


# Parents: create_web_server, ScraperRuntime.open_browser_page
# Keywords: url, web address, format
def web_url(host: str, port: int) -> str:
    assert host and 0 < port <= 65535
    result = "http://%s:%d/" % (host, port)
    assert result.startswith("http://")
    return result


# Parents: CatalogRequestHandler.render_stats, render_search, render_torrent, render_error
# Keywords: json, encode, ascii, compact
def encode_json(payload: Any) -> bytes:
    assert payload is not None
    result = json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("ascii")
    assert isinstance(result, bytes)
    return result


class CatalogWebServer(http.server.ThreadingHTTPServer):
    """HTTP server that carries the catalog and the stats provider for its handlers."""

    daemon_threads = True
    allow_reuse_address = True

    # Parents: create_web_server
    # Keywords: http server, bind, catalog, stats
    def __init__(self, address: Tuple[str, int], catalog: TorrentCatalog, stats_provider: StatsProvider) -> None:
        assert len(address) == 2 and callable(stats_provider)
        self.catalog = catalog
        self.stats_provider = stats_provider
        super().__init__(address, CatalogRequestHandler)
        assert self.server_address[1] > 0


class CatalogRequestHandler(http.server.BaseHTTPRequestHandler):
    """GET-only request handler."""

    server_version = "dht_scraper/2.0"
    sys_version = ""

    # Parents: http.server (on request)
    # Keywords: GET, route, respond
    def do_GET(self) -> None:  # noqa: N802 (http.server naming)
        assert isinstance(self.path, str)
        parts = urllib.parse.urlsplit(self.path)
        status, content_type, body = self.dispatch(parts.path, parts.query)
        self.send_bytes(status, content_type, body)
        assert status >= 200

    # Parents: do_GET, tests
    # Keywords: routing, pure, dispatch
    def dispatch(self, path: str, query_string: str) -> Response:
        assert isinstance(path, str)
        if path == "/":
            result: Response = (200, CONTENT_HTML, render_index_page())
        elif path == "/api/stats":
            result = self.render_stats()
        elif path == "/api/search":
            result = self.render_search(query_string)
        elif path.startswith(ROUTE_TORRENT_PREFIX):
            result = self.render_torrent(path[len(ROUTE_TORRENT_PREFIX):])
        else:
            result = self.render_error(404, "not found")
        assert len(result) == 3
        return result

    # Parents: dispatch
    # Keywords: stats, json, provider
    def render_stats(self) -> Response:
        assert callable(self.server.stats_provider)
        result = (200, CONTENT_JSON, encode_json(self.server.stats_provider()))
        assert result[0] == 200
        return result

    # Parents: dispatch
    # Keywords: search, json, ranked results
    def render_search(self, query_string: str) -> Response:
        assert isinstance(query_string, str)
        query, limit = parse_search_query(query_string)
        results = [attach_magnet(record, record["name"]) for record in self.server.catalog.search(query, limit)]
        result = (200, CONTENT_JSON, encode_json({"query": query, "limit": limit, "count": len(results), "results": results}))
        assert result[0] == 200
        return result

    # Parents: dispatch
    # Keywords: torrent detail, json, 400, 404
    def render_torrent(self, hex_hash: str) -> Response:
        assert isinstance(hex_hash, str)
        info_hash = parse_info_hash_hex(hex_hash)
        if info_hash is None:
            return self.render_error(400, "invalid info hash")
        record = self.server.catalog.torrent_detail(info_hash)
        if record is None:
            return self.render_error(404, "not found")
        name = record["metadata"]["name"] if record.get("metadata") else ""
        result = (200, CONTENT_JSON, encode_json(attach_magnet(record, name)))
        assert result[0] == 200
        return result

    # Parents: dispatch, render_torrent
    # Keywords: error, json, status
    def render_error(self, status: int, message: str) -> Response:
        assert status >= 400
        result = (status, CONTENT_JSON, encode_json({"error": message}))
        assert result[0] == status
        return result

    # Parents: do_GET
    # Keywords: headers, security, send
    def send_bytes(self, status: int, content_type: str, body: bytes) -> None:
        assert 200 <= status <= 599 and isinstance(body, bytes)
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        if content_type == CONTENT_HTML:
            self.send_header("Content-Security-Policy", CONTENT_SECURITY_POLICY)
        self.end_headers()
        self.wfile.write(body)
        assert True

    # Parents: http.server (on request)
    # Keywords: logging, project logger, user action
    def log_message(self, format: str, *args: Any) -> None:  # noqa: A002 (http.server signature)
        assert isinstance(format, str)
        level = logging.DEBUG if "/api/stats" in getattr(self, "path", "") else logging.INFO
        LOGGER.log(level, "web %s %s", self.address_string(), format % args)
        assert True


# Parents: ScraperRuntime.start_web
# Keywords: web server, bind, create
def create_web_server(host: str, port: int, catalog: TorrentCatalog, stats_provider: StatsProvider) -> CatalogWebServer:
    assert host and 0 <= port <= 65535
    server = CatalogWebServer((host, port), catalog, stats_provider)
    LOGGER.info("web interface listening on %s", web_url(server.server_address[0], server.server_address[1]))
    assert server.server_address[1] > 0
    return server


# Parents: ScraperRuntime.start_web (thread target)
# Keywords: serve forever, poll, thread
def serve_web_server(server: CatalogWebServer) -> None:
    assert isinstance(server, CatalogWebServer)
    LOGGER.info("web server started")
    server.serve_forever(poll_interval=SERVER_POLL_SECONDS)
    LOGGER.info("web server stopped")
    assert True
