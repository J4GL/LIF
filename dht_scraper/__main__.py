"""Command line entry point: python3 -m dht_scraper."""
import argparse
import logging
import sys
from typing import List, Optional

from dht_scraper.dht_crawler import DEFAULT_BOOTSTRAP_NODES
from dht_scraper.event_log import LOGGER_NAME, configure_logging
from dht_scraper.fetch_engine import DEFAULT_MAX_CONNECTIONS
from dht_scraper.metadata_fetcher import fetch_metadata
from dht_scraper.scraper_runtime import DEFAULT_BATCH_SIZE, DEFAULT_INTERVAL, DEFAULT_NODES, DEFAULT_PORT, RuntimeSettings, ScraperRuntime, settings_error
from dht_scraper.stream_encryption import fetch_metadata_encrypted
from dht_scraper.utp_transport import fetch_metadata_utp
from dht_scraper.torrent_catalog import TorrentCatalog
from dht_scraper.web_interface import DEFAULT_WEB_HOST, DEFAULT_WEB_PORT

LOGGER = logging.getLogger(LOGGER_NAME)


# Parents: main
# Keywords: argparse, cli, options, validation
def parse_arguments(argv: List[str]) -> argparse.Namespace:
    assert isinstance(argv, list), "argv must be a list"
    parser = argparse.ArgumentParser(prog="dht_scraper", description="Educational BitTorrent DHT scraper with metadata fetching and a web UI. Nothing is stored on disk.")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help="first UDP port; each node uses the next port (default %d, 0 = OS chosen)" % DEFAULT_PORT)
    parser.add_argument("--nodes", type=int, default=DEFAULT_NODES, help="number of simulated DHT nodes (default %d)" % DEFAULT_NODES)
    parser.add_argument("--web-host", default=DEFAULT_WEB_HOST, help="web UI bind address (default %s)" % DEFAULT_WEB_HOST)
    parser.add_argument("--web-port", type=int, default=DEFAULT_WEB_PORT, help="web UI port (default %d, 0 = OS chosen)" % DEFAULT_WEB_PORT)
    parser.add_argument("--duration", type=float, default=None, help="seconds to run (default: until Ctrl-C)")
    parser.add_argument("--fetch-workers", type=int, default=DEFAULT_MAX_CONNECTIONS, help="simultaneous metadata connections (default %d)" % DEFAULT_MAX_CONNECTIONS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="crawl queries per node per interval (default %d)" % DEFAULT_BATCH_SIZE)
    parser.add_argument("--interval", type=float, default=DEFAULT_INTERVAL, help="seconds between crawl batches (default %.1f)" % DEFAULT_INTERVAL)
    parser.add_argument("--log-file", default=None, help="also write the log to this file")
    parser.add_argument("--verbose", action="store_true", help="DEBUG logging (every hash, every request)")
    parser.add_argument("--no-fetch", action="store_true", help="crawl only, do not fetch metadata")
    parser.add_argument("--no-browser", action="store_true", help="do not open the web UI in the browser")
    parser.add_argument("--ipv6", action="store_true", help="also crawl the IPv6 DHT (BEP 32): more hashes, about 20 %% more UDP traffic")
    args = parser.parse_args(argv)
    problem = settings_error(args.port, args.nodes, args.web_host, args.web_port, args.duration, args.fetch_workers, args.batch_size, args.interval)
    if problem is not None:
        parser.error(problem)
    assert isinstance(args, argparse.Namespace)
    return args


# Parents: run_scraper, tests
# Keywords: settings, mapping, arguments
def build_settings(args: argparse.Namespace) -> RuntimeSettings:
    assert hasattr(args, "port") and hasattr(args, "nodes")
    result = RuntimeSettings(
        port=args.port,
        nodes=args.nodes,
        web_host=args.web_host,
        web_port=args.web_port,
        duration=args.duration,
        fetch_workers=args.fetch_workers,
        batch_size=args.batch_size,
        interval=args.interval,
        fetch_enabled=not args.no_fetch,
        open_browser=not args.no_browser,
        ipv6_enabled=args.ipv6,
    )
    assert result.nodes == args.nodes
    return result


# Parents: main
# Keywords: run, runtime, exit code
def run_scraper(args: argparse.Namespace) -> int:
    assert hasattr(args, "duration")
    settings = build_settings(args)
    runtime = ScraperRuntime(settings, TorrentCatalog(), fetch_metadata, DEFAULT_BOOTSTRAP_NODES, retry_function=fetch_metadata_encrypted, utp_function=fetch_metadata_utp)
    try:
        runtime.run(settings.duration)
    except OSError as error:
        LOGGER.error("cannot start: %s", error)
        return 1
    assert runtime.stopped
    return 0


# Parents: none (entry point)
# Keywords: main, entry point, exit code
def main(argv: Optional[List[str]] = None) -> int:
    assert argv is None or isinstance(argv, list)
    args = parse_arguments(sys.argv[1:] if argv is None else argv)
    try:
        configure_logging(args.log_file, args.verbose)
    except OSError as error:
        print("cannot open log file %s: %s" % (args.log_file, error), file=sys.stderr)
        return 1
    exit_code = run_scraper(args)
    assert isinstance(exit_code, int)
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
