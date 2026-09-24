"""CLI category: argument parsing, settings mapping and start failures (F-006)."""
import contextlib
import io
import os
import socket
import tempfile
import unittest

from dht_scraper.__main__ import build_settings, main, parse_arguments, run_scraper


class ParseArgumentsTest(unittest.TestCase):
    def test_parse_f006_t1(self):
        args = parse_arguments(["--port", "7000", "--nodes", "3", "--web-port", "9000", "--duration", "5", "--no-fetch", "--no-browser"])
        self.assertEqual((args.port, args.nodes, args.web_port, args.duration, args.no_fetch, args.no_browser), (7000, 3, 9000, 5.0, True, True))

    def test_defaults(self):
        args = parse_arguments([])
        self.assertEqual((args.port, args.nodes, args.web_host, args.web_port), (6881, 8, "127.0.0.1", 8080))
        self.assertEqual((args.fetch_workers, args.batch_size, args.interval, args.duration), (256, 6, 0.1, None))
        self.assertFalse(args.verbose or args.no_fetch or args.no_browser)
        self.assertIsNone(args.log_file)

    def test_invalid_values_exit(self):
        invalid = [
            ["--port", "70000"], ["--nodes", "0"], ["--nodes", "65"], ["--port", "65530", "--nodes", "8"], ["--web-host", ""],
            ["--web-port", "-1"], ["--fetch-workers", "0"], ["--fetch-workers", "1025"], ["--batch-size", "0"],
            ["--interval", "0"], ["--interval", "nan"], ["--interval", "inf"], ["--duration", "-1"], ["--duration", "nan"],
        ]
        for argv in invalid:
            with self.assertRaises(SystemExit, msg=repr(argv)), contextlib.redirect_stderr(io.StringIO()):
                parse_arguments(argv)

    def test_CLI_001_new_options(self):
        defaults = parse_arguments([])
        self.assertEqual((defaults.fetch_workers, defaults.batch_size, defaults.ipv6), (256, 6, False))
        run_settings = build_settings(parse_arguments(["--ipv6", "--fetch-workers", "1024"]))
        self.assertEqual((run_settings.ipv6_enabled, run_settings.fetch_workers), (True, 1024))
        with self.assertRaises(SystemExit), contextlib.redirect_stderr(io.StringIO()):
            parse_arguments(["--fetch-workers", "1025"])

    def test_build_settings_maps_arguments(self):
        run_settings = build_settings(parse_arguments(["--nodes", "2", "--no-fetch", "--no-browser", "--fetch-workers", "3"]))
        self.assertEqual((run_settings.nodes, run_settings.fetch_enabled, run_settings.open_browser, run_settings.fetch_workers), (2, False, False, 3))


class StartFailureTest(unittest.TestCase):
    def test_run_scraper_returns_1_when_web_port_in_use(self):
        blocker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        blocker.bind(("127.0.0.1", 0))
        blocker.listen(1)
        try:
            args = parse_arguments(["--port", "0", "--nodes", "1", "--web-port", str(blocker.getsockname()[1]), "--no-browser", "--duration", "0"])
            self.assertEqual(run_scraper(args), 1)
        finally:
            blocker.close()

    def test_main_returns_1_when_log_file_cannot_open(self):
        with tempfile.TemporaryDirectory() as directory, contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(main(["--log-file", os.path.join(directory, "missing", "x.log"), "--duration", "0"]), 1)


if __name__ == "__main__":
    unittest.main()
