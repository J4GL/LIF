"""Runs JavaScript in the page with the Obscura headless browser (https://github.com/h4ckf0r0day/obscura)."""
import json
import os
import shutil
import subprocess
import unittest

OBSCURA_VERSION = "0.2.3"


def find_obscura():
    candidates = [os.environ.get("OBSCURA_BIN"), shutil.which("obscura"), os.path.expanduser("~/.local/bin/obscura")]
    return next((path for path in candidates if path and os.access(path, os.X_OK)), None)


OBSCURA = find_obscura()
requires_browser = unittest.skipUnless(OBSCURA, "install Obscura %s (OBSCURA_BIN, PATH or ~/.local/bin) to run the page tests" % OBSCURA_VERSION)


def evaluate(url, expression, timeout=60):
    """Loads url, waits for the page to settle, evaluates expression and returns its JSON value."""
    result = subprocess.run(
        [OBSCURA, "fetch", url, "--allow-private-network", "--quiet", "--timeout", "20", "--eval", "JSON.stringify((function () { %s })())" % expression],
        capture_output=True, text=True, timeout=timeout,
    )
    if result.returncode != 0:
        raise AssertionError("obscura failed (%d): %s" % (result.returncode, result.stderr.strip()[-2000:]))
    return json.loads(result.stdout.strip())
