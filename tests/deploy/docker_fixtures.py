"""Helpers for the deploy category: it needs Docker and the network, and runs only with DHT_DEPLOY_TESTS=1."""
import json
import os
import secrets
import subprocess
import time
import unittest
import urllib.error
import urllib.request

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
ENABLED = os.environ.get("DHT_DEPLOY_TESTS") == "1"
requires_docker = unittest.skipUnless(ENABLED, "set DHT_DEPLOY_TESTS=1 to run the Docker tests (they need Docker and the network)")


def docker(*args, check=True, timeout=600, env=None, cwd=None):
    result = subprocess.run(["docker"] + list(args), capture_output=True, text=True, timeout=timeout, env=env, cwd=cwd)
    if check and result.returncode != 0:
        raise AssertionError("docker %s failed (%d): %s" % (" ".join(args), result.returncode, result.stderr.strip()))
    return result


def unique_name(prefix):
    return "%s-%s" % (prefix, secrets.token_hex(4))


def wait_for(condition, timeout, step=0.5):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if condition():
                return True
        except Exception:
            pass
        time.sleep(step)
    return bool(condition())


def http_json(url, timeout=3.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        error.close()
        raise


def clean_environment(**overrides):
    """The test process environment without DHT_* and COMPOSE_* settings, plus overrides."""
    result = {key: value for key, value in os.environ.items() if not key.startswith(("DHT_", "COMPOSE_"))}
    result["DOCKER_CLI_HINTS"] = "false"
    result.update(overrides)
    return result


class ComposeProject:
    """A uniquely named compose project with the test overlay, its own env file, volume and image tag."""

    def __init__(self, directory, prefix="dht-test"):
        self.project = unique_name(prefix)
        self.image = "%s:test" % self.project
        self.env_file = os.path.join(directory, "test.env")
        with open(self.env_file, "w") as handle:
            handle.write("# deploy test settings\n")
        self.environment = clean_environment(
            DHT_PROJECT=self.project, DHT_IMAGE=self.image, DHT_ENV_FILE=self.env_file,
            COMPOSE_FILE="compose.yaml:tests/deploy/compose.test.yaml", DHT_LOG_DIR=os.path.join(directory, "logs"),
        )

    @property
    def volume(self):
        return self.project + "-data"

    def compose(self, *args, project=None, check=True, timeout=600):
        return docker("compose", "--project-directory", REPO_ROOT, "--env-file", self.env_file, "-p", project or self.project, *args, check=check, timeout=timeout, env=self.environment, cwd=REPO_ROOT)

    def published(self, service, port, project=None):
        address = self.compose("port", service, str(port), project=project).stdout.strip().splitlines()[0]
        return "http://127.0.0.1:%s" % address.rsplit(":", 1)[1]

    def remove(self):
        for project in (self.project, self.project + "-run"):
            self.compose("down", "--remove-orphans", "-t", "5", project=project, check=False)
        docker("volume", "rm", "-f", self.volume, check=False)
        docker("image", "rm", "-f", self.image, self.image + "-run", check=False)
