"""Deploy category: the image, the compose configuration and the running stack."""
import json
import os
import shutil
import tempfile
import time
import unittest

from tests.deploy.docker_fixtures import REPO_ROOT, ComposeProject, clean_environment, docker, http_json, requires_docker, unique_name, wait_for
from tests.search.database_fixtures import document


@requires_docker
class DockerImageSpecTest(unittest.TestCase):
    def test_DEPLOY_001_image_contents(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory)
        shutil.copy(os.path.join(REPO_ROOT, "Dockerfile"), directory)
        shutil.copy(os.path.join(REPO_ROOT, ".dockerignore"), directory)
        shutil.copytree(os.path.join(REPO_ROOT, "dht_scraper"), os.path.join(directory, "dht_scraper"), ignore=shutil.ignore_patterns("__pycache__"))
        for name in (".env", "secret.txt"):
            with open(os.path.join(directory, name), "w") as handle:
                handle.write("SECRET=do-not-ship\n")
        tag = unique_name("dht-image-test") + ":test"
        self.addCleanup(docker, "image", "rm", "-f", tag, check=False)
        docker("build", "-q", "--build-arg", "REVISION=abc123", "-t", tag, directory, env=clean_environment())
        listing = docker("run", "--rm", "--entrypoint", "ls", tag, "-A", "/app").stdout.split()
        self.assertEqual(listing, ["dht_scraper"])
        config = json.loads(docker("image", "inspect", "-f", "{{json .Config}}", tag).stdout)
        self.assertEqual(config["User"], "10001:10001")
        self.assertIn("DHT_DATABASE=/data/torrents.sqlite3", config["Env"])
        owner = docker("run", "--rm", "--entrypoint", "stat", tag, "-c", "%u:%g", "/data").stdout.strip()
        self.assertEqual(owner, "10001:10001")
        self.assertEqual(config["Entrypoint"], ["python3", "-m", "dht_scraper", "--no-browser", "--web-host", "0.0.0.0"])
        self.assertIn("/api/stats", " ".join(config["Healthcheck"]["Test"]))
        self.assertEqual(config["Labels"]["org.opencontainers.image.revision"], "abc123")
        self.assertEqual(docker("run", "--rm", tag, "--help", check=False).returncode, 0)


@requires_docker
class ComposeConfigSpecTest(unittest.TestCase):
    def config(self, env_lines, **environment):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory)
        env_file = os.path.join(directory, "config.env")
        with open(env_file, "w") as handle:
            handle.write("".join(line + "\n" for line in env_lines))
        return docker("compose", "--project-directory", REPO_ROOT, "--env-file", env_file, "-p", "dht-scraper", "config", "--format", "json",
                      check=False, env=clean_environment(**environment), cwd=REPO_ROOT)

    def test_DEPLOY_002_compose_configuration(self):
        with self.subTest(case="defaults"):
            result = self.config([])
            self.assertEqual((result.returncode, result.stderr), (0, ""))
            config = json.loads(result.stdout)
            self.assertEqual(sorted(config["services"]), ["scraper"])
            scraper = config["services"]["scraper"]
            web = [port for port in scraper["ports"] if port["target"] == 8080]
            self.assertEqual([(port["host_ip"], port["published"], port.get("protocol", "tcp")) for port in web], [("0.0.0.0", "8080", "tcp")])
            udp = sorted((port["target"], int(port["published"])) for port in scraper["ports"] if port.get("protocol") == "udp")
            self.assertEqual(udp, [(port, port) for port in range(6881, 6889)])
            self.assertEqual([(mount["source"], mount["target"]) for mount in scraper["volumes"]], [("data", "/data")])
            volume = config["volumes"]["data"]
            self.assertEqual((volume["external"], volume["name"]), (True, "dht-scraper-data"))
            self.assertEqual(scraper["restart"], "unless-stopped")
            self.assertEqual(scraper["logging"]["options"], {"max-size": "10m", "max-file": "3"})
        with self.subTest(case="overrides"):
            result = self.config(["DHT_WEB_BIND=127.0.0.1", "DHT_WEB_PORT=9090"], DHT_RESTART="no")
            config = json.loads(result.stdout)
            web = [port for port in config["services"]["scraper"]["ports"] if port["target"] == 8080]
            self.assertEqual([(port["host_ip"], port["published"]) for port in web], [("127.0.0.1", "9090")])
            self.assertEqual({service["restart"] for service in config["services"].values()}, {"no"})


@requires_docker
class ComposeStackSpecTest(unittest.TestCase):
    def test_DEPLOY_003_stack_search_and_persistence(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory)
        stack = ComposeProject(directory)
        self.addCleanup(stack.remove)
        docker("volume", "create", stack.volume)
        stack.compose("up", "-d", "--build", "--wait", "--wait-timeout", "180")
        web = stack.published("scraper", 8080)
        self.assertTrue(wait_for(lambda: http_json(web + "/api/stats")["index_documents"] == 0, timeout=60))
        self.assertEqual(http_json(web + "/api/search?q=persist")["engine"], "sqlite")
        stack.compose("down", "--remove-orphans")
        writer = "import json, sys; from dht_scraper.torrent_database import TorrentDatabase; d = TorrentDatabase('/data/torrents.sqlite3'); d.open_writer(); d.write_documents([json.loads(sys.argv[1])]); d.close()"
        docker("run", "--rm", "--entrypoint", "python3", "-v", stack.volume + ":/data", stack.image, "-c", writer, json.dumps(document(77, "persist me")))
        stack.compose("up", "-d", "--wait", "--wait-timeout", "180")
        web = stack.published("scraper", 8080)
        for query in ("persist", "persits"):
            self.assertTrue(wait_for(lambda: [row["name"] for row in http_json(web + "/api/search?q=" + query)["results"]] == ["persist me"], timeout=60), query)
        started = time.monotonic()
        stack.compose("stop", "scraper")
        self.assertLess(time.monotonic() - started, 30.0)
        lines = stack.compose("logs", "--no-log-prefix", "scraper").stdout.strip().splitlines()
        self.assertIn("finished: reason=terminate", lines[-1])


if __name__ == "__main__":
    unittest.main()
