"""Deploy category: the scripts against the real Docker engine (DEPLOY-030, DEPLOY-031)."""
import json
import os
import re
import shutil
import signal
import subprocess
import tempfile
import threading
import time
import unittest

from tests.deploy.docker_fixtures import REPO_ROOT, ComposeProject, docker, http_json, requires_docker, wait_for

FAKE_CRONTAB = os.path.join(REPO_ROOT, "tests", "scripts", "fake_bin", "crontab")
GIT_IDENTITY = {"GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "test@example.invalid", "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "test@example.invalid"}


def project_containers(project):
    return docker("ps", "-a", "-q", "--filter", "label=com.docker.compose.project=%s" % project).stdout.split()


def scraper_container(stack, project=None):
    ids = stack.compose("ps", "-q", "scraper", project=project).stdout.split()
    return ids[0] if ids else None


def inspect(container, template):
    return docker("inspect", "-f", template, container).stdout.strip()


@requires_docker
class RunScriptDockerSpecTest(unittest.TestCase):
    def test_DEPLOY_030_run_cleans_up_real_containers(self):
        for case, sent, expected in (("INT", signal.SIGINT, 130), ("HUP", signal.SIGHUP, 129)):
            with self.subTest(case=case):
                directory = tempfile.mkdtemp()
                self.addCleanup(shutil.rmtree, directory)
                stack = ComposeProject(directory)
                self.addCleanup(stack.remove)
                process = subprocess.Popen(["/bin/bash", os.path.join(REPO_ROOT, "scripts", "run.sh")], cwd=REPO_ROOT, env=stack.environment,
                                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
                lines = []
                reader = threading.Thread(target=lambda: lines.extend(process.stdout), daemon=True)
                reader.start()
                try:
                    self.assertTrue(wait_for(lambda: any("http://localhost:" in line for line in lines), timeout=300, step=1.0), "".join(lines))
                    port = re.search(r"http://localhost:(\d+)/", "".join(lines)).group(1)
                    self.assertIn("uptime_seconds", http_json("http://127.0.0.1:%s/api/stats" % port))
                    os.killpg(process.pid, sent)
                    self.assertEqual(process.wait(60), expected)
                finally:
                    if process.poll() is None:
                        os.killpg(process.pid, signal.SIGKILL)
                        process.wait(10)
                    reader.join(10)
                    process.stdout.close()
                self.assertTrue(wait_for(lambda: project_containers(stack.project + "-run") == [], timeout=60))
                self.assertEqual(docker("volume", "inspect", stack.volume, check=False).returncode, 0)


@requires_docker
class ServiceScriptsDockerSpecTest(unittest.TestCase):
    def git(self, *args, cwd):
        environment = dict(os.environ, **GIT_IDENTITY)
        return subprocess.run(["git"] + list(args), cwd=cwd, env=environment, capture_output=True, text=True, check=True, timeout=120).stdout.strip()

    def make_checkout(self, directory):
        source = os.path.join(directory, "source")
        files = self.git("ls-files", "-z", "--cached", "--others", "--exclude-standard", cwd=REPO_ROOT).split("\0")
        for name in filter(None, files):
            if os.path.isfile(os.path.join(REPO_ROOT, name)):
                os.makedirs(os.path.dirname(os.path.join(source, name)), exist_ok=True)
                shutil.copy2(os.path.join(REPO_ROOT, name), os.path.join(source, name))
        self.git("init", "-q", "-b", "main", cwd=source)
        self.git("add", "-A", cwd=source)
        self.git("commit", "-q", "-m", "C", cwd=source)
        origin, checkout = os.path.join(directory, "origin.git"), os.path.join(directory, "checkout")
        self.git("clone", "-q", "--bare", source, origin, cwd=directory)
        self.git("clone", "-q", origin, checkout, cwd=directory)
        return source, checkout

    def push(self, source, path, text, message):
        with open(os.path.join(source, path), "a") as handle:
            handle.write(text)
        self.git("commit", "-q", "-am", message, cwd=source)
        self.git("push", "-q", os.path.join(os.path.dirname(source), "origin.git"), "main", cwd=source)
        return self.git("rev-parse", "HEAD", cwd=source)

    def run_script(self, checkout, environment, *args):
        return subprocess.run(["/bin/bash", os.path.join(checkout, "scripts", args[0])] + list(args[1:]), cwd=checkout, env=environment, capture_output=True, text=True, timeout=900)

    def test_DEPLOY_031_install_update_uninstall(self):
        directory = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, directory)
        stack = ComposeProject(directory)
        self.addCleanup(stack.remove)
        source, checkout = self.make_checkout(directory)
        fake_bin = os.path.join(directory, "bin")
        os.makedirs(fake_bin)
        shutil.copy2(FAKE_CRONTAB, fake_bin)
        with open(os.path.join(fake_bin, "fake-config.json"), "w") as handle:
            json.dump({"crontab": os.path.join(directory, "crontab.txt")}, handle)
        environment = dict(stack.environment, PATH=fake_bin + os.pathsep + stack.environment["PATH"])

        def healthy_scraper():
            container = scraper_container(stack)
            return container if container and inspect(container, "{{.State.Health.Status}}") == "healthy" else None

        first = self.git("rev-parse", "HEAD", cwd=checkout)
        result = self.run_script(checkout, environment, "install-service.sh")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(wait_for(healthy_scraper, timeout=120))
        installed = healthy_scraper()
        self.assertEqual(inspect(installed, "{{.HostConfig.RestartPolicy.Name}}"), "unless-stopped")
        self.assertEqual(inspect(inspect(installed, "{{.Image}}"), '{{index .Config.Labels "org.opencontainers.image.revision"}}'), first)

        second = self.push(source, "dht_scraper/__init__.py", "# update test\n", "C2")
        result = self.run_script(checkout, environment, "update.sh")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertTrue(wait_for(healthy_scraper, timeout=120))
        updated = healthy_scraper()
        self.assertNotEqual(updated, installed)
        self.assertEqual(inspect(inspect(updated, "{{.Image}}"), '{{index .Config.Labels "org.opencontainers.image.revision"}}'), second)

        self.push(source, "Dockerfile", "RUN false\n", "C3 breaks the build")
        result = self.run_script(checkout, environment, "update.sh")
        self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
        self.assertEqual(scraper_container(stack), updated)
        self.assertEqual(inspect(updated, "{{.State.Running}}"), "true")
        self.assertEqual(self.git("rev-parse", "HEAD", cwd=checkout), second)

        result = self.run_script(checkout, environment, "install-service.sh", "--uninstall")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(project_containers(stack.project), [])
        self.assertEqual(docker("volume", "inspect", stack.volume, check=False).returncode, 0)


if __name__ == "__main__":
    unittest.main()
