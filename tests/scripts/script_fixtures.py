"""Sandbox for the script tests: a bare origin, a clone holding the scripts, fake docker and crontab."""
import contextlib
import fcntl
import json
import os
import shutil
import subprocess
import tempfile
import threading

from tests.shared_fixtures import wait_until

HERE = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HERE))
BASH = "/bin/bash"
PROJECT = "dht-test"
RUN_PROJECT = PROJECT + "-run"
TRACKED_FILES = ("scripts/common.sh", "scripts/run.sh", "scripts/install-service.sh", "scripts/update.sh", "compose.yaml", "Dockerfile", "dht_scraper/__init__.py")
STATE_CHANGING_COMPOSE = ("pull", "build", "up", "down", "kill", "stop", "start", "rm")
GIT_IDENTITY = {"GIT_AUTHOR_NAME": "test", "GIT_AUTHOR_EMAIL": "test@example.invalid", "GIT_COMMITTER_NAME": "test", "GIT_COMMITTER_EMAIL": "test@example.invalid"}
__all__ = ["BASH", "PROJECT", "RUN_PROJECT", "ScriptSandbox", "read_text", "wait_until"]


def read_text(path):
    with open(path) as handle:
        return handle.read()


class ScriptSandbox:
    """One temporary checkout of the scripts with its own fake Docker state, crontab, home and logs."""

    def __init__(self, root):
        self.root = root
        self.repo = os.path.join(root, "repo")
        self.origin = os.path.join(root, "origin.git")
        self.home = os.path.join(root, "home")
        self.log_dir = os.path.join(root, "logs")
        self.env_file = os.path.join(root, "test.env")
        self.crontab_path = os.path.join(root, "crontab")
        self.bin_dir = os.path.join(root, "bin")
        self.state_path = os.path.join(root, "docker-state.json")
        self.log_path = os.path.join(root, "docker-calls.jsonl")
        self.cleanups = []
        os.makedirs(self.home)
        shutil.copytree(os.path.join(HERE, "fake_bin"), self.bin_dir)
        with open(os.path.join(self.bin_dir, "fake-config.json"), "w") as handle:
            json.dump({"state": self.state_path, "log": self.log_path, "crontab": self.crontab_path}, handle)
        with open(self.state_path, "w") as handle:
            json.dump({"daemon": True, "containers": {}, "images": {}, "tags": {}, "volumes": [], "fail": {}, "counter": 0}, handle)
        open(self.log_path, "w").close()
        environment = {key: value for key, value in os.environ.items() if not key.startswith(("DHT_", "COMPOSE_", "GIT_"))}
        environment.update(GIT_IDENTITY)
        environment.update(PATH=self.bin_dir + os.pathsep + os.environ.get("PATH", "/usr/bin:/bin"), HOME=self.home, DHT_PROJECT=PROJECT, DHT_ENV_FILE=self.env_file, DHT_LOG_DIR=self.log_dir)
        self.environment = environment
        self.make_repositories()

    @classmethod
    def make(cls, test):
        root = tempfile.mkdtemp(prefix="dht-scripts-")
        test.addCleanup(shutil.rmtree, root, True)
        sandbox = cls(os.path.realpath(root))
        test.addCleanup(sandbox.close)
        return sandbox

    def make_repositories(self):
        source = os.path.join(self.root, "source")
        for relative in TRACKED_FILES:
            os.makedirs(os.path.dirname(os.path.join(source, relative)), exist_ok=True)
            shutil.copy2(os.path.join(REPO_ROOT, relative), os.path.join(source, relative))
        self.run_git(source, "init", "-q", "-b", "main")
        self.run_git(source, "add", "-A")
        self.run_git(source, "commit", "-q", "-m", "initial")
        self.run_git(self.root, "clone", "-q", "--bare", source, self.origin)
        self.run_git(self.root, "clone", "-q", self.origin, self.repo)
        shutil.rmtree(source)

    def run_git(self, cwd, *args):
        result = subprocess.run(["git"] + list(args), cwd=cwd, env=self.environment, capture_output=True, text=True, timeout=30)
        if result.returncode != 0:
            raise AssertionError("git %s failed: %s" % (" ".join(args), result.stderr))
        return result.stdout.strip()

    def git(self, *args):
        return self.run_git(self.repo, *args)

    def head(self):
        return self.git("rev-parse", "HEAD")

    def push_commit(self):
        """Commits a change of dht_scraper/__init__.py to the origin from another clone; returns its hash."""
        work = tempfile.mkdtemp(dir=self.root)
        self.run_git(self.root, "clone", "-q", self.origin, work)
        with open(os.path.join(work, "dht_scraper", "__init__.py"), "a") as handle:
            handle.write("# change %s\n" % os.path.basename(work))
        self.run_git(work, "commit", "-q", "-am", "change")
        self.run_git(work, "push", "-q", "origin", "main")
        commit = self.run_git(work, "rev-parse", "HEAD")
        shutil.rmtree(work)
        return commit

    def script(self, name):
        return os.path.join(self.repo, "scripts", name)

    def run(self, name, *args, timeout=60):
        return subprocess.run([BASH, self.script(name)] + list(args), cwd=self.repo, env=self.environment, capture_output=True, text=True, timeout=timeout)

    def start(self, name, *args):
        """Starts a script in its own session; returns the process and the list its output lines go to."""
        process = subprocess.Popen([BASH, self.script(name)] + list(args), cwd=self.repo, env=self.environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
        lines = []

        def collect():
            for line in process.stdout:
                lines.append(line)
            process.stdout.close()

        reader = threading.Thread(target=collect, daemon=True)
        reader.start()

        def stop():
            if process.poll() is None:
                process.kill()
            process.wait(10)
            reader.join(5)

        self.cleanups.append(stop)
        return process, lines

    def close(self):
        for cleanup in reversed(self.cleanups):
            cleanup()

    def read_state(self):
        with open(self.state_path) as handle:
            return json.load(handle)

    @contextlib.contextmanager
    def state(self):
        with open(self.state_path + ".lock", "a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            state = self.read_state()
            yield state
            with open(self.state_path + ".tmp", "w") as handle:
                json.dump(state, handle)
            os.replace(self.state_path + ".tmp", self.state_path)

    def add_container(self, project, service="scraper", running=True):
        with self.state() as state:
            state["counter"] += 1
            image = "img%04d" % state["counter"]
            state["images"][image] = {"revision": "unknown"}
            state["counter"] += 1
            state["containers"]["ctr%04d" % state["counter"]] = {"project": project, "service": service, "image": image, "running": running, "exit_code": 0}

    def containers(self, project, service=None, running_only=False):
        return {cid: item for cid, item in self.read_state()["containers"].items() if item["project"] == project and (service is None or item["service"] == service) and (item["running"] or not running_only)}

    def deployed_revision(self):
        state = self.read_state()
        running = self.containers(PROJECT, "scraper", running_only=True)
        image = next(iter(running.values()))["image"] if running else state["tags"].get("dht-scraper:local")
        return None if image is None else state["images"][image]["revision"]

    def calls(self):
        with open(self.log_path) as handle:
            return [json.loads(line) for line in handle if line.strip()]

    def clear_calls(self):
        open(self.log_path, "w").close()

    def compose_calls(self, project, sub=None):
        return [call for call in self.calls() if call["argv"][0] == "compose" and call["project"] == project and (sub is None or call["sub"] == sub)]

    def state_changing_calls(self):
        result = []
        for call in self.calls():
            if call["argv"][0] == "compose" and call["sub"] in STATE_CHANGING_COMPOSE:
                result.append(call)
            elif call["argv"][:2] == ["volume", "create"]:
                result.append(call)
        return result

    def crontab(self):
        return read_text(self.crontab_path) if os.path.exists(self.crontab_path) else ""
