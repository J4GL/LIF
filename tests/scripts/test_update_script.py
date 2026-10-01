"""Scripts category: scripts/update.sh with the fake docker, a bare origin and the fake crontab."""
import os
import shlex
import subprocess
import time
import unittest

from tests.scripts.script_fixtures import PROJECT, ScriptSandbox, read_text, wait_until


def installed(test, running=True):
    sandbox = ScriptSandbox.make(test)
    result = sandbox.run("install-service.sh")
    test.assertEqual(result.returncode, 0, result.stderr)
    if not running:
        with sandbox.state() as state:
            for item in state["containers"].values():
                if item["service"] == "scraper":
                    item["running"] = False
    sandbox.clear_calls()
    return sandbox


def subs(sandbox, project=PROJECT):
    return [call["sub"] for call in sandbox.compose_calls(project) if call["sub"] in ("pull", "build", "up", "down", "kill", "stop")]


class UpdateScriptSpecTest(unittest.TestCase):
    def test_DEPLOY_015_up_to_date(self):
        sandbox = installed(self)
        commit = sandbox.head()
        result = sandbox.run("update.sh")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("up to date", result.stdout)
        self.assertIn(commit[:12], result.stdout)
        self.assertEqual(sandbox.state_changing_calls(), [])
        self.assertEqual(sandbox.head(), commit)

    def test_DEPLOY_016_update_builds_then_restarts(self):
        for case in ("running", "stopped"):
            with self.subTest(case=case):
                sandbox = installed(self, running=case == "running")
                new = sandbox.push_commit()
                result = sandbox.run("update.sh")
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                self.assertEqual(sandbox.head(), new)
                calls = sandbox.compose_calls(PROJECT)
                build = next(call for call in calls if call["sub"] == "build")
                self.assertIn("--pull", build["argv"])
                self.assertEqual(build["revision"], new)
                before_build = [call["sub"] for call in calls[:calls.index(build)]]
                self.assertFalse(set(before_build) & {"down", "stop", "kill", "up"}, before_build)
                if case == "running":
                    self.assertEqual(subs(sandbox), ["build", "up"])
                    up = next(call for call in calls if call["sub"] == "up")
                    for flag in ("-d", "--wait", "--remove-orphans"):
                        self.assertIn(flag, up["argv"])
                    self.assertEqual(sandbox.deployed_revision(), new)
                else:
                    self.assertEqual(subs(sandbox), ["build"])
                    self.assertEqual(sandbox.containers(PROJECT, "scraper", running_only=True), {})

    def test_DEPLOY_017_refusals(self):
        for case, word in (("docker", "Docker"), ("fetch", "fetch"), ("dirty", "local changes"), ("diverged", "diverged")):
            with self.subTest(case=case):
                sandbox = installed(self)
                sandbox.push_commit()
                tracked = os.path.join(sandbox.repo, "compose.yaml")
                if case == "docker":
                    with sandbox.state() as state:
                        state["daemon"] = False
                elif case == "fetch":
                    sandbox.git("remote", "set-url", "origin", os.path.join(sandbox.root, "missing.git"))
                elif case == "dirty":
                    with open(tracked, "a") as handle:
                        handle.write("# local edit\n")
                else:
                    with open(os.path.join(sandbox.repo, "local.txt"), "w") as handle:
                        handle.write("local\n")
                    sandbox.git("add", "local.txt")
                    sandbox.git("commit", "-q", "-m", "local")
                head = sandbox.head()
                content = read_text(tracked)
                sandbox.clear_calls()
                result = sandbox.run("update.sh")
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn(word, result.stderr)
                self.assertEqual((sandbox.head(), read_text(tracked)), (head, content))
                self.assertEqual(sandbox.state_changing_calls(), [])

    def test_DEPLOY_018_rollback(self):
        for case in ("build", "up"):
            with self.subTest(case=case):
                sandbox = installed(self)
                old = sandbox.head()
                new = sandbox.push_commit()
                with sandbox.state() as state:
                    state["fail"][case] = [new]
                result = sandbox.run("update.sh")
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertEqual(sandbox.head(), old)
                calls = sandbox.compose_calls(PROJECT)
                self.assertEqual([call for call in calls if call["sub"] == "build"][-1]["revision"], old)
                last = calls[-1]
                self.assertEqual(last["sub"], "up")
                for flag in ("-d", "--wait"):
                    self.assertIn(flag, last["argv"])
                if case == "build":
                    self.assertFalse([call for call in calls if call["sub"] == "up" and call["revision"] == new])
                self.assertEqual(sandbox.deployed_revision(), old)

    def test_DEPLOY_019_interrupted_update_resumed(self):
        sandbox = installed(self)
        new = sandbox.push_commit()
        sandbox.git("fetch", "-q")
        sandbox.git("merge", "-q", "--ff-only", "origin/main")
        result = sandbox.run("update.sh")
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        self.assertEqual(subs(sandbox), ["build", "up"])
        self.assertEqual(sandbox.compose_calls(PROJECT, "build")[0]["revision"], new)
        self.assertEqual(sandbox.deployed_revision(), new)

    def test_DEPLOY_020_cron_line_runs(self):
        sandbox = installed(self)
        line = sandbox.crontab().strip()
        command = line.split(None, 5)[5]
        result = subprocess.run(["env", "-i", "HOME=" + sandbox.home, "/bin/sh", "-c", command], cwd="/", capture_output=True, text=True, timeout=30)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        log = read_text(os.path.join(sandbox.log_dir, "update.log")).strip().splitlines()
        self.assertIn("up to date", log[-1])

    def test_DEPLOY_021_lock(self):
        with self.subTest(case="live"):
            sandbox = installed(self)
            holder = subprocess.Popen(["sleep", "30"])
            self.addCleanup(holder.wait)
            self.addCleanup(holder.kill)
            lock = os.path.join(sandbox.repo, ".git", "dht-scraper.lock")
            os.mkdir(lock)
            with open(os.path.join(lock, "pid"), "w") as handle:
                handle.write("%d\n" % holder.pid)
            for script in ("update.sh", "install-service.sh"):
                started = time.monotonic()
                result = sandbox.run(script, timeout=10)
                self.assertLess(time.monotonic() - started, 5.0)
                self.assertEqual(result.returncode, 1, script)
                self.assertIn(str(holder.pid), result.stderr)
            self.assertEqual(sandbox.state_changing_calls(), [])
        with self.subTest(case="stale"):
            sandbox = installed(self)
            finished = subprocess.Popen(["true"])
            finished.wait()
            lock = os.path.join(sandbox.repo, ".git", "dht-scraper.lock")
            os.mkdir(lock)
            with open(os.path.join(lock, "pid"), "w") as handle:
                handle.write("%d\n" % finished.pid)
            result = sandbox.run("update.sh")
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse(os.path.exists(lock))


if __name__ == "__main__":
    unittest.main()
