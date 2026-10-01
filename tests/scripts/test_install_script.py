"""Scripts category: scripts/install-service.sh with the fake docker and crontab."""
import os
import unittest

from tests.scripts.script_fixtures import PROJECT, RUN_PROJECT, ScriptSandbox

UNRELATED = "0 1 * * * /usr/bin/true # unrelated\n"


def expected_cron_line(sandbox):
    def quoted(value):
        return "'" + value.replace("'", "'\\''") + "'"

    return "17 4 * * * PATH=%s DHT_PROJECT=%s DHT_ENV_FILE=%s DHT_LOG_DIR=%s /bin/bash %s >> %s 2>&1 # dht-scraper-update:%s" % (
        quoted(sandbox.environment["PATH"]), quoted(PROJECT), quoted(sandbox.env_file), quoted(sandbox.log_dir),
        quoted(os.path.join(sandbox.repo, "scripts", "update.sh")), quoted(os.path.join(sandbox.log_dir, "update.log")), sandbox.repo,
    )


def ordered(calls, subs):
    names = [call["sub"] for call in calls if call["sub"] in subs]
    return names


class InstallScriptSpecTest(unittest.TestCase):
    def test_DEPLOY_013_install_service(self):
        with self.subTest(case="install twice"):
            sandbox = ScriptSandbox.make(self)
            commit = sandbox.head()
            for _ in range(2):
                sandbox.clear_calls()
                result = sandbox.run("install-service.sh")
                self.assertEqual(result.returncode, 0, result.stderr)
                calls = sandbox.compose_calls(PROJECT)
                self.assertEqual(ordered(calls, ("pull", "build", "up")), ["build", "up"])
                build, up = (next(call for call in calls if call["sub"] == sub) for sub in ("build", "up"))
                self.assertEqual(build["revision"], commit)
                for flag in ("-d", "--wait", "--remove-orphans"):
                    self.assertIn(flag, up["argv"])
                self.assertIn("http://", result.stdout)
            self.assertTrue(os.path.exists(sandbox.env_file))
            self.assertIn(PROJECT + "-data", sandbox.read_state()["volumes"])
            self.assertEqual(sandbox.crontab(), expected_cron_line(sandbox) + "\n")
        with self.subTest(case="temporary run active"):
            sandbox = ScriptSandbox.make(self)
            sandbox.add_container(RUN_PROJECT)
            result = sandbox.run("install-service.sh")
            self.assertEqual(result.returncode, 1)
            self.assertIn(RUN_PROJECT, result.stderr)
            self.assertEqual(sandbox.state_changing_calls(), [])

    def test_DEPLOY_014_uninstall_service(self):
        sandbox = ScriptSandbox.make(self)
        self.assertEqual(sandbox.run("install-service.sh").returncode, 0)
        with open(sandbox.crontab_path, "a") as handle:
            handle.write(UNRELATED)
        sandbox.clear_calls()
        result = sandbox.run("install-service.sh", "--uninstall")
        self.assertEqual(result.returncode, 0, result.stderr)
        downs = sandbox.compose_calls(PROJECT, "down")
        self.assertEqual(len(downs), 1)
        self.assertIn("--remove-orphans", downs[0]["argv"])
        self.assertFalse([call for call in sandbox.calls() if call["argv"][:2] == ["volume", "rm"]])
        self.assertIn(PROJECT + "-data", sandbox.read_state()["volumes"])
        self.assertEqual(sandbox.containers(PROJECT), {})
        self.assertEqual(sandbox.crontab(), UNRELATED)
        self.assertIn("docker volume rm %s-data" % PROJECT, result.stdout)


if __name__ == "__main__":
    unittest.main()
