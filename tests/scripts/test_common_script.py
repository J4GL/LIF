"""Scripts category: helpers of scripts/common.sh."""
import os
import stat
import subprocess
import unittest

from tests.scripts.script_fixtures import BASH, ScriptSandbox, read_text


class CommonScriptSpecTest(unittest.TestCase):
    def ensure_env(self, sandbox, env_file):
        environment = sandbox.environment
        environment["DHT_ENV_FILE"] = env_file
        command = '. "%s" && ensure_env' % sandbox.script("common.sh")
        return subprocess.run([BASH, "-c", command], cwd=sandbox.repo, env=environment, capture_output=True, text=True, timeout=10)

    def test_DEPLOY_010_env_file_created_once(self):
        sandbox = ScriptSandbox.make(self)
        with self.subTest(case="new"):
            env_file = os.path.join(sandbox.root, "new.env")
            self.assertEqual(self.ensure_env(sandbox, env_file).returncode, 0)
            content = read_text(env_file)
            self.assertEqual(stat.S_IMODE(os.stat(env_file).st_mode), 0o600)
            self.assertRegex(content, r"(?m)^# DHT_WEB_BIND=")
            self.assertRegex(content, r"(?m)^# DHT_WEB_PORT=")
            self.assertEqual([line for line in content.splitlines() if line and not line.startswith("#")], [])
        with self.subTest(case="existing"):
            env_file = os.path.join(sandbox.root, "existing.env")
            with open(env_file, "w") as handle:
                handle.write("DHT_WEB_PORT=9090\n")
            self.assertEqual(self.ensure_env(sandbox, env_file).returncode, 0)
            self.assertEqual(read_text(env_file), "DHT_WEB_PORT=9090\n")

if __name__ == "__main__":
    unittest.main()
