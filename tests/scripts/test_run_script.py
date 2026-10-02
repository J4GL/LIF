"""Scripts category: scripts/run.sh with the fake docker."""
import os
import signal
import time
import unittest

from tests.scripts.script_fixtures import PROJECT, RUN_PROJECT, ScriptSandbox, read_text, wait_until


class RunScriptSpecTest(unittest.TestCase):
    def start_run(self, sandbox):
        process, lines = sandbox.start("run.sh")
        self.assertTrue(wait_until(lambda: any("http://" in line for line in lines), timeout=15), "".join(lines))
        self.assertTrue(sandbox.compose_calls(RUN_PROJECT, "up"))
        return process, lines

    def test_DEPLOY_011_run_removes_stack_on_exit(self):
        cases = (("INT", 130), ("TERM", 143), ("HUP", 129), ("EXIT", 3), ("KILL", -9))
        for case, expected in cases:
            with self.subTest(case=case):
                sandbox = ScriptSandbox.make(self)
                process, _ = self.start_run(sandbox)
                if case == "INT":
                    os.killpg(process.pid, signal.SIGINT)
                elif case == "TERM":
                    process.send_signal(signal.SIGTERM)
                elif case == "HUP":
                    os.killpg(process.pid, signal.SIGHUP)
                elif case == "EXIT":
                    with sandbox.state() as state:
                        for item in state["containers"].values():
                            if item["project"] == RUN_PROJECT and item["service"] == "scraper":
                                item.update(running=False, exit_code=3)
                else:
                    process.kill()
                self.assertEqual(process.wait(10), expected)
                cleaned = lambda: [call["sub"] for call in sandbox.compose_calls(RUN_PROJECT) if call["sub"] in ("kill", "down")][-2:] == ["kill", "down"]
                self.assertTrue(wait_until(cleaned, timeout=5), [call["argv"] for call in sandbox.compose_calls(RUN_PROJECT)])
                kill = [call for call in sandbox.compose_calls(RUN_PROJECT, "kill")][-1]
                down = [call for call in sandbox.compose_calls(RUN_PROJECT, "down")][-1]
                self.assertIn("SIGTERM", kill["argv"])
                self.assertIn("--remove-orphans", down["argv"])
                self.assertTrue(wait_until(lambda: sandbox.containers(RUN_PROJECT, running_only=True) == {}, timeout=5))
                self.assertEqual([call for call in sandbox.state_changing_calls() if call["project"] == PROJECT], [])
                self.assertIn(PROJECT + "-data", sandbox.read_state()["volumes"])
                self.assertEqual(sorted(sandbox.read_state()["tags"]), ["dht-scraper:local-run"])
                self.assertTrue(all(call["restart"] == "no" for call in sandbox.state_changing_calls() if call["argv"][0] == "compose"))
                if case == "HUP":
                    self.assertTrue(wait_until(lambda: "stopping" in read_text(os.path.join(sandbox.log_dir, "run.log")) if os.path.exists(os.path.join(sandbox.log_dir, "run.log")) else False, timeout=5))

    def test_DEPLOY_012_run_refuses_when_running(self):
        for case, project in (("service", PROJECT), ("other", RUN_PROJECT)):
            with self.subTest(case=case):
                sandbox = ScriptSandbox.make(self)
                sandbox.add_container(project)
                started = time.monotonic()
                result = sandbox.run("run.sh", timeout=10)
                self.assertLess(time.monotonic() - started, 5.0)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertIn(project, result.stderr)
                self.assertIn("stop", result.stderr)
                self.assertEqual(sandbox.state_changing_calls(), [])


if __name__ == "__main__":
    unittest.main()
