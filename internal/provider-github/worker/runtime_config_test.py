# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

"""Check Worker runtime settings with its executable outside the workspace."""
import json
import os
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest


WORKER = Path(sys.argv.pop(1)).resolve()


class RuntimeConfigTest(unittest.TestCase):
    def test_worker_uses_upstream_pipes(self):
        with tempfile.TemporaryDirectory() as temporary:
            environment = dict(os.environ, RUNNER_CONFIG=json.dumps({
                "agentId": 7, "agentName": "pipe-test", "workFolder": ".",
            }), RUNNER_DIAG=temporary)
            # Cancel cannot be the initial message. Its decoded enum in the
            # error proves the real .NET Worker received the frame through its
            # original anonymous-pipe channel, without modifying ProcessChannel.
            read, feed = os.pipe()
            back, write = os.pipe()
            try:
                options = {}
                handles = [read, write]
                if os.name == "nt":
                    import msvcrt
                    handles = [msvcrt.get_osfhandle(fd) for fd in handles]
                    for handle in handles:
                        os.set_handle_inheritable(handle, True)
                    startup = subprocess.STARTUPINFO()
                    startup.lpAttributeList = {"handle_list": handles}
                    options["startupinfo"] = startup
                else:
                    options["pass_fds"] = handles
                os.write(feed, struct.pack("<ii", 2, 0))
                result = subprocess.run(
                    [str(WORKER), "spawnclient", *map(str, handles)],
                    cwd=temporary, env=environment,
                    capture_output=True, timeout=30, **options)
            finally:
                for fd in (read, feed, back, write):
                    os.close(fd)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn(b"Expected 'NewJobRequest'. Actual 'CancelRequest'", result.stdout)

    def test_environment_settings_ignore_legacy_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            program, work = root / "programs", root / "work"
            program.mkdir()
            work.mkdir()
            executable = program / WORKER.name
            shutil.copy2(WORKER, executable)
            legacy = (".runner", ".credentials", ".credentials_rsaparams", ".runner_migrated")
            for name in legacy:
                (work / name).write_text("invalid legacy configuration", encoding="utf-8")
            environment = dict(os.environ)
            environment.pop("RUNNER_CONFIG", None)
            environment.pop("RUNNER_DIAG", None)
            missing = subprocess.run([str(executable)], cwd=work, env=environment,
                                     capture_output=True, text=True, timeout=30)
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("RUNNER_CONFIG", missing.stdout + missing.stderr)
            environment["RUNNER_CONFIG"] = json.dumps({
                "agentId": 7, "agentName": "runtime-config-test", "poolId": 3,
                "workFolder": ".",
            })
            result = subprocess.run([str(executable)], cwd=work, env=environment,
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("Expected '3'. Actual '0'", result.stdout)
            self.assertTrue(list((work / "_diag").glob("Worker_*.log")))
            for name in legacy:
                self.assertEqual((work / name).read_text(encoding="utf-8"),
                                 "invalid legacy configuration")

    def test_separate_diagnostics_per_invocation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for job in ("first", "second"):
                work = root / "work" / job
                work.mkdir(parents=True)
                diag = root / "logs" / job
                environment = dict(os.environ,
                    RUNNER_CONFIG=json.dumps({"agentId": 7, "agentName": job,
                                              "poolId": 3, "workFolder": "."}),
                    RUNNER_DIAG=str(diag))
                result = subprocess.run([str(WORKER)], cwd=work, env=environment,
                                        capture_output=True, text=True, timeout=30)
                self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
                self.assertIn("Expected '3'. Actual '0'", result.stdout)
                self.assertEqual(len(list(diag.glob("Worker_*.log"))), 1)
                self.assertFalse((work / "_diag").exists())
            self.assertEqual(len(list((root / "logs").glob("*/Worker_*.log"))), 2)


if __name__ == "__main__":
    unittest.main()
