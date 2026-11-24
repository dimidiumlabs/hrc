# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: MIT

"""Keep the upstream anonymous-pipe channel usable without runtime env settings."""

import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import unittest


WORKER = Path(sys.argv.pop(1)).resolve()


class RuntimeConfigTest(unittest.TestCase):
    def test_worker_uses_upstream_pipes(self):
        with tempfile.TemporaryDirectory() as temporary:
            # Only the legacy CLI uses a .runner file. The gRPC adapter passes
            # settings directly; its isolation is covered by the C# and RPC tests.
            Path(temporary, ".runner").write_text(
                json.dumps({"agentId": 7, "agentName": "pipe-test", "workFolder": "."}),
                encoding="utf-8",
            )
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
                    cwd=temporary,
                    capture_output=True,
                    timeout=30,
                    **options,
                )
            finally:
                for fd in (read, feed, back, write):
                    os.close(fd)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn(
                b"Expected 'NewJobRequest'. Actual 'CancelRequest'", result.stdout
            )


if __name__ == "__main__":
    unittest.main()
