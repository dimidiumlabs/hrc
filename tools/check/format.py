# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

"""Run shared formatters in check mode, or explicitly fix the checkout."""

import argparse
import os
from pathlib import Path
import subprocess

from python.runfiles import runfiles


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--fix", action="store_true")
    args = parser.parse_args()
    resolver = runfiles.Create()
    env = {**os.environ, **resolver.EnvVars()}
    if args.fix:
        workspace = env.get("BUILD_WORKSPACE_DIRECTORY")
        if not workspace:
            parser.error("--fix is only supported by bazel run //:format -- --fix")
        env["BUILD_WORKING_DIRECTORY"] = workspace
        command = [resolver.Rlocation(env["FORMAT_FIX"])]
    else:
        manifest = Path(resolver.Rlocation(env["FORMAT_SOURCES"]))
        workspace_name, *sources = manifest.read_text(encoding="utf-8").splitlines()
        workspace = Path(resolver.Rlocation(f"{workspace_name}/MODULE.bazel")).parent
        env.pop("BUILD_WORKING_DIRECTORY", None)
        env.pop("BUILD_WORKSPACE_DIRECTORY", None)
        env["TEST_WORKSPACE"] = workspace_name
        command = [resolver.Rlocation(env["FORMAT_CHECK"]), *sources]
    return subprocess.run(command, cwd=workspace, env=env).returncode


if __name__ == "__main__":
    raise SystemExit(main())
