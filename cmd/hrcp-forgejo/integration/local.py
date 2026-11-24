# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

"""Run the actual HRC provider against a disposable loopback Forgejo instance."""

import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request

from python.runfiles import runfiles


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--state-dir", help="New directory for logs, SQLite and evidence"
    )
    args = parser.parse_args()
    state = Path(args.state_dir or tempfile.mkdtemp(prefix="hrc-forgejo-local-"))
    state = state.absolute()
    state.mkdir(mode=0o700, parents=True, exist_ok=not args.state_dir)
    os.chmod(state, 0o700)
    print(f"State and evidence: {state}", flush=True)
    rf = runfiles.Create()
    paths = {
        name: Path(rf.Rlocation(os.environ[key])).resolve()
        for name, key in {
            "forgejo": "FORGEJO_SERVER",
            "hrcd": "HRC_DAEMON",
            "hrcm": "HRC_SPACE",
            "hrcp-forgejo-queue": "FORGEJO_QUEUE",
            "hrcp-forgejo-worker": "FORGEJO_WORKER",
        }.items()
    }
    bindir = state / "bin"
    bindir.mkdir()
    for name, path in paths.items():
        (bindir / name).symlink_to(path)
    # Fail loudly if any JS action falls back to the host PATH.
    (bindir / "node").write_text(
        "#!/bin/sh\necho HOST_NODE_MUST_NOT_RUN >&2\nexit 97\n"
    )
    (bindir / "node").chmod(0o755)
    # A fresh HOME isolates Git config, tool caches and Forgejo settings.
    home = state / "home"
    home.mkdir()
    env = dict(os.environ, HOME=str(home), XDG_CACHE_HOME=str(home / ".cache"))
    env["PATH"] = str(bindir) + os.pathsep + env["PATH"]
    env["GIT_CONFIG_NOSYSTEM"] = "1"
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base = f"http://127.0.0.1:{port}"
    serverdir = state / "forgejo"
    serverdir.mkdir()
    ini = serverdir / "app.ini"
    ini.write_text(
        f"""APP_NAME = HRC Forgejo PoC
RUN_MODE = prod
WORK_PATH = {serverdir}
[server]
PROTOCOL = http
HTTP_ADDR = 127.0.0.1
HTTP_PORT = {port}
DOMAIN = 127.0.0.1
ROOT_URL = {base}/
DISABLE_SSH = true
OFFLINE_MODE = true
[database]
DB_TYPE = sqlite3
PATH = {serverdir}/forgejo.db
[repository]
ROOT = {serverdir}/repositories
DEFAULT_BRANCH = main
DEFAULT_REPO_UNITS = repo.code,repo.issues,repo.pulls,repo.actions
[security]
INSTALL_LOCK = true
SECRET_KEY = {secrets.token_hex(32)}
INTERNAL_TOKEN = {secrets.token_hex(64)}
[service]
DISABLE_REGISTRATION = true
[actions]
ENABLED = true
[log]
MODE = console
LEVEL = info
"""
    )
    os.chmod(ini, 0o600)
    forgejo = [
        str(paths["forgejo"]),
        "--work-path",
        str(serverdir),
        "--config",
        str(ini),
    ]
    children = []
    logs = []

    def launch(argv, filename):
        log = (state / filename).open("w")
        logs.append(log)
        child = subprocess.Popen(
            argv, env=env, stdout=log, stderr=log, start_new_session=True
        )
        children.append(child)
        return child

    auth = ""

    def request(path, method="GET", body=None, raw=False):
        data = None if body is None else json.dumps(body).encode()
        headers = {"Authorization": auth} if auth else {}
        if data is not None:
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            base + path, data=data, method=method, headers=headers
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            content = response.read()
            return content if raw else (json.loads(content) if content else None)

    def wait_for(probe, timeout=60):
        deadline = time.monotonic() + timeout
        last = None
        while time.monotonic() < deadline:
            try:
                value = probe()
                if value:
                    return value
            except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
                last = error
            for child in children:
                if child.poll() is not None:
                    raise RuntimeError(
                        f"Child exited with {child.returncode}; inspect {state}"
                    )
            time.sleep(0.25)
        raise TimeoutError(
            f"Timed out waiting for local Forgejo/HRC: {last}; inspect {state}"
        )

    try:
        launch(forgejo + ["web"], "forgejo.log")
        version = wait_for(lambda: request("/api/v1/version"))
        print(f"Local Forgejo: {version}", flush=True)
        username = "pocadmin"
        password = secrets.token_urlsafe(32)
        admin = subprocess.run(
            forgejo
            + [
                "admin",
                "user",
                "create",
                "--username",
                username,
                "--password",
                password,
                "--email",
                "poc@localhost.invalid",
                "--admin",
                "--must-change-password=false",
            ],
            env=env,
            capture_output=True,
            timeout=30,
            check=False,
        )
        if admin.returncode:
            raise RuntimeError("Unable to create local admin: " + admin.stderr.decode())
        auth = "Basic " + base64.b64encode(f"{username}:{password}".encode()).decode()
        token = request(
            f"/api/v1/users/{username}/tokens",
            "POST",
            {"name": "poc", "scopes": ["all"]},
        )
        auth = "token " + token["sha1"]
        repo = request(
            "/api/v1/user/repos",
            "POST",
            {
                "name": "hrc-proof",
                "auto_init": True,
                "default_branch": "main",
                "private": False,
            },
        )
        prefix = f"/api/v1/repos/{username}/{repo['name']}"
        request(prefix, "PATCH", {"has_actions": True})
        registration = request(prefix + "/actions/runners/registration-token")
        registered = subprocess.run(
            [
                str(paths["hrcp-forgejo-queue"]),
                "-register",
                base,
                "-name",
                "hrc-local-poc",
            ],
            env=dict(env, FORGEJO_REGISTRATION_TOKEN=registration["token"]),
            capture_output=True,
            timeout=30,
            check=False,
        )
        if registered.returncode:
            raise RuntimeError(
                "Runner registration failed: " + registered.stderr.decode()
            )
        connection = json.loads(registered.stdout)
        tokenfile = state / "runner-token"
        tokenfile.write_text(connection["token"])
        os.chmod(tokenfile, 0o600)
        hrcroot = state / "hrc"
        runtimes = {}
        for major in (20, 24):
            bundled = Path(rf.Rlocation(os.environ[f"NODE{major}"])).resolve()
            target = hrcroot / "runtime" / "externals" / f"node{major}" / "bin" / "node"
            target.parent.mkdir(parents=True)
            target.symlink_to(bundled)
            runtimes[major] = bundled
        config = {
            "instanceId": "forgejo-local-poc",
            "directories": {
                "runtime": str(hrcroot / "runtime"),
                "spaces": str(hrcroot / "spaces"),
                "logs": str(hrcroot / "logs"),
            },
            "spaces": [
                {
                    "id": "local",
                    "type": "host",
                    "resources": {"cpu": 1, "memory": "128MiB"},
                }
            ],
            "slots": [
                {
                    "id": "workers",
                    "space": "local",
                    "type": "pgroup",
                    "quota": {"cpu": 1, "memory": "128MiB"},
                    "ephemeral": True,
                }
            ],
            "forges": [
                {
                    "id": "forgejo-poc",
                    "slot": "workers",
                    "type": "forgejo",
                    "url": base,
                    "uuid": connection["uuid"],
                    "token": {"type": "file", "value": str(tokenfile)},
                }
            ],
        }
        configfile = state / "hrc.json"
        configfile.write_text(json.dumps(config, indent=2) + "\n")
        launch([str(paths["hrcd"]), "-config", str(configfile)], "hrc.log")
        proof = "HRC_FORGEJO_REAL_JOB_" + secrets.token_hex(8)
        marker = state / "executed-by-hrc.txt"
        # Serve real JS actions from the same loopback repository. Unlike local
        # actions generated by a main step, these also exercise pre execution.
        node_steps = ""
        for major, bundled in runtimes.items():
            actiondir = f".forgejo/actions/node{major}"
            files = {
                "action.yml": f"""name: Bundled Node {major}
description: Verify exact runtime in every action phase
runs:
  using: node{major}
  pre: pre.js
  main: main.js
  post: post.js
""",
            }
            for phase in ("pre", "main", "post"):
                files[f"{phase}.js"] = f"""
const fs = require('fs');
if (Number(process.versions.node.split('.')[0]) !== {major})
  throw new Error('wrong Node version: ' + process.version);
if (fs.realpathSync(process.execPath) !== {json.dumps(str(bundled))})
  throw new Error('unbundled Node: ' + process.execPath);
console.log('HRC_NODE{major}_{phase.upper()}_VERIFIED ' + process.version + ' ' + process.execPath);
"""
            for filename, content in files.items():
                request(
                    prefix + f"/contents/{actiondir}/{filename}",
                    "POST",
                    {
                        "content": base64.b64encode(content.encode()).decode(),
                        "message": f"Add Node {major} {filename} integration fixture",
                        "branch": "main",
                    },
                )
            node_steps += (
                f"      - uses: {base}/{username}/{repo['name']}/{actiondir}@main\n"
            )
        workflow = f"""name: HRC local Forgejo proof
on: [push]
jobs:
  proof:
    runs-on: poc
    steps:
      - name: Run through HRC slot
        id: proof
        shell: bash
        run: |
          test "$FORGEJO_ACTIONS" = true
          test -n "$RUNNER_TRACKING_GROUP"
          printf '%s\\n' '{proof}' > {shlex.quote(str(marker))}
          printf 'proof=passed\\n' >> "$GITHUB_OUTPUT"
          echo {proof}
      - name: Verify step outputs
        shell: bash
        env:
          PROOF: ${{{{ steps.proof.outputs.proof }}}}
        run: |
          test "$PROOF" = passed
          echo HRC_FORGEJO_OUTPUTS_VERIFIED
{node_steps}"""
        request(
            prefix + "/contents/.forgejo/workflows/proof.yaml",
            "POST",
            {
                "content": base64.b64encode(workflow.encode()).decode(),
                "message": "Exercise the independent HRC Forgejo provider",
                "branch": "main",
            },
        )

        def completed():
            listing = request(prefix + "/actions/runs")
            runs = listing.get("workflow_runs", [])
            return next(
                (
                    run
                    for run in runs
                    if run.get("status")
                    in ("success", "failure", "cancelled", "skipped")
                ),
                None,
            )

        run = wait_for(completed, timeout=120)
        jobs_response = request(prefix + f"/actions/runs/{run['id']}/jobs")
        jobs = (
            jobs_response
            if isinstance(jobs_response, list)
            else jobs_response.get("jobs", [])
        )
        if len(jobs) != 1:
            raise AssertionError(f"Expected one real job, got {jobs_response}")
        job = jobs[0]
        joblog = request(prefix + f"/actions/jobs/{job['id']}/logs", raw=True).decode()
        (state / "job.log").write_text(joblog)
        evidence = {
            "forgejo": version,
            "url": base,
            "run": run,
            "jobs": jobs,
            "runner_uuid": connection["uuid"],
            "proof": proof,
        }
        (state / "result.json").write_text(json.dumps(evidence, indent=2) + "\n")
        if run["status"] != "success" or job.get("status") != "success":
            raise AssertionError(
                f"Real workflow did not succeed: {run}; inspect {state / 'job.log'}"
            )
        if (
            marker.read_text().strip() != proof
            or proof not in joblog
            or "HRC_FORGEJO_OUTPUTS_VERIFIED" not in joblog
        ):
            raise AssertionError(
                "Missing execution marker, server-side log or verified outputs"
            )
        wait_for(
            lambda: "Finished Forgejo task" in (state / "hrc.log").read_text(),
            timeout=15,
        )
        diagnostic_files = list(
            (hrcroot / "logs" / "jobs").glob("forgejo-*/worker.log")
        )
        if len(diagnostic_files) != 1:
            raise AssertionError(
                f"Expected one worker diagnostic file: {diagnostic_files}"
            )
        diagnostic = diagnostic_files[0]
        diagnostic_text = diagnostic.read_text()
        for major in runtimes:
            for phase in ("PRE", "MAIN", "POST"):
                message = f"HRC_NODE{major}_{phase}_VERIFIED"
                if message not in joblog or message not in diagnostic_text:
                    raise AssertionError(
                        f"Missing runtime proof in server/local logs: {message}"
                    )
        if (
            "level=debug" not in diagnostic_text
            or "Finished Forgejo execution" not in diagnostic_text
        ):
            raise AssertionError("Missing local debug/execution diagnostics")
        if diagnostic.stat().st_mode & 0o777 != 0o600:
            raise AssertionError("Worker diagnostic log is not private")
        if "HRC_NODE20_MAIN_VERIFIED" in (state / "hrc.log").read_text():
            raise AssertionError("Worker execution logs leaked to stdout/stderr")
        if connection["token"] in diagnostic_text:
            raise AssertionError("Runner token leaked into diagnostic log")
        shutil.copyfile(diagnostic, state / "worker.log")
        print(f"Worker diagnostics: {diagnostic}", flush=True)
        print(
            f"PASS: actual Forgejo run {run['id']}, job {job['id']}, runner {connection['uuid']}",
            flush=True,
        )
        print(f"Server-side logs: {state / 'job.log'}", flush=True)
        print(f"API evidence: {state / 'result.json'}", flush=True)
    finally:
        for child in reversed(children):
            if child.poll() is None:
                child.send_signal(signal.SIGTERM)
                try:
                    child.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    os.killpg(child.pid, signal.SIGKILL)
                    child.wait()
        for log in logs:
            log.close()
        # Export only evidence/logs, never SQLite, app.ini or runner credentials.
        outputs = os.environ.get("TEST_UNDECLARED_OUTPUTS_DIR")
        if outputs:
            for filename in (
                "result.json",
                "job.log",
                "hrc.log",
                "forgejo.log",
                "worker.log",
            ):
                source = state / filename
                if source.exists():
                    shutil.copyfile(source, Path(outputs) / filename)


if __name__ == "__main__":
    main()
