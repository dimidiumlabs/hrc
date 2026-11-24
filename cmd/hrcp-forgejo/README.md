# Independent Forgejo provider — draft

Built against HRC main `42db41e`, Forgejo Runner **v13.2.2**
(`0e61343d1cb98d2a381d0d18f34aea408e66e4d2`) and tested with Forgejo
**17.0.2**. This is a draft, not a production/release feature.

## Layout

- `queue/`: standalone HRC acquisition provider.
- `worker/`: standalone HRC single-job execution provider.
- `bridge/`: narrow adapter to upstream's internal Go API.
- `patch/`: pinned upstream/build patches, described below.
- `integration/`: real-server harness and synthetic Connect API tests. Both
  launch the actual queue and worker executables through HRC IPC; there is no
  separate stdin/stdout execution path.

## Worker runtime and diagnostics

HRC passes `Dir` and `LogRoot` from `directories.runtime` and
`directories.logs`, just as it does for the other built-in worker.

JavaScript actions resolve an **absolute executable path for each
`runs.using`**, including their pre/main/post phases:

```text
<directories.runtime>/externals/node20/bin/node
<directories.runtime>/externals/node24/bin/node
```

Windows paths use `node.exe`. These are the same Node distributions already
packaged with HRC; Forgejo does not launch or import the GitHub worker.
Resolution never falls back to a host `node` or silently upgrades a version.
The current HRC bundle contains Node 20 and 24. An action requesting Node 12 or
16 fails unless that exact runtime is also supplied under the corresponding
`externals/nodeXX/bin/` directory. Host execution on Linux glibc amd64 is tested;
musl runtime selection and other platforms are not yet validated.

The process working directory is the HRC slot workspace. Upstream uses it for
action caches, temporary host execution directories and tool cache; it is
distinct from `Dir`, which contains the installed runtimes.

Each accepted job creates a private diagnostic file:

```text
<directories.logs>/jobs/forgejo-<random>/worker.log
```

The job diagnostic directory is mode 0700 and the file is mode 0600. It contains
HRC worker start/result/error messages, upstream process debug logs, and upstream
job/step logs at debug level. Upstream's masked job logger and server Reporter
remain in use. The logs are outside the ephemeral slot and survive its removal.
No stdout/stderr collector is required for these execution diagnostics.
Failure to create the log fails worker startup rather than silently dropping
diagnostics. A direct worker invocation with an empty `LogRoot` uses `_diag`
under its working directory; the daemon always passes an explicit absolute root.

Diagnostic files are sensitive: do not publish them without review. Rotation,
retention and logging before the initial job/config arrives are not implemented.
Queue lifecycle messages still use HRC's existing slog output.

## Build and use

```sh
bazel build //cmd/hrcp-forgejo/queue //cmd/hrcp-forgejo/worker //:hrcd //:hrcm
```

Put `hrcp-forgejo-queue` and `hrcp-forgejo-worker` alongside HRC's executables
or on its PATH. They are not yet included in release packaging.

Obtain an instance/repository runner registration token, then register once:

```sh
# Prints credentials: capture privately, never in shared logs.
FORGEJO_REGISTRATION_TOKEN='<registration token>' \
  hrcp-forgejo-queue -register http://127.0.0.1:3000 -name hrc-forgejo
```

Use the returned runner UUID and runner token, not the registration token:

```json
{
  "instanceId": "forgejo-draft",
  "directories": {
    "runtime": "/opt/hrc/runner",
    "logs": "/var/log/hrc"
  },
  "spaces": [{"id": "local", "type": "host", "resources": {"cpu": 1}}],
  "slots": [{
    "id": "workers", "space": "local", "type": "pgroup",
    "quota": {"cpu": 1}, "ephemeral": true
  }],
  "forges": [{
    "id": "local-forgejo", "slot": "workers", "type": "forgejo",
    "url": "http://127.0.0.1:3000", "uuid": "<registered runner UUID>",
    "token": {"type": "file", "value": "/private/path/runner-token"}
  }]
}
```

The experimental server label is still fixed to `poc` and mapped to host
execution (`poc:host`). Workflows must use `runs-on: poc`.

## Acquisition and execution

The queue declares the identity and reserves HRC capacity before fetching.
Empty polls release reservations; ambiguous acquisition failures retry with the
same request key. An opaque assignment carries the task and runner connection
over HRC's private process channel.

The worker receives exactly one assignment through `provider.WorkerClient`,
calls upstream `Runner.Run`, and returns the acknowledged result. It neither
registers nor fetches jobs. HRC stop requests cancel its execution context.

Upstream Reporter stays worker-owned: it sends logs, outputs and status and
receives server cancellation. Since `Run` returns void, a client decorator
observes acknowledged terminal `UpdateTask` responses. A completion marker
prevents the queue from issuing another terminal update; without the marker the
queue provides fallback finalization, including failure to launch the worker.

The daemon passes config/payload without importing Forgejo's engine. Its
runtime.Pool, provider contracts and protobuf are unchanged. Neither production
provider imports the upstream CLI, daemon or poller. Upstream logging has
process-global state: one worker process owns one execution, not concurrent jobs.

## Upstream integration and patches

Bazel gives `bridge/` this synthetic import path to admit upstream Go
`internal/` imports:

```text
code.forgejo.org/forgejo/runner/v13/hrcbridge
```

The visibility exception admits only the bridge. Use Bazel: ordinary
`go build ./...` and `go fmt ./...` do not understand this synthetic path.

- `patch/node-runtime.patch` adds a context-scoped Node executable resolver to
  upstream's action executor, used in pre/main/post. Without a resolver upstream
  retains its original behavior; the HRC bridge always installs one. This is a
  source patch, unlike the original unmodified-engine experiment.
- `patch/docker-embed.patch` fixes generated Docker CLI BUILD files to supply
  embedded JSON schemas across a package boundary.
- `actionlint v1.7.12` gets private YAML `v4.0.0-rc.3` with an isolated importmap
  `hrc.internal/forgejo-yaml/v4`. HRC's stable YAML v4 is not downgraded.

## Validation

From the repository root on Linux amd64:

```sh
bazel run //cmd/hrcp-forgejo/integration:local
bazel test //cmd/hrcp-forgejo:test --test_output=errors
bazel test //:tests --test_output=errors
```

The harness uses a pinned, SHA256-verified Forgejo executable with fresh SQLite,
repositories and HOME on a random loopback HTTP port. It needs host Git/Bash but
no Docker, root, user credentials or external action service. It registers a
runner and starts the real HRC daemon, monitor, queue and worker through a host
space and ephemeral pgroup slot.

A push-triggered workflow verifies shell execution, step outputs and
RUNNER_TRACKING_GROUP. Two JS actions hosted on the same local Forgejo verify
Node 20 and 24 in **all six pre/main/post invocations** using both
`process.version` and `process.execPath`. A deliberately failing `node` in PATH
detects any fallback to the host executable. The harness requires all six proof
markers in both the server job log and the worker diagnostic file; it also
checks debug output and private file permissions.

Manual runs print a private state/evidence directory. Use
`-- --state-dir /absolute/new/directory` to choose it. It contains credentials
and must be kept private and deleted after inspection. Bazel exports only
`result.json`, `job.log`, `worker.log`, `hrc.log` and `forgejo.log` as test
evidence, not credentials or SQLite.

The synthetic API tests also use the real provider processes through
`provider.ProcessQueue` and `provider.StartWorker`. They cover success, job
failure, rejected terminal reporting, server cancellation and malformed payloads.
Acquisition is stopped before execution; the queue retains the delivered handle
for Finish/Close. Tests verify no duplicate finalization after an acknowledged
worker result, and fallback finalization/retry after reporting failure.

## Remaining limitations

- Trusted host workflows only. Docker/services, composite actions, cache proxy
  and the platform/server-version matrix remain unvalidated. Execution timeout
  is still one minute and the runner label is fixed.
- Server cancellation stops execution in synthetic tests, but upstream 13.2.2
  reports terminal FAILURE rather than CANCELLED in that scenario.
- Retry keys survive in-memory retries only. Durable crash recovery and
  exactly-once finalization after ambiguous reporting are not solved.
- Cleanup hardening and adversarial workflow isolation are not implemented.
  Host jobs run as the service account; separate processes are not a security
  boundary. Task payloads and runner connections contain credentials.
- This pins an internal API and carries a source patch. Licensing needs review
  before distribution: upstream root is GPLv3, some files are MIT. Linked
  artifacts are not added to HRC releases.
