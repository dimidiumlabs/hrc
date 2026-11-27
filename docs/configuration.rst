Configuration
=============

hrc reads the file supplied with ``hrcd -config PATH``. Its extension selects
the format: ``.json``, ``.yaml`` or ``.yml``. Configuration is read
at startup; restart the service to apply changes. For installation examples,
see `Installation <./manual/installation.rst>`__. Restarting cancels active jobs and
removes all slots.

``instanceId``, forge, slot pool and space ``id`` values, and their references
must match ``[A-Za-z][A-Za-z0-9_-]*``: an ASCII letter first, then ASCII letters,
digits, underscores or hyphens.

For example, a manual YAML config can be started with
``sudo hrcd -config hrc.yaml`` on Unix:

.. code-block:: yaml

   instanceId: build-01
   spaces:
     - id: build
       type: host
       user: hrc-build
       resources: {cpu: 8, memory: 16GiB}
   slots:
     - id: workers
       space: build
       type: pgroup
       ephemeral: true
       quota: {cpu: 2, memory: 4GiB}
   forges:
     - id: project-builds
       type: github
       slot: workers
       url: https://github.com/OWNER/REPO
       pat: {type: env, value: GITHUB_TOKEN}

Contents
--------

- `Host settings <#host-settings>`__
- `Spaces and forges <#spaces-and-forges>`__
- `GitHub provider <#github-provider>`__
- `Resources <#resources>`__
- `Secrets <#secrets>`__
- `Ansible variables <#ansible-variables>`__
- `Command line <#command-line>`__

Host settings
-------------

.. list-table::
   :header-rows: 1

   * - Field
     - Default
     - Meaning
   * - ``instanceId``
     - Required
     - Daemon instance identifier used in GitHub sessions and runner names. Give each instance a distinct ID.
   * - ``spaces``
     - No spaces
     - Named environments and their resource budgets.
   * - ``slots``
     - No slot pools
     - Named pools of identical slots, each assigned to one space.
   * - ``forges``
     - No forges
     - Named provider connections, each assigned to one slot pool.
   * - ``directories.runtime``
     - ``./runner``
     - Shared read-only Worker resources.
   * - ``directories.logs``
     - ``./hrc/log``
     - Parent of per-job diagnostics under ``jobs/``.

Relative paths use the daemon's working directory, not the config file's
directory, except on Unix where provider credential file paths use the ``hrc``
account's home. The supplied services use their state directory
as working directory. Keep state and job directory paths free of spaces on macOS.
The Ansible role supplies installation-specific defaults for these paths;
the table above describes the daemon's own defaults.

Spaces and forges
-----------------

Each forge names one slot pool; several forges can share it. Each pool names one
space. Currently, at most one pool may reference a space. This is an implementation
restriction, not a requirement of the configuration model. A space without a pool
is not started. The supported implementations are host spaces and process-group slots.

.. code-block:: yaml

   instanceId: build-01
   spaces:
     - id: build
       type: host
       user: hrc-build
       resources:
         cpu: 8
         memory: 16GiB
   slots:
     - id: workers
       space: build
       type: pgroup
       quota:
         cpu: 2
         memory: 4GiB
       ephemeral: false
   forges:
     - id: project-builds
       slot: workers
       type: github
       url: https://github.com/OWNER/REPO
       pat:
         type: env
         value: GITHUB_TOKEN

``resources`` is the space budget, and pool ``quota`` is the allocation per slot.
An omitted budget dimension uses the full host capacity.
Space budgets must fit together within that capacity. Values are fixed until
restart. Each space requires ``user``, an existing host account. The monitor
runs as that user in its home directory and prepares ``cache/``, ``tmp/``,
``log/slots/``, ``log/jobs/``, and ``slots/``. Each slot has ``cache/``,
``tmp/``, and ``work/`` under ``slots/NAME/``; workers start in ``work/``.
Slot names have the form ``slt-XXXXXX``. At startup the monitor clears
``tmp/`` and old ``slots/``; before each job it clears that slot's ``tmp/``.
Space cache and logs survive these cleanups. Job diagnostics go to
``log/jobs/JOB_ID/``; ``log/slots/NAME/`` survives slot destruction.

Workers inherit ``HRC_VERSION``, ``HRC_SPACE_TYPE``, ``HRC_SLOT_TYPE``, and
absolute paths in ``HRC_SPACE_DIR_CACHE``, ``HRC_SPACE_DIR_LOG``,
``HRC_SPACE_DIR_TMP``, ``HRC_SLOT_DIR_CACHE``, ``HRC_SLOT_DIR_LOG``, and
``HRC_SLOT_DIR_TMP``. These describe the layout; they do not yet change
``HOME``, XDG directories, or the runner's temporary directory.

``ephemeral`` in the slot pool defaults to false (reusable slots). Set it to true to
destroy each slot after its Run. The current space type is ``host`` and slot
type is ``pgroup``; these are also the defaults when omitted.

Each forge has a unique ``id``, a ``slot`` reference, a provider ``type``,
and provider-specific fields at the same level. Currently ``github`` is supported.
Reusable slots can pass between forges sharing a pool. Use separate spaces
to keep their workspaces separate.

GitHub provider
---------------

The following fields belong directly to a GitHub forge.
The common forge ``id`` is used as the GitHub scale set name.

.. list-table::
   :header-rows: 1

   * - Field
     - Default
     - Meaning
   * - ``url``
     - Required
     - Full repository or organization URL, for example ``https://github.com/OWNER/REPO``.
   * - ``group``
     - ``default``
     - GitHub runner group containing the scale set. Organization policies determine which repositories can use it.
   * - ``labels``
     - Scale set name
     - With no explicit labels, jobs select the scale set by ``id``. Otherwise only the configured labels are used.
   * - ``pat``
     - Not set
     - PAT described by a secret object.
   * - ``app``
     - Not set
     - GitHub App credentials: ``clientId`` (string), ``installationId`` (positive integer), and ``privateKey`` (secret object).
   * - ``jwt``
     - Not set
     - Externally signed GitHub App JWT: ``installationId`` (positive integer) and ``token`` (secret object).
   * - ``runnerVersion``
     - Runner version pinned at build time
     - GitHub Runner protocol version advertised by this queue.

Supply exactly one of ``pat``, ``app`` or ``jwt``.
See `GitHub access <./manual/github-auth.rst>`__ for credential setup and permissions.
Changing labels recreates the scale set on startup, rather than updating it in
place. Other hosts using the same scale set should agree on its labels.
There are no automatic ``self-hosted``, OS, or architecture labels; include
those explicitly if you want to route by them. See
`Workflow configuration <./manual/workflows.rst>`__ for examples.

Resources
---------

Space ``resources`` and slot ``quota`` each accept ``cpu`` and ``memory``:

.. code-block:: yaml

   resources:
     cpu: 8
     memory: 16GiB
   quota:
     cpu: 2
     memory: 4GiB

- ``cpu`` is a non-negative number of logical CPU threads, including fractions.
- ``memory`` is a string containing integer bytes or an integer followed
  by ``KiB``, ``MiB``, ``GiB``, or ``TiB``, for example ``4GiB``.
- Zero or an omitted quota field leaves that resource unaccounted.

Each space divides its resource budget by its per-slot quota, rounding down.
The smallest result is its concurrency limit, shared by the forges assigned
to that space. Startup fails if the budget cannot hold one slot. Leave part
of the host capacity unassigned to keep it available for the system.

These are scheduling values, **not enforced CPU or memory limits**. Capacity
is calculated at startup, not continuously adjusted to currently free memory.
Idle reusable slots can retain processes even though they do not occupy a busy
runner slot. See `Workspaces and cleanup
<./manual/operation.rst#workspaces-and-cleanup>`__ for slot lifetime.

Secrets
-------

``pat``, ``app.privateKey`` and ``jwt.token`` use the same object format:

.. code-block:: yaml

   type: file
   value: /etc/hrc/secrets/github-token

.. list-table::
   :header-rows: 1

   * - ``type``
     - Meaning of ``value``
   * - ``file``
     - Path to a file readable by the queue account (``hrc`` on Unix).
   * - ``env``
     - Name of an environment variable in the daemon's environment.
   * - ``inline``
     - The secret itself. Anyone who can read the configuration can read it.

For ``type: subcommand``, replace ``value`` with ``command: [executable, arg, ...]``.
The command runs without a shell; stdout is the secret. ``timeout`` defaults to ``30s``.

Every source accepts a cache ``ttl``, such as ``5m``; omitted or zero disables
caching. Expired values are reread on demand, not on a timer.

External JWTs require RS256 and ``iss``, ``iat``, ``exp`` claims. JWTs with less
than 30 seconds remaining are reread regardless of TTL.

Leading and trailing whitespace is removed from secret values; an empty secret
is an error. Services do not inherit your interactive shell's ``GITHUB_TOKEN``.
Jobs execute as the configured space account and can access that account's
credentials and readable secret files.

Ansible variables
-----------------

.. list-table::
   :header-rows: 1

   * - Variable
     - Default
     - Meaning
   * - ``hrc_config``
     - Required for installation
     - Mapping written as JSON. Must contain ``instanceId``, nonempty ``spaces``, ``slots`` and ``forges``.
   * - ``hrc_secrets``
     - ``{}``
     - Simple filenames mapped to secret contents, written under the target's secrets directory.
   * - ``hrc_artifact_source``
     - URL embedded by release CI
     - Base HTTP(S) URL or absolute directory on the Ansible controller containing release archives.
   * - ``hrc_remove_data``
     - ``false``
     - Remove role only: also delete configuration, secrets, state, and logs at default locations.

The install role supplies ``directories.runtime`` and ``directories.logs``; ``hrc_config`` can override them. Other fields pass through unchanged.
Secret filenames may contain letters, digits, underscores, dots, and hyphens,
and must start with a letter, digit, or underscore. Omitted secret files are
retained; remove or revoke credentials explicitly.

Each collection pins a binary release and its checksums. To vendor binaries,
copy the release tarballs without renaming them and set ``hrc_artifact_source``
to their directory. Changing the source does not disable checksum verification.
There is no separate binary version override.

Command line
------------

- ``hrcd -config PATH`` starts the daemon using the specified config file.
- ``hrcd -version`` prints the build version.
- ``hrcd -help`` shows the available flags.
