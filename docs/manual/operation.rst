Operation
=========

Manage an installed host. For first-time setup, follow the
`Manual <./index.rst>`__. Configuration fields are described in the
`Configuration reference <../configuration.rst>`__.
For lifecycle changes, see `Upgrading <./upgrading.rst>`__ or
`Removal <./removal.rst>`__.

Contents
--------

- `Service management <#service-management>`__
- `Draining <#draining>`__
- `Workspaces and cleanup <#workspaces-and-cleanup>`__
- `Logs <#logs>`__

Service management
------------------

Start only after configuring GitHub access and secrets. Restart after
configuration changes. Stopping or restarting the daemon cancels active
jobs and cleans up their workspaces; it does not wait for jobs to finish.

.. tabs::
   :group: host-platform

   Windows
   ~~~~~~~

   Run PowerShell as Administrator:

   .. code-block:: powershell

      Start-Service hrc
      Get-Service hrc

   Stop: ``Stop-Service hrc``. Restart: ``Restart-Service hrc``.
   The manually installed WinSW service starts manually; use
   ``Set-Service hrc -StartupType Automatic`` if it should start at boot.
   The Ansible role enables automatic startup itself.

   macOS
   ~~~~~

   .. code-block:: sh

      sudo launchctl enable system/io.dimidiumlabs.hrc
      sudo launchctl bootstrap system /Library/LaunchDaemons/io.dimidiumlabs.hrc.plist
      sudo launchctl print system/io.dimidiumlabs.hrc

   Stop: ``sudo launchctl bootout system/io.dimidiumlabs.hrc``. To restart, boot out
   and bootstrap again. If already unloaded, only bootstrap is needed.

   Linux / Systemd
   ~~~~~~~~~~~~~~~

   Enable at boot, start, and check status:

   .. code-block:: sh

      sudo systemctl enable --now hrc.service
      sudo systemctl status hrc.service

   Stop: ``sudo systemctl stop hrc.service``.
   Restart: ``sudo systemctl restart hrc.service``.

   Linux / OpenRC
   ~~~~~~~~~~~~~~

   .. code-block:: sh

      sudo rc-update add hrc default
      sudo rc-service hrc start
      sudo rc-service hrc status

   Stop: ``sudo rc-service hrc stop``. Restart: ``sudo rc-service hrc restart``.

Draining
--------

On Linux and macOS, send ``SIGUSR1`` to **hrcd only** to stop acquisition and
finish already accepted jobs before releasing queues and spaces:

.. code-block:: sh

   kill -USR1 <hrcd-pid>

For the packaged systemd service:

.. code-block:: sh

   sudo systemctl kill --kill-who=main --signal=SIGUSR1 hrc.service

Drain has no additional overall time limit. Already accepted jobs waiting for a
slot may still start. A subsequent ``SIGTERM`` or ``SIGINT`` requests cancellation
instead of waiting for normal completion. Ordinary service stop/restart commands
still request cancellation; they do not initiate Drain.

Finalization errors can escalate cleanup for the affected queue or space, or stop
the daemon if that scope cannot be collected. Remaining cleanup obligations are
reported as errors, not treated as successful release. Loss of the daemon itself
still requires the service manager's external cleanup.

The daemon exits after draining. Check the service manager's restart policy before
using this for maintenance: supervision that restarts successful exits, such as
OpenRC's configured supervisor, may immediately start acquisition again.

Windows currently supports cancellation through the existing stop mechanism, but
not a separate drain request. WinSW does not automatically forward an SCM custom
control code to ``hrcd``; Ctrl+Break is not a distinct drain signal in Go.

Workspaces and cleanup
----------------------

Each runner slot owns a workspace and a process group. With ``ephemeral: true``, both are
cleaned up after the runner finishes. Otherwise, workspaces and their remaining build processes can be reused across
jobs during one daemon run. Each job still gets a new ephemeral runner
registration. Installed tools and caches outside the slot directories remain
on the host independently of this setting.

Graceful daemon shutdown expires all slots, including idle reusable ones:
remaining slot processes are killed and slot directories are deleted. Job
diagnostics are stored separately under ``<directories.logs>/jobs/`` and are not removed
with the workspaces. Log retention is managed by the administrator.

On all three platforms, process cleanup uses the inherited
``RUNNER_TRACKING_GROUP`` environment variable and is best effort. Windows also
retains a Job Object as a crash-cleanup safety net. A monitor retires its slots
when its connection to the daemon is lost. On Linux and macOS, descendant cleanup
cannot run if the owning monitor itself is killed with ``SIGKILL`` or crashes.

Logs
----

Daemon output:

.. tabs::
   :group: host-platform

   Windows
   ~~~~~~~

   WinSW logs are stored in ``C:\ProgramData\hrc\logs``.

   macOS
   ~~~~~

   - ``/Library/Logs/hrc/stdout.log``
   - ``/Library/Logs/hrc/stderr.log``

   Linux / Systemd
   ~~~~~~~~~~~~~~~

   .. code-block:: sh

      sudo journalctl -u hrc.service -f

   Linux / OpenRC
   ~~~~~~~~~~~~~~

   - ``/var/log/hrc/stdout.log``
   - ``/var/log/hrc/stderr.log``

Job logs are stored under ``<directories.logs>/jobs/`` as configured in ``hrc.json``.
Retain logs from a failed job before removing them; deleting a workspace does
not delete its diagnostic logs.
