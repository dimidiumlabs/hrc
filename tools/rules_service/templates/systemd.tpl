# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

[Unit]
Description=@DESCRIPTION@
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=0

[Service]
Type=exec
User=@USER@
Group=@GROUP@
WorkingDirectory=@STATE@
ExecStart=@COMMAND@
Restart=on-failure
RestartSec=10
# Let the daemon coordinate graceful shutdown; kill the whole cgroup on timeout.
KillMode=mixed
TimeoutStopSec=60
UMask=0077

[Install]
WantedBy=multi-user.target
