#!/bin/sh
# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause
# Target: @ARCH@-@OS@

set -eu
umask 0077

current_uid=$(id -u)
if [ "$current_uid" != 0 ]; then
  echo '@NAME@: run this command as root' >&2
  exit 1
fi

# Retire the old label before installing the renamed daemon.
if launchctl print system/org.@NAME@ >/dev/null 2>&1; then
  launchctl bootout system/org.@NAME@
fi
if [ -f /Library/LaunchDaemons/org.@NAME@.plist ]; then
  launchctl disable system/org.@NAME@
  rm /Library/LaunchDaemons/org.@NAME@.plist
fi
if launchctl print system/io.dimidiumlabs.@NAME@ >/dev/null 2>&1; then
  launchctl bootout system/io.dimidiumlabs.@NAME@
fi

# Allocate unused local system IDs; never renumber existing accounts.
next_id() {
  entries=$(dscl . -list "$1" "$2") || return 1
  value=400
  while [ "$value" -lt 500 ]; do
    if ! printf '%s\n' "$entries" | awk -v value="$value" '$NF == value { found = 1 } END { exit !found }'; then
      printf '%s\n' "$value"
      return 0
    fi
    value=$((value + 1))
  done
  echo "@NAME@: no unused system IDs available" >&2
  return 1
}

if ! dscl . -read /Groups/@GROUP@ >/dev/null 2>&1; then
  gid=$(next_id /Groups PrimaryGroupID)
  dscl . -create /Groups/@GROUP@ PrimaryGroupID "$gid"
  dscl . -create /Groups/@GROUP@ RealName @SHELL_DESCRIPTION@
  dscl . -create /Groups/@GROUP@ Password '*'
fi
if ! dscl . -read /Users/@USER@ >/dev/null 2>&1; then
  group=$(dscl . -read /Groups/@GROUP@ PrimaryGroupID)
  gid=$(printf '%s\n' "$group" | awk '{ print $2 }')
  uid=$(next_id /Users UniqueID)
  dscl . -create /Users/@USER@ UniqueID "$uid"
  dscl . -create /Users/@USER@ PrimaryGroupID "$gid"
  dscl . -create /Users/@USER@ RealName @SHELL_DESCRIPTION@
  dscl . -create /Users/@USER@ NFSHomeDirectory @SHELL_STATE_DIR@
  dscl . -create /Users/@USER@ UserShell /usr/bin/false
  dscl . -create /Users/@USER@ Password '*'
  dscl . -create /Users/@USER@ IsHidden 1
fi

# Move the account's home out of the legacy /Library/Application Support path.
# dscl moves a value containing spaces to the next line.
home=$(dscl . -read /Users/@USER@ NFSHomeDirectory | sed '1s/^NFSHomeDirectory://' | tr -d '\n' | sed 's/^ *//')
if [ "$home" != @SHELL_STATE_DIR@ ]; then
  dscl . -create /Users/@USER@ NFSHomeDirectory @SHELL_STATE_DIR@
fi

# Refuse privileged identities and a conflicting primary group.
account_uid=$(id -u @USER@)
account_gid=$(id -g @USER@)
account_group=$(id -gn @USER@)
if [ "$account_uid" = 0 ] || [ "$account_gid" = 0 ] || [ "$account_group" != @GROUP@ ]; then
  echo '@NAME@: existing account must be unprivileged with primary group @GROUP@' >&2
  exit 1
fi

# Keep missing parents accessible despite the hook's private umask.
(umask 022 && mkdir -p "$(dirname @SHELL_CONFIG_DIR@)" "$(dirname @SHELL_STATE_DIR@)" "$(dirname @SHELL_LOG_DIR@)")

# Do not recursively change ownership of existing secrets or build data.
install -d -m 0750 -o root -g @GROUP@ @SHELL_CONFIG_DIR@
install -d -m 0750 -o root -g @GROUP@ @SHELL_CONFIG_DIR@/secrets
install -d -m 0700 -o @USER@ -g @GROUP@ @SHELL_STATE_DIR@
install -d -m 0700 -o @USER@ -g @GROUP@ @SHELL_LOG_DIR@
