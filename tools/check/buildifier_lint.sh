#!/bin/sh
# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

set -eu
buildifier="${RUNFILES_DIR:-$0.runfiles}/@TOOL_RUNFILE@"

# Aspect's explicit fix action operates on its own temporary copies.
for argument; do
  case "$argument" in
  --lint=fix | -lint=fix) exec "$buildifier" "$@" ;;
  esac
done

lint_file() {
  input=$1
  shift
  # Keep flag arguments exactly as supplied, without eval or word splitting.
  for argument; do
    shift
    case "$argument" in
    -*) set -- "$@" "$argument" ;;
    esac
  done
  "$buildifier" "$@" --mode=fix --path="$input" <"$input" >/dev/null
}

status=0
for file; do
  case "$file" in
  -*) continue ;;
  esac
  # With stdin, fix mode prints formatted code instead of writing the input.
  # Discard that code, retaining lint diagnostics and their exit status.
  # lint_file returns buildifier\'s status; collect all file diagnostics deliberately.
  # shellcheck disable=SC2310
  if lint_file "$file" "$@"; then
    :
  else
    status=$?
  fi
done
exit "$status"
