#!/usr/bin/env bash
# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

set -euo pipefail

[[ $(uname -sm) == 'Linux x86_64' ]] || {
  echo 'Requires Linux/amd64' >&2
  exit 1
}

command -v bazel gh gh-actions-lock jq >/dev/null

runtime=$(realpath -m "${1:?Usage: setup.sh INSTALL_DIR}")
cd "$(dirname "${BASH_SOURCE[0]}")/../.."

source_dir="$PWD/tools/renovate"
mkdir -p "$runtime/bin" "$runtime/extensions/gh-actions-lock" "$runtime/npm"

# Register the mise-installed binary locally; no gh extension downloads.
extension="$runtime/extensions/gh-actions-lock/gh-actions-lock"
ln -sfn "$(command -v gh-actions-lock)" "$extension"

export GH_CONFIG_DIR="$runtime/gh-config" XDG_DATA_HOME="$runtime/share"
registration="$XDG_DATA_HOME/gh/extensions/gh-actions-lock"

if [[ -L $registration ]]; then rm "$registration"; fi
(cd "$(dirname "$extension")" && gh extension install .)

# Reuse the raw SDKs, not the rules_go runner with its automatic tidy hooks.
bazel build --lockfile_mode=error //tools/renovate:go //tools/renovate:node
execroot=$(bazel info execution_root)
go=$(realpath "$execroot/$(bazel cquery //tools/renovate:go --lockfile_mode=error --output=files)")
node=$(realpath "$execroot/$(bazel cquery //tools/renovate:node --lockfile_mode=error --output=files)")
GOROOT=$(dirname "$(dirname "$go")")
PATH="$runtime/bin:$(dirname "$node"):$(dirname "$go"):$PATH"
export GOROOT PATH GOTOOLCHAIN=local
export RENOVATE_BINARY_SOURCE=global

cp "$source_dir/package.json" "$source_dir/package-lock.json" "$runtime/npm/"
npm ci --prefix "$runtime/npm" --ignore-scripts --engine-strict --no-audit --no-fund
modules="$runtime/npm/node_modules"
# No RE2 install script or downloaded prebuilt binaries; use locked sources/headers.
"$node" "$modules/node-gyp/bin/node-gyp.js" rebuild \
  --directory="$modules/re2" --nodedir="$(dirname "$(dirname "$node")")"
(cd "$runtime/npm" && "$node" -e "require('re2')")
ln -sfn "$modules/renovate/dist/renovate.js" "$runtime/bin/renovate"
ln -sfn "$modules/renovate/dist/config-validator.js" "$runtime/bin/renovate-config-validator"

# Forward only SDK/extension settings through Renovate's child-env sanitization.
export RENOVATE_CUSTOM_ENV_VARIABLES
RENOVATE_CUSTOM_ENV_VARIABLES=$(jq -cn \
  --argjson existing "${RENOVATE_CUSTOM_ENV_VARIABLES:-null}" \
  --arg GOROOT "$GOROOT" --arg GOTOOLCHAIN "$GOTOOLCHAIN" \
  --arg GH_CONFIG_DIR "$GH_CONFIG_DIR" --arg XDG_DATA_HOME "$XDG_DATA_HOME" \
  '$existing + ($ARGS.named | del(.existing))')
: >"$runtime/env.sh"

for key in PATH GOROOT GOTOOLCHAIN GH_CONFIG_DIR XDG_DATA_HOME RENOVATE_BINARY_SOURCE RENOVATE_CUSTOM_ENV_VARIABLES; do
  printf 'export %s=%q\n' "$key" "${!key}" >>"$runtime/env.sh"
  if [[ -n ${GITHUB_ENV:-} ]]; then printf '%s=%s\n' "$key" "${!key}" >>"$GITHUB_ENV"; fi
done
