// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

import { rmSync } from "node:fs";

// State is scoped to this invocation; never remove the caller's own netrc file.
if (process.env.STATE_directory) {
  rmSync(process.env.STATE_directory, { recursive: true, force: true });
}
