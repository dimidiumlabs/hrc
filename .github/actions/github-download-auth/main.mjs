// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

import { appendFileSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const token = process.env.INPUT_TOKEN;
if (!token || /\s/.test(token)) {
  throw new Error("A nonempty GitHub token without whitespace is required");
}
console.log(`::add-mask::${token.replaceAll("%", "%25")}`);

const directory = mkdtempSync(
  join(process.env.RUNNER_TEMP, "github-download-auth-"),
);

const netrc = join(directory, "netrc");
try {
  // Register cleanup before writing credentials, including if exporting NETRC fails.
  appendFileSync(process.env.GITHUB_STATE, `directory=${directory}\n`);
  writeFileSync(
    netrc,
    `machine github.com\nlogin x-access-token\npassword ${token}\n`,
    { mode: 0o600, flag: "wx" },
  );
  appendFileSync(process.env.GITHUB_ENV, `NETRC=${netrc}\n`);
} catch (error) {
  rmSync(directory, { recursive: true, force: true });
  throw error;
}
