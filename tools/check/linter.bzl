# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

"""Declare the repository's rules_lint aspects."""

load("@aspect_rules_lint//lint:buildifier.bzl", "lint_buildifier_aspect")
load("@aspect_rules_lint//lint:lint_test.bzl", "lint_test")
load("@aspect_rules_lint//lint:shellcheck.bzl", "lint_shellcheck_aspect")

# Keep author-defined ordering of named arguments when formatting.
BUILDIFIER_ARGS = ["-buildifier_disable=callsort"]

def _buildifier_file_impl(ctx):
    tool = ctx.toolchains["@buildifier_prebuilt//buildifier:toolchain"].tool
    return [DefaultInfo(files = depset([tool]))]

buildifier_file = rule(
    implementation = _buildifier_file_impl,
    toolchains = ["@buildifier_prebuilt//buildifier:toolchain"],
)

buildifier = lint_buildifier_aspect(
    args = BUILDIFIER_ARGS,
    binary = Label("//tools/check:buildifier_lint"),
    warnings = "all",
)

shellcheck = lint_shellcheck_aspect(
    binary = Label("//tools/check:shellcheck"),
    config = Label("//tools/check:.shellcheckrc"),
    rule_kinds = ["filegroup"],
)

buildifier_test = lint_test(aspect = buildifier)
shellcheck_test = lint_test(aspect = shellcheck)
