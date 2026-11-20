# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

"""Shared formatter configuration for local fixes and cacheable CI checks."""

load("@aspect_rules_lint//format:defs.bzl", "format_multirun")
load("@rules_python//python:py_test.bzl", "py_test")
load(":linter.bzl", "BUILDIFIER_ARGS")

_FORMATTERS = {
    # Sandboxed checks must not depend on an undeclared Git checkout.
    "disable_git_attribute_checks": True,

    # sh checks
    "shell": "@aspect_rules_lint//format:shfmt",
    "shell_check_args": ["--diff", "-i", "2"],
    "shell_fix_args": ["-w", "-i", "2"],

    # starlark checks
    "starlark": "//tools/check:buildifier",
    "starlark_check_args": ["-mode=check"] + BUILDIFIER_ARGS,
    "starlark_fix_args": ["-mode=fix"] + BUILDIFIER_ARGS,
}

def _source_manifest_impl(ctx):
    manifest = ctx.actions.declare_file(ctx.label.name + ".sources")
    ctx.actions.write(manifest, "\n".join(
        [ctx.workspace_name] + [src.short_path for src in ctx.files.srcs],
    ) + "\n")
    return [DefaultInfo(files = depset([manifest]))]

_source_manifest = rule(
    implementation = _source_manifest_impl,
    attrs = {"srcs": attr.label_list(allow_files = True)},
)

def repository_format(name, srcs, visibility = None):
    """Declare a cacheable check supporting explicit --fix via bazel run.

    Args:
        name: Name of the format test/executable.
        srcs: Explicit source inputs for cacheable checks.
        visibility: Visibility of the generated target.
    """
    tools = name + "_tools"
    manifest = name + "_sources"
    format_multirun(
        name = tools,
        tags = ["manual"],
        **_FORMATTERS
    )
    _source_manifest(name = manifest, srcs = srcs)
    py_test(
        name = name,
        srcs = [Label(":format.py")],
        main = Label(":format.py"),
        data = srcs + [":" + tools, ":" + tools + ".check", ":" + manifest],
        deps = ["@rules_python//python/runfiles"],
        env = {
            "FORMAT_CHECK": "$(rlocationpath :" + tools + ".check)",
            "FORMAT_FIX": "$(rlocationpath :" + tools + ")",
            "FORMAT_SOURCES": "$(rlocationpath :" + manifest + ")",
        },
        legacy_create_init = False,
        size = "small",
        visibility = visibility,
    )
