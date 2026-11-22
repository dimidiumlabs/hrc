# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

"""Register typed package sources shared by formatting and linting."""

def check_sources(name = "check_sources", bazel = ["BUILD.bazel", "*.bzl"], sh = [], go = ["*.go"], py = ["*.py"], deps = []):
    """Declare typed source groups and their combined check_sources target.

    Place this call immediately after the load statements, before other rules.

    Args:
        name: Name of the combined source group (one mixin per package).
        bazel: Package-local glob patterns for Bazel/Starlark files.
        sh: Package-local glob patterns for POSIX sh scripts, including extensionless files.
            Use Python rather than Bash when portable shell is unsuitable.
        go: Package-local glob patterns for owned Go sources, including tests and all platforms.
        py: Package-local glob patterns for owned Python sources, including tests.
        deps: Packages whose check sources should be included, e.g. ["//tools/check"].
    """

    existing = native.existing_rules().keys()
    if existing:
        fail("check_sources must be the first rule declaration, immediately after load statements; found: %s" % existing)
    for kind, patterns in {"bazel": bazel, "go": go, "py": py, "sh": sh}.items():
        native.filegroup(
            name = kind + "_sources",
            srcs = native.glob(patterns, allow_empty = True) + [package + ":" + kind + "_sources" for package in deps],
            tags = ["manual"] + (["lint-with-buildifier"] if kind == "bazel" else []),
            visibility = ["//visibility:public"],
        )
    native.filegroup(
        name = name,
        srcs = [":bazel_sources", ":sh_sources", ":go_sources", ":py_sources"],
        tags = ["manual"],
        visibility = ["//visibility:public"],
    )
