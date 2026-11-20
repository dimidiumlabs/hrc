# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

"""Expose the host Go SDK executable without the go_bin_runner's tidy hooks."""

load("@platforms//host:constraints.bzl", "HOST_CONSTRAINTS")

def _go_sdk_binary_impl(ctx):
    sdk = ctx.toolchains["@rules_go//go:toolchain"].sdk
    return [DefaultInfo(files = depset([sdk.go]))]

go_sdk_binary = rule(
    implementation = _go_sdk_binary_impl,
    toolchains = ["@rules_go//go:toolchain"],
    exec_compatible_with = HOST_CONSTRAINTS,
)
