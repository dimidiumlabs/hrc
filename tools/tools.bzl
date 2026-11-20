# SPDX-FileCopyrightText: 2026 Nikolay Govorov
# SPDX-License-Identifier: BSD-3-Clause

"""Build tools and the documentation archive."""

def _export_tool_impl(ctx):
    tool = ctx.file.src
    suffix = ".exe" if not ctx.file.wrapper and tool.extension == "exe" else ""
    executable = ctx.actions.declare_file(ctx.label.name + suffix)
    if ctx.file.wrapper:
        runfile = tool.short_path.removeprefix("../") if tool.short_path.startswith("../") else ctx.workspace_name + "/" + tool.short_path
        ctx.actions.expand_template(
            template = ctx.file.wrapper,
            output = executable,
            substitutions = {"@TOOL_RUNFILE@": runfile},
            is_executable = True,
        )
    else:
        ctx.actions.symlink(output = executable, target_file = tool, is_executable = True)
    runfiles = ctx.runfiles(files = [tool]).merge(ctx.attr.src[DefaultInfo].default_runfiles)
    providers = [DefaultInfo(executable = executable, runfiles = runfiles)]
    if RunEnvironmentInfo in ctx.attr.src:
        providers.append(ctx.attr.src[RunEnvironmentInfo])
    return providers

def _docs_archive_impl(ctx):
    output = ctx.actions.declare_directory(ctx.label.name)
    ctx.actions.run_shell(
        inputs = ctx.files.srcs + [ctx.file.conf, ctx.info_file],
        tools = [
            ctx.attr._helper[DefaultInfo].files_to_run,
            ctx.attr._sphinx[DefaultInfo].files_to_run,
            ctx.file._esbuild,
        ],
        outputs = [output],
        arguments = [
            ctx.executable._helper.path,
            ctx.executable._sphinx.path,
            ctx.file.conf.dirname,
            output.path,
            ctx.info_file.path,
            ctx.file._esbuild.path,
        ],
        command = """
            set -euo pipefail
            work=$(mktemp -d)
            trap 'rm -rf "$work"' EXIT
            "$1" --stage . "$work/src"
            export HRC_DOCS_VERSION="$(awk '$1 == "STABLE_HRC_VERSION" { print $2 }' "$5")"
            export HRC_DOCS_COMMIT="$(awk '$1 == "STABLE_HRC_COMMIT" { print $2 }' "$5")"
            : "${HRC_DOCS_VERSION:?Missing STABLE_HRC_VERSION in workspace status}"
            export HRC_ESBUILD="$(pwd)/$6"
            SOURCE_DATE_EPOCH=0 "$2" -b html -n -W --keep-going \\
                -c "$3" -d "$work/doctrees" "$work/src" "$work/html"
            SOURCE_DATE_EPOCH=0 "$2" -b text -n -W --keep-going \\
                -c "$3" -d "$work/text-doctrees" "$work/src" "$work/text"
            cp -R "$work/text/." "$work/html/"
            rm -f "$work/html/.buildinfo"
            "$1" --archive "$work/html" "$4"
        """,
        mnemonic = "BuildDocs",
        progress_message = "Building versioned documentation archive",
    )
    return [DefaultInfo(files = depset([output]))]

docs_archive = rule(
    implementation = _docs_archive_impl,
    attrs = {
        "conf": attr.label(allow_single_file = True, mandatory = True),
        "srcs": attr.label_list(allow_files = True),
        "_esbuild": attr.label(default = Label("//tools:esbuild"), allow_single_file = True, cfg = "exec"),
        "_helper": attr.label(default = Label("//tools:site_dev"), executable = True, cfg = "exec"),
        "_sphinx": attr.label(default = Label("//tools:sphinx"), executable = True, cfg = "exec"),
    },
)

export_tool = rule(
    implementation = _export_tool_impl,
    doc = "Expose a native tool at bazel-bin/<package>/<name>, preserving its runfiles.",
    executable = True,
    attrs = {
        "src": attr.label(mandatory = True, allow_single_file = True, cfg = "exec"),
        "wrapper": attr.label(allow_single_file = True, doc = "Optional shell template with @TOOL_RUNFILE@."),
    },
)
