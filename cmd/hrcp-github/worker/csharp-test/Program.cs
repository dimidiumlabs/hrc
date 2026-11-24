// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using GitHub.Runner.Common;
using GitHub.Runner.Worker;

internal static class Program
{
    private static void Check(bool condition, string message)
    {
        if (!condition) throw new Exception(message);
    }

    private static void CheckEnvironment(string[] expected)
    {
        foreach (string entry in expected)
        {
            int equal = entry.IndexOf('=');
            string name = entry.Substring(0, equal);
            Check(Environment.GetEnvironmentVariable(name) == entry.Substring(equal + 1), name + " was not preserved");
        }
    }

    private static void CheckEnvironmentUnchanged(IDictionary expected)
    {
        var actual = Environment.GetEnvironmentVariables();
        Check(actual.Count == expected.Count, "runtime configuration changed environment variables");
        foreach (string name in expected.Keys)
            Check(Equals(expected[name], actual[name]), name + " changed during runtime configuration");
    }

    private static async Task CheckChildEnvironment(string[] expected, CancellationToken token)
    {
        CheckEnvironment(expected);
        var start = new ProcessStartInfo(Environment.ProcessPath) { UseShellExecute = false };
        start.ArgumentList.Add("--check-env");
        foreach (string entry in expected) start.ArgumentList.Add(entry);
        // Do not filter the child's environment: test ordinary inheritance.
        using var child = Process.Start(start);
        await child.WaitForExitAsync(token);
        Check(child.ExitCode == 0, "child environment contract failed");
    }

    private static HostContext CreateContext(string root, string diag, RunnerSettings settings) =>
        new("Worker", settings: settings, directories: new Dictionary<WellKnownDirectory, string>
        {
            [WellKnownDirectory.Root] = root,
            [WellKnownDirectory.Diag] = diag,
            [WellKnownDirectory.Scripts] = Path.Combine(root, "scripts"),
            [WellKnownDirectory.Externals] = Path.Combine(root, "externals"),
        });

    public static async Task Main(string[] args)
    {
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var token = deadline.Token;
        if (args.Length > 0 && args[0] == "--check-env")
        {
            CheckEnvironment(args[1..]);
            return;
        }
        if (args.Length == 2 && args[0] == "--message-size")
        {
            // Cross-language constants cannot be imported from the Go config package.
            // Check the private limit without exposing a production API for tests.
            var limit = typeof(HrcWorkerHost).GetField("MaxMessageSize", BindingFlags.NonPublic | BindingFlags.Static);
            Check(limit != null && (int)limit.GetRawConstantValue() == int.Parse(args[1]), "Go and C# gRPC limits differ");
        }
        var environment = Environment.GetEnvironmentVariables();
        if (args.Length > 0 && args[0] == "--worker-env")
        {
            Check(await HrcWorkerHost.RunAsync(args[1..]) == 0, "worker transport failed");
            // Only the explicitly requested user environment may change.
            environment["HRC_TEST_USER"] = "explicit";
            CheckEnvironmentUnchanged(environment);
            await CheckChildEnvironment(new[]
            {
                "RUNNER_TRACKING_GROUP=tracking-from-parent",
                "HRC_TEST_USER=explicit",
                "HRC_TEST_DOTENV=inherited",
            }, token);
            return;
        }
        using (var channel = new HrcWorkerHost("{\"raw\":true}"))
        {
            channel.StartClient("grpc", "grpc");
            channel.Request(MessageType.CancelRequest);
            var first = await channel.ReceiveAsync(token);
            Check(first.MessageType == MessageType.NewJobRequest && first.Body == "{\"raw\":true}", "job must precede an early cancel");
            Check((await channel.ReceiveAsync(token)).MessageType == MessageType.CancelRequest, "cancel mapping");
            using var cancel = new CancellationTokenSource();
            var waiting = channel.ReceiveAsync(cancel.Token);
            cancel.Cancel();
            try { await waiting; throw new Exception("ReceiveAsync ignored cancellation"); }
            catch (OperationCanceledException) { }
            channel.Request(MessageType.RunnerShutdown);
            Check((await channel.ReceiveAsync(token)).MessageType == MessageType.RunnerShutdown, "shutdown mapping");
        }

        string root = Path.Combine(Path.GetTempPath(), "hrc-channel-" + Guid.NewGuid());
        Directory.CreateDirectory(root);
        try
        {
            foreach (string name in new[] { ".runner", ".credentials", ".credentials_rsaparams", ".runner_migrated" })
                File.WriteAllText(Path.Combine(root, name), "invalid legacy configuration");
            var settings = new RunnerSettings { AgentId = 7, AgentName = "channel-test", WorkFolder = "." };
            foreach (var reason in new[] { MessageType.RunnerShutdown, MessageType.OperatingSystemShutdown })
            {
                string diag = Path.Combine(root, reason.ToString());
                using var context = CreateContext(root, diag, settings);
                Check(ReferenceEquals(settings, context.GetService<IConfigurationStore>().GetSettings()), "settings were not passed directly");
                Check(context.GetDirectory(WellKnownDirectory.Diag) == diag, "diagnostics path override lost");
                Check(context.GetDirectory(WellKnownDirectory.Scripts) == Path.Combine(root, "scripts"), "scripts path override lost");
                Check(context.GetDirectory(WellKnownDirectory.Externals) == Path.Combine(root, "externals"), "externals path override lost");
                using var channel = new HrcWorkerHost("{}");
                channel.Initialize(context);
                await channel.ReceiveAsync(token);
                channel.Request(MessageType.CancelRequest);
                await channel.ReceiveAsync(token);
                Check(!context.RunnerShutdownToken.IsCancellationRequested, "job cancel must not shut down the host");
                // Worker consumes only the first stop. A later shutdown must
                // still reach HostContext and escalate that cancellation.
                channel.Request(reason);
                Check(context.RunnerShutdownToken.IsCancellationRequested, "shutdown did not reach HostContext");
                channel.Dispose();
                channel.Request(reason);
            }
            using (var context = CreateContext(root, Path.Combine(root, "early"), settings))
            using (var channel = new HrcWorkerHost("{}"))
            {
                channel.Request(MessageType.RunnerShutdown);
                channel.Initialize(context);
                Check(context.RunnerShutdownToken.IsCancellationRequested, "early shutdown lost during initialization");
            }
            foreach (string reason in new[] { "RunnerShutdown", "OperatingSystemShutdown", "early" })
                Check(Directory.GetFiles(Path.Combine(root, reason), "Worker_*.log").Length == 1, "diagnostics are not isolated per invocation");
            CheckEnvironmentUnchanged(environment);
            await CheckChildEnvironment(Array.Empty<string>(), token);
        }
        finally { Directory.Delete(root, recursive: true); }
    }
}
