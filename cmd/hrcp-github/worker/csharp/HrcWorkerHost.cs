// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using GitHub.Runner.Common;
using GitHub.Runner.Sdk;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Net.Client;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Wire = Hrc.Provider;

namespace GitHub.Runner.Worker
{
    public sealed class HrcWorkerHost : RunnerService, IProcessChannel
    {
        // Keep in sync with config.GRPCMaxMessageSize in internal/config/limits.go.
        private const int MaxMessageSize = 256 << 10;

        private static readonly AsyncLocal<HrcWorkerHost> CurrentChannel = new();
        public static bool Handles(string[] args) => args.Length > 0 && (args[0] == "-connect" || args[0] == "-version");

        public static IProcessChannel CreateChannel(IHostContext context)
        {
            if (CurrentChannel.Value == null) return context.CreateService<IProcessChannel>();
            CurrentChannel.Value.Initialize(context);
            return CurrentChannel.Value;
        }

        private readonly Channel<WorkerMessage> _messages = Channel.CreateUnbounded<WorkerMessage>(
            new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });
        private readonly object _gate = new();
        private MessageType? _shutdown;
        private bool _disposed;

        public HrcWorkerHost(string job = null)
        {
            if (job != null) _messages.Writer.TryWrite(new WorkerMessage(MessageType.NewJobRequest, job));
        }

        public override void Initialize(IHostContext context)
        {
            lock (_gate)
            {
                base.Initialize(context);
                Trace.Info("Using gRPC process channel.");
                ApplyShutdown();
            }
        }

        public void Request(MessageType reason)
        {
            if (reason != MessageType.CancelRequest && reason != MessageType.RunnerShutdown &&
                reason != MessageType.OperatingSystemShutdown)
            {
                throw new ArgumentOutOfRangeException(nameof(reason));
            }
            lock (_gate)
            {
                if (_disposed) return;
                if (reason != MessageType.CancelRequest)
                {
                    _shutdown = reason;
                    ApplyShutdown();
                }
                _messages.Writer.TryWrite(new WorkerMessage(reason, string.Empty));
            }
        }

        private void ApplyShutdown()
        {
            if (HostContext != null && _shutdown.HasValue)
            {
                HostContext.ShutdownRunner(_shutdown == MessageType.OperatingSystemShutdown
                    ? ShutdownReason.OperatingSystemShutdown : ShutdownReason.UserCancelled);
            }
        }

        public void StartClient(string pipeNameInput, string pipeNameOutput) { }
        public void StartServer(StartProcessDelegate startProcess) => throw new NotSupportedException();
        public Task SendAsync(MessageType type, string body, CancellationToken cancellationToken) => throw new NotSupportedException();
        public Task<WorkerMessage> ReceiveAsync(CancellationToken cancellationToken) => _messages.Reader.ReadAsync(cancellationToken).AsTask();
        // Upstream Worker disposes its IProcessChannel when execution ends.
        // Transport resources remain scoped to ConnectAsync until parent retirement.
        public void Dispose()
        {
            lock (_gate)
            {
                _disposed = true;
                _messages.Writer.TryComplete();
            }
        }

        public static async Task<int> RunAsync(string[] args)
        {
            if (args.Length == 1 && args[0] == "-version")
            {
                Console.WriteLine(BuildConstants.RunnerPackage.Version);
                return 0;
            }
            if (args.Length != 2 || args[0] != "-connect") return 1;
            try
            {
                using var host = new HrcWorkerHost();
                await host.ConnectAsync(args[1]);
                return 0;
            }
            catch (OperationCanceledException) { return 0; }
            // Parent retirement can race completion of the shutdown reply's
            // HTTP/2 write. Some platforms surface the closed transport directly
            // as HttpIOException rather than wrapping it in RpcException.
            catch (HttpIOException) { return 0; }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled || ex.StatusCode == StatusCode.Unavailable) { return 0; }
            catch (Exception ex)
            {
                // Malformed jobs can contain secrets; never print their payloads.
                Console.Error.WriteLine($"Worker transport failed: {ex.GetType().Name}");
                return 1;
            }
        }

        private async Task ConnectAsync(string socketPath)
        {
            int connected = 0;
            using var handler = new SocketsHttpHandler
            {
                ConnectCallback = async (_, token) =>
                {
                    // A replacement connection must never replay a task.
                    if (Interlocked.Exchange(ref connected, 1) != 0) throw new IOException("Parent connection lost");
                    var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
                    try
                    {
                        using var startup = CancellationTokenSource.CreateLinkedTokenSource(token);
                        startup.CancelAfter(TimeSpan.FromSeconds(30));
                        await socket.ConnectAsync(new UnixDomainSocketEndPoint(socketPath), startup.Token);
                        return new NetworkStream(socket, ownsSocket: true);
                    }
                    catch { socket.Dispose(); throw; }
                },
            };
            using var connection = GrpcChannel.ForAddress("http://parent", new GrpcChannelOptions
            {
                HttpHandler = handler,
                MaxReceiveMessageSize = MaxMessageSize,
                MaxSendMessageSize = MaxMessageSize,
            });
            using var execute = new Wire.WorkerService.WorkerServiceClient(connection).Execute();
            using var writes = new SemaphoreSlim(1);
            async Task Send(Wire.TaskEvent message)
            {
                await writes.WaitAsync();
                try { await execute.RequestStream.WriteAsync(message); }
                finally { writes.Release(); }
            }

            Task execution = null;
            var state = Wire.WorkerState.Ready;
            try
            {
                await Send(new Wire.TaskEvent { Ready = true });
                // The sole reader owns idle/running/stopping transitions.
                // Job execution only writes its terminal outcome.
                while (await execute.ResponseStream.MoveNext(CancellationToken.None))
                {
                    var command = execute.ResponseStream.Current;
                    if (command.Control != null)
                    {
                        var request = command.Control;
                        var reply = new Wire.WorkerReply { Id = request.Id };
                        if (request.MethodCase == Wire.WorkerRequest.MethodOneofCase.Shutdown)
                        {
                            if (request.Shutdown.Reason > 2) throw new InvalidOperationException("Invalid shutdown reason");
                            state = Wire.WorkerState.Stopping;
                            reply.Exited = execution == null;
                            Request(request.Shutdown.Reason == 0 ? MessageType.CancelRequest : MessageType.RunnerShutdown);
                        }
                        else if (request.MethodCase != Wire.WorkerRequest.MethodOneofCase.Status || !request.Status)
                            throw new InvalidOperationException("Invalid control request");
                        reply.State = state;
                        await Send(new Wire.TaskEvent { Reply = reply });
                    }
                    else if (command.Start != null && execution == null && state == Wire.WorkerState.Ready)
                        execution = ExecuteJob(command.Start, Send);
                    else if (command.Stop != null && execution != null && command.Stop.Reason <= 2)
                        Request(command.Stop.Reason == 0 ? MessageType.CancelRequest : MessageType.RunnerShutdown);
                    else throw new InvalidOperationException("Invalid worker command");
                }
            }
            finally
            {
                // Parent loss must stop upstream Worker too, even when it
                // happens before HostContext has initialized the channel.
                Request(MessageType.RunnerShutdown);
                if (execution != null) await execution;
            }
        }

        private async Task ExecuteJob(Wire.TaskStart start, Func<Wire.TaskEvent, Task> send)
        {
            Wire.Outcome outcome;
            try
            {
                JObject payload;
                try
                {
                    payload = JObject.Parse(start.Payload.ToStringUtf8());
                    // Queue NewJob before yielding to the command loop: even
                    // an immediate Stop must follow it in IProcessChannel.
                    _messages.Writer.TryWrite(new WorkerMessage(MessageType.NewJobRequest, payload["Job"]?.ToString(Formatting.None) ?? ""));
                }
                catch { throw new JobConfigurationException("job"); }
                int result = await Task.Run(() => RunJob(start.Config.ToStringUtf8(), payload));
                outcome = result >= 100 && result <= 105
                    ? new Wire.Outcome { Result = (uint)(result - 99), Data = ByteString.CopyFrom(new byte[] { 1 }) }
                    : new Wire.Outcome { Error = new Wire.Failure { Kind = "unknown", Message = "Worker did not return a task result" } };
            }
            catch (JobConfigurationException ex) { outcome = new Wire.Outcome { Error = new Wire.Failure { Kind = ex.Kind, Message = "Invalid worker " + ex.Kind } }; }
            catch { outcome = new Wire.Outcome { Error = new Wire.Failure { Kind = "unknown", Message = "Worker execution failed" } }; }
            // Retain the channel until the parent has consumed this outcome.
            await send(new Wire.TaskEvent { Outcome = outcome });
        }

        private int RunJob(string config, JObject payload)
        {
            CurrentChannel.Value = this;
            try
            {
                using var context = Prepare(config, payload);
                return Program.MainAsync(context, new[] { "spawnclient", "grpc", "grpc" }).GetAwaiter().GetResult();
            }
            finally { CurrentChannel.Value = null; }
        }

        private static HostContext Prepare(string config, JObject job)
        {
            RunnerSettings settings;
            WorkerOptions options;
            string id;
            try
            {
                id = job.Value<string>("ID");
                settings = job["Settings"]?.ToObject<RunnerSettings>() ?? new RunnerSettings();
                if (job["Job"] is not JObject) throw new ArgumentException();
                if (string.IsNullOrEmpty(id) || id == "." || id == ".." || id.IndexOfAny(new[] { '/', '\\', ':' }) >= 0) throw new ArgumentException();
            }
            catch { throw new JobConfigurationException("job"); }
            try
            {
                options = JObject.Parse(config).ToObject<WorkerOptions>();
                string dir = options.Dir;
                if (string.IsNullOrEmpty(dir)) throw new ArgumentException();
                dir = Path.GetFullPath(dir);
                foreach (string entry in options.Env ?? Array.Empty<string>()) SetEnvironment(entry);
                settings.WorkFolder = ".";
                settings.DisableUpdate = true;
                if (string.IsNullOrEmpty(settings.AgentName)) settings.AgentName = Environment.MachineName;
                string logs = options.LogRoot;
                if (string.IsNullOrEmpty(logs)) logs = Path.Combine(Directory.GetCurrentDirectory(), "_diag");
                return new HostContext("Worker", settings: settings, directories: new Dictionary<WellKnownDirectory, string>
                {
                    [WellKnownDirectory.Root] = Directory.GetCurrentDirectory(),
                    [WellKnownDirectory.Diag] = Path.GetFullPath(Path.Combine(logs, "jobs", id)),
                    [WellKnownDirectory.Scripts] = Path.Combine(dir, "scripts"),
                    [WellKnownDirectory.Externals] = Path.Combine(dir, "externals"),
                });
            }
            catch { throw new JobConfigurationException("config"); }
        }

        private static void SetEnvironment(string entry)
        {
            int equal = entry?.IndexOf('=') ?? -1;
            if (equal <= 0 || entry.IndexOf('\0') >= 0)
            {
                throw new ArgumentException();
            }
            string name = entry.Substring(0, equal);
            if (name.Equals("RUNNER_TRACKING_GROUP", StringComparison.OrdinalIgnoreCase))
            {
                throw new ArgumentException();
            }
            Environment.SetEnvironmentVariable(name, entry.Substring(equal + 1));
        }

        private sealed class WorkerOptions
        {
            public string Dir { get; set; }
            public string LogRoot { get; set; }
            public string[] Env { get; set; }
        }

        private sealed class JobConfigurationException : Exception
        {
            public string Kind { get; }
            public JobConfigurationException(string kind) { Kind = kind; }
        }
    }
}
