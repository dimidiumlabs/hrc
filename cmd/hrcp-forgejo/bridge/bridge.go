// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

// Package bridge adapts Forgejo Runner's internal API to HRC provider processes.
// Its upstream import path is intentional; see ../patch for upstream changes.
package bridge

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	runnerv1 "code.forgejo.org/forgejo/actions-proto/runner/v1"
	actrunner "code.forgejo.org/forgejo/runner/v13/act/runner"
	"code.forgejo.org/forgejo/runner/v13/internal/app/run"
	"code.forgejo.org/forgejo/runner/v13/internal/pkg/client"
	"code.forgejo.org/forgejo/runner/v13/internal/pkg/config"
	"code.forgejo.org/forgejo/runner/v13/internal/pkg/labels"
	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const Version = "v13.2.2"

// Connection uses an already registered runner identity, not a registration token.
type Connection struct {
	URL   string `json:"url"`
	UUID  string `json:"uuid"`
	Token string `json:"token"`
}

// Assignment is transported only over HRC's private process channels.
// Both the task and the runner connection contain credentials.
type Assignment struct {
	Connection Connection `json:"connection"`
	Task       []byte     `json:"task"`
}

var ErrNoTask = errors.New("no task available")

// Reported is the worker's acknowledgment that upstream finalized the task.
const Reported = "forgejo-reported-v1"

func Register(ctx context.Context, address, name, registrationToken string) (Connection, error) {
	cli := (Connection{URL: address}).client()
	resp, err := cli.Register(ctx, connect.NewRequest(&runnerv1.RegisterRequest{
		Name: name, Token: registrationToken, Version: Version, Labels: []string{"poc"},
	}))
	if err != nil {
		return Connection{}, err
	}
	return Connection{URL: address, UUID: resp.Msg.Runner.Uuid, Token: resp.Msg.Runner.Token}, nil
}

func (c Connection) Declare(ctx context.Context) error {
	_, err := c.client().Declare(ctx, connect.NewRequest(&runnerv1.DeclareRequest{
		Version: Version, Labels: []string{"poc"},
	}))
	return err
}

// FinishUnreported closes an acquired task that could not be finalized by its
// worker (including a failure to launch it). Do not call after Reported.
func (c Connection) FinishUnreported(ctx context.Context, payload []byte, result runnerv1.Result) error {
	var task runnerv1.Task
	if err := proto.Unmarshal(payload, &task); err != nil {
		return err
	}
	_, err := c.client().UpdateTask(ctx, connect.NewRequest(&runnerv1.UpdateTaskRequest{
		State: &runnerv1.TaskState{Id: task.Id, Result: result, StoppedAt: timestamppb.Now()},
	}))
	return err
}

func (c Connection) client() client.Client {
	return client.New(c.URL, false, c.UUID, c.Token, Version, time.Second)
}

// FetchOne performs one acquisition request. A caller retrying an ambiguous
// failure must retain requestKey. The provider owns the acquisition loop.
func FetchOne(ctx context.Context, c Connection, requestKey uuid.UUID) ([]byte, error) {
	if err := c.Declare(ctx); err != nil {
		return nil, fmt.Errorf("declare: %w", err)
	}
	return c.Fetch(ctx, requestKey)
}

// Fetch acquires at most one task. Call Declare before the acquisition loop.
func (c Connection) Fetch(ctx context.Context, requestKey uuid.UUID) ([]byte, error) {
	cli := c.client()
	cleanup := cli.SetRequestKey(requestKey)
	defer cleanup()
	capacity := int64(1)
	resp, err := cli.FetchTask(ctx, connect.NewRequest(&runnerv1.FetchTaskRequest{TaskCapacity: &capacity}))
	if err != nil {
		return nil, fmt.Errorf("fetch: %w", err)
	}
	if len(resp.Msg.GetAdditionalTasks()) != 0 {
		return nil, errors.New("server exceeded requested capacity of one")
	}
	if resp.Msg.Task == nil {
		return nil, ErrNoTask
	}
	return proto.Marshal(resp.Msg.Task)
}

// Execution configures a single worker process. Dir is HRC's runtime directory,
// containing externals/nodeXX/bin/node; it is not the job workspace.
type Execution struct {
	Workdir string
	Dir     string
	Log     io.Writer
}

// NodeRuntime resolves the exact requested version without consulting PATH or
// upgrading older actions. An unbundled version fails explicitly.
func NodeRuntime(dir, using string) (string, error) {
	switch using {
	case "node12", "node16", "node20", "node24":
	default:
		return "", fmt.Errorf("unsupported JavaScript runtime %q", using)
	}
	if dir == "" {
		return "", fmt.Errorf("runtime directory is required for %s", using)
	}
	name := "node"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	path, err := filepath.Abs(filepath.Join(dir, "externals", using, "bin", name))
	if err != nil {
		return "", err
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf("%s runtime unavailable: %w", using, err)
	}
	if !info.Mode().IsRegular() || (runtime.GOOS != "windows" && info.Mode().Perm()&0111 == 0) {
		return "", fmt.Errorf("%s runtime is not executable: %s", using, path)
	}
	return path, nil
}

type jobLogger struct{ output io.Writer }

func (l jobLogger) WithJobLogger() *logrus.Logger {
	logger := logrus.New()
	logger.SetOutput(l.output)
	logger.SetLevel(logrus.DebugLevel)
	logger.SetFormatter(&logrus.TextFormatter{DisableColors: true, FullTimestamp: true})
	return logger
}

// Execute runs exactly one task with upstream Runner and Reporter, without its
// daemon/CLI/poller. It owns the process-global upstream logger for its duration;
// callers must use a separate worker process per job, not concurrent executions.
func Execute(ctx context.Context, c Connection, execution Execution, payload []byte) (runnerv1.Result, error) {
	var task runnerv1.Task
	if err := proto.Unmarshal(payload, &task); err != nil {
		return runnerv1.Result_RESULT_UNSPECIFIED, fmt.Errorf("decode task: %w", err)
	}
	if task.Id <= 0 || task.Context == nil || len(task.WorkflowPayload) == 0 {
		return runnerv1.Result_RESULT_UNSPECIFIED, errors.New("incomplete task")
	}
	output := execution.Log
	if output == nil {
		output = os.Stderr
	}
	logger := logrus.StandardLogger()
	oldOutput, oldLevel := logger.Out, logger.GetLevel()
	logger.SetOutput(output)
	logger.SetLevel(logrus.DebugLevel)
	defer func() { logger.SetOutput(oldOutput); logger.SetLevel(oldLevel) }()
	logger.WithField("task", task.Id).Debug("Executing Forgejo task")
	ctx = actrunner.WithJobLoggerFactory(ctx, jobLogger{output: output})
	ctx = actrunner.WithNodeRuntimeResolver(ctx, func(using string) (string, error) {
		return NodeRuntime(execution.Dir, using)
	})
	cfg, err := config.New(func(cfg *config.Config) error {
		cfg.Host.WorkdirParent = execution.Workdir
		cfg.Log.JobLevel = "debug"
		cfg.Cache.Enabled = false
		cfg.Runner.Timeout = time.Minute
		cfg.Runner.ReportInterval = 20 * time.Millisecond
		cfg.Runner.ReportRetry.MaxRetries = 2
		cfg.Runner.ReportRetry.InitialDelay = time.Millisecond
		return nil
	})
	if err != nil {
		return runnerv1.Result_RESULT_UNSPECIFIED, err
	}
	cli := &observedClient{Client: c.client()}
	runner := run.NewRunner(cfg, "hrc-forgejo-poc", labels.Labels{labels.MustParse("poc:host")}, cli, nil)
	runner.Run(ctx, &task)
	// Upstream Run returns void. Observe the *acknowledged* terminal update,
	// not the process exit status, to distinguish job failure from RPC failure.
	cli.mu.Lock()
	defer cli.mu.Unlock()
	if !cli.finished {
		return runnerv1.Result_RESULT_UNSPECIFIED, fmt.Errorf("no acknowledged terminal update: %w", errors.Join(cli.lastError, ctx.Err(), errors.New("upstream Run returned without a confirmed result")))
	}
	return cli.result, nil
}

type observedClient struct {
	client.Client
	mu        sync.Mutex
	finished  bool
	result    runnerv1.Result
	lastError error
}

func (c *observedClient) UpdateTask(ctx context.Context, req *connect.Request[runnerv1.UpdateTaskRequest]) (*connect.Response[runnerv1.UpdateTaskResponse], error) {
	resp, err := c.Client.UpdateTask(ctx, req)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.lastError = err
	if err == nil && req.Msg.State.GetStoppedAt() != nil && req.Msg.State.GetResult() != runnerv1.Result_RESULT_UNSPECIFIED {
		c.finished = true
		c.result = req.Msg.State.Result
	}
	return resp, err
}
