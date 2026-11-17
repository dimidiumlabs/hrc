// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package github

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/provider"
	"github.com/actions/scaleset"
)

func queueFixture(t *testing.T, mode string) (*testRunner, *atomic.Int32, *atomic.Int32, *atomic.Int32, context.CancelFunc) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	jobID := fmt.Sprintf("job-%d", time.Now().UnixNano())
	var polls, renews, completes, deletes, tokens, sessions, acquires atomic.Int32
	started := make(chan struct{})
	var startedOnce sync.Once
	renewed := make(chan struct{})
	var renewedOnce sync.Once
	var server *httptest.Server
	var encoded string
	names := make(chan string, 4)
	jitRequested := make(chan struct{})
	var jitOnce sync.Once
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		// The fixture's scale-set API supplies the same acquired job as Broker.
		switch r.URL.Path {
		case "/api/v3/orgs/org/actions/runners/registration-token":
			w.WriteHeader(http.StatusCreated)
			io.WriteString(w, `{"token":"registration"}`)
			return
		case "/api/v3/actions/runner-registration":
			claims, _ := json.Marshal(map[string]int64{"exp": time.Now().Add(time.Hour).Unix()})
			token := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims) + ".c2ln"
			json.NewEncoder(w).Encode(map[string]string{"url": server.URL + "/", "token": token})
			return
		case "/_apis/runtime/runnerscalesets":
			io.WriteString(w, `{"count":1,"value":[{"id":7,"name":"smoke","runnerGroupId":1,"labels":[{"name":"smoke"}]}]}`)
			return
		case "/_apis/runtime/runnerscalesets/7/sessions":
			json.NewEncoder(w).Encode(map[string]any{
				"sessionId":       "00000000-0000-0000-0000-000000000007",
				"messageQueueUrl": server.URL + "/queue", "messageQueueAccessToken": "queue-token",
				"statistics": map[string]int{"totalAssignedJobs": 1},
			})
			return
		case "/_apis/runtime/runnerscalesets/7/sessions/00000000-0000-0000-0000-000000000007":
			w.WriteHeader(http.StatusNoContent)
			return
		case "/queue":
			<-r.Context().Done()
			return
		case "/_apis/runtime/runnerscalesets/7/generatejitconfig":
			jitOnce.Do(func() { close(jitRequested) })
			if mode == "jiterror" {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			var settings map[string]any
			json.NewDecoder(r.Body).Decode(&settings)
			name := fmt.Sprint(settings["name"])
			if settings["workFolder"] != "." || !strings.HasPrefix(name, "host-runner-") {
				t.Errorf("bad JIT settings: %v", settings)
			}
			names <- name
			json.NewEncoder(w).Encode(map[string]string{"encodedJITConfig": encoded})
			return
		}
		if r.URL.Path == "/oauth" {
			if err := r.ParseForm(); err != nil {
				t.Error(err)
			}
			assertion := strings.Split(r.Form.Get("client_assertion"), ".")
			if len(assertion) != 3 {
				t.Error("missing assertion")
				w.WriteHeader(400)
				return
			}
			signature, _ := base64.RawURLEncoding.DecodeString(assertion[2])
			hash := sha256.Sum256([]byte(assertion[0] + "." + assertion[1]))
			if rsa.VerifyPKCS1v15(&key.PublicKey, crypto.SHA256, hash[:], signature) != nil {
				t.Error("bad RSA signature")
			}
			var claims map[string]any
			data, _ := base64.RawURLEncoding.DecodeString(assertion[1])
			json.Unmarshal(data, &claims)
			if claims["aud"] != server.URL+"/oauth" || claims["iss"] != "client" || r.Form.Get("grant_type") != "client_credentials" {
				t.Error("bad OAuth claims/form")
			}
			fmt.Fprintf(w, `{"access_token":"runner-token-%d","expires_in":3600}`, tokens.Add(1))
			return
		}
		expected := "Bearer runner-token-1"
		if mode == "refresh" {
			expected = "Bearer runner-token-2"
		}
		if strings.HasPrefix(r.URL.Path, "/job/") {
			expected = "Bearer job-token"
		}
		if mode == "refresh" && r.URL.Path == "/broker/session" && r.Method == "POST" && r.Header.Get("Authorization") == "Bearer runner-token-1" {
			w.WriteHeader(401)
			return
		}
		if r.Header.Get("Authorization") != expected {
			t.Errorf("%s wrong auth: %q", r.URL.Path, r.Header.Get("Authorization"))
			w.WriteHeader(403)
			return
		}
		switch r.URL.Path {
		case "/broker/session":
			if r.Method == "DELETE" {
				deletes.Add(1)
				w.WriteHeader(204)
				return
			}
			var data map[string]any
			json.NewDecoder(r.Body).Decode(&data)
			if data["ownerName"] == nil || data["brokerSession"] != true || data["sessionName"] != nil {
				t.Error("bad session payload")
			}
			n := sessions.Add(1)
			fmt.Fprintf(w, `{"sessionId":"session-%d"}`, n)
		case "/broker/message":
			if r.URL.Query().Get("disableUpdate") != "true" {
				t.Error("updates enabled")
			}
			if r.URL.Query().Get("status") == "Online" {
				body, _ := json.Marshal(map[string]any{"runner_request_id": "request", "run_service_url": server.URL + "/run/", "should_acknowledge": true, "billing_owner_id": "owner"})
				json.NewEncoder(w).Encode(map[string]any{"messageType": "RunnerJobRequest", "body": string(body)})
				return
			}
			n := polls.Add(1)
			if mode == "recover" && n == 1 {
				w.WriteHeader(400)
				io.WriteString(w, `{"source":"actions-broker-listener","errorKind":"RunnerSessionInvalid"}`)
				return
			}
			if mode == "recover" && r.URL.Query().Get("sessionId") != "session-2" {
				t.Error("session not recovered")
			}
			if mode == "cancel" || mode == "kill" || mode == "recover" {
				select {
				case <-started:
				case <-r.Context().Done():
					return
				}
				body := fmt.Sprintf(`{"jobId":%q,"timeout":"00:00:16"}`, jobID)
				if mode == "kill" {
					// Exercise the caller's short default grace when the server
					// supplies no timeout, without waiting 45 seconds in tests.
					body = fmt.Sprintf(`{"jobId":%q}`, jobID)
				}
				json.NewEncoder(w).Encode(map[string]any{"messageType": "JobCancellation", "body": body})
				return
			}
			<-r.Context().Done()
		case "/broker/acknowledge":
			var body map[string]string
			json.NewDecoder(r.Body).Decode(&body)
			if body["runnerRequestId"] != "request" {
				t.Error("bad ack")
			}
			w.WriteHeader(204)
		case "/run/acquirejob":
			n := acquires.Add(1)
			if mode == "retry" && n == 1 {
				w.WriteHeader(503)
				return
			}
			var request map[string]string
			json.NewDecoder(r.Body).Decode(&request)
			if request["runnerOS"] != testRunnerOS() || request["billingOwnerId"] != "owner" {
				t.Error("bad acquire payload")
			}
			json.NewEncoder(w).Encode(map[string]any{"jobId": jobID, "messageType": "RunnerJobRequest", "plan": map[string]string{"planId": "plan"}, "unknownFutureField": "Привет 🌍", "testEndpoint": server.URL + "/job", "resources": map[string]any{"endpoints": []any{map[string]any{"name": "SystemVssConnection", "url": server.URL + "/job/", "authorization": map[string]any{"parameters": map[string]string{"AccessToken": "job-token"}}}}}})
		case "/job/renewjob":
			n := renews.Add(1)
			if mode == "lease" && n > 1 {
				w.WriteHeader(404)
				return
			}
			// Leave scheduling headroom on loaded CI hosts, especially Windows.
			json.NewEncoder(w).Encode(map[string]any{"lockedUntil": time.Now().Add(4 * time.Second)})
			if n > 1 {
				renewedOnce.Do(func() { close(renewed) })
			}
		case "/job/renewed":
			select {
			case <-renewed:
				w.WriteHeader(204)
			case <-r.Context().Done():
			}
		case "/job/started":
			startedOnce.Do(func() { close(started) })
			w.WriteHeader(204)
		case "/job/completejob":
			completes.Add(1)
			var body map[string]any
			json.NewDecoder(r.Body).Decode(&body)
			expected := provider.Failed
			if mode == "failure" || mode == "crash" || mode == "startfailure" {
				expected = provider.Failed
			}
			if mode == "cancel" || mode == "shutdown" || mode == "lease" || mode == "kill" || mode == "recover" {
				expected = provider.Canceled
			}
			if mode == "undelivered" {
				// Cancellation can interrupt acquisition or the subsequent send.
				expected = provider.Canceled
				if body["conclusion"] == provider.Failed.String() {
					expected = provider.Failed
				}
			}
			if body["conclusion"] != expected.String() {
				t.Errorf("wrong completion: %#v", body)
			}
			if mode == "crash" || mode == "kill" {
				if body["planId"] != "plan" || body["jobId"] != jobID || body["billingOwnerId"] != "owner" {
					t.Error("bad fallback completion")
				}
			}
			w.WriteHeader(204)
		default:
			t.Errorf("unexpected endpoint %s", r.URL.Path)
			w.WriteHeader(404)
		}
	}))
	t.Cleanup(server.Close)
	files := map[string]string{}
	encode := func(name string, object any) {
		data, _ := json.Marshal(object)
		files[name] = base64.StdEncoding.EncodeToString(data)
	}
	// GitHub's JIT endpoint serializes .runner values as strings, including
	// numbers and booleans, with the same property names as .NET RunnerSettings.
	encode(".runner", map[string]any{"AgentId": "1", "AgentName": "test", "Ephemeral": "True", "UseV2Flow": "True", "ServerUrlV2": server.URL + "/broker/", "WorkFolder": "_work", "PoolId": "1", "PoolName": nil, "DisableUpdate": "True"})
	encode(".credentials", map[string]any{"Scheme": "OAuth", "Data": map[string]string{"ClientId": "client", "AuthorizationUrl": server.URL + "/oauth"}})
	encode(".credentials_rsaparams", map[string][]byte{"modulus": key.N.Bytes(), "exponent": big.NewInt(int64(key.E)).Bytes(), "d": key.D.Bytes(), "p": key.Primes[0].Bytes(), "q": key.Primes[1].Bytes()})
	data, _ := json.Marshal(files)
	encoded = base64.StdEncoding.EncodeToString(data)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	runner := &testRunner{
		jit: encoded, ctx: ctx, started: started, markStarted: func() { startedOnce.Do(func() { close(started) }) }, renewed: renewed, names: names, jitRequested: jitRequested,
		config: Config{
			ID: "host", Name: "smoke", Version: "test", RunnerVersion: "2.333.0",
			RetryMin: 10 * time.Millisecond, RetryMax: 50 * time.Millisecond,
			NewClient: func() (*scaleset.Client, error) {
				return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
					GitHubConfigURL: server.URL + "/org", PersonalAccessToken: "pat",
				})
			},
		},
	}
	return runner, &completes, &deletes, &renews, cancel
}

type testRunner struct {
	config       Config
	jit          string
	markStarted  func()
	renewed      <-chan struct{}
	ctx          context.Context
	started      <-chan struct{}
	names        <-chan string
	jitRequested <-chan struct{}
}

func TestAcquisitionDoesNotPrepareExecution(t *testing.T) {
	fixture, completed, deleted, renewed, _ := queueFixture(t, "startfailure")
	// A real provider can acquire and complete jobs without a Worker installation.
	queue, err := New(fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "not-created")
	demand := newQueueDemand()
	ctx, cancel := context.WithCancel(fixture.ctx)
	defer cancel()
	called := false
	for job := range queue.Serve(ctx, demand) {
		called = true
		if job.Job().ID() == "" || renewed.Load() < 1 {
			t.Error("queue delivered a job before acquisition and initial renewal")
		}
		if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("acquisition prepared a slot: %v", err)
		}
		err = job.Finish(ctx, provider.Failed, nil)
		err = errors.Join(err, job.Close(ctx))
		cancel()
	}
	if err != nil || !called || completed.Load() != 1 || deleted.Load() != 1 {
		t.Fatalf("acquisition: called=%v complete=%d delete=%d err=%v", called, completed.Load(), deleted.Load(), err)
	}
	if available, _ := demand.State(); available != 1 {
		t.Fatalf("acquisition leaked capacity: %d", available)
	}
}

func TestQueueStopPreservesDeliveredHandle(t *testing.T) {
	fixture, completed, deleted, _, _ := queueFixture(t, "startfailure")
	queue, err := New(fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	demand := newQueueDemand()
	defer demand.Close()
	ctx, cancel := context.WithCancel(fixture.ctx)
	defer cancel()
	jobs := queue.Serve(ctx, demand)
	var job provider.Handle
	select {
	case job = <-jobs:
		if job == nil {
			t.Fatal("queue stopped before delivery")
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	defer job.Close(context.Background())
	cancel()
	for extra := range jobs {
		extra.Close(context.Background())
		t.Error("unexpected extra Job")
	}
	if free, _ := demand.State(); free != 0 || deleted.Load() != 0 {
		t.Fatal("queue stop released a delivered Handle")
	}
	cleanup, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	if err := job.Finish(cleanup, provider.Failed, nil); err != nil {
		t.Fatal(err)
	}
	if free, _ := demand.State(); free != 0 {
		t.Fatal("Finish released reservation")
	}
	if err := job.Close(cleanup); err != nil {
		t.Fatal(err)
	}
	if err := job.Close(cleanup); err != nil {
		t.Fatal(err)
	}
	if free, _ := demand.State(); free != 1 || completed.Load() != 1 || deleted.Load() != 1 {
		t.Fatalf("cleanup: free=%d complete=%d delete=%d", free, completed.Load(), deleted.Load())
	}
	if err := job.Finish(cleanup, provider.Failed, nil); !errors.Is(err, provider.ErrClosed) {
		t.Fatalf("Finish after Close: %v", err)
	}
}

func TestQueueStopCleansUndeliveredHandle(t *testing.T) {
	fixture, completed, deleted, renewed, _ := queueFixture(t, "undelivered")
	queue, err := New(fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	demand := newQueueDemand()
	defer demand.Close()
	ctx, cancel := context.WithCancel(fixture.ctx)
	defer cancel()
	jobs := queue.Serve(ctx, demand)
	deadline := time.Now().Add(5 * time.Second)
	for renewed.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("Job was not acquired")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	// Do not receive until the producer has disposed of the undelivered Job.
	for deleted.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("undelivered Handle was not closed")
		}
		time.Sleep(time.Millisecond)
	}
	if job, ok := <-jobs; ok {
		job.Close(context.Background())
		t.Fatal("delivered a Job after cleanup")
	}
	if free, _ := demand.State(); free != 1 || completed.Load() != 1 {
		t.Fatalf("cleanup: free=%d complete=%d", free, completed.Load())
	}
}

func TestAcquisitionFailureDoesNotPrepareExecution(t *testing.T) {
	fixture, _, _, _, _ := queueFixture(t, "jiterror")
	queue, err := New(fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(t.TempDir(), "not-created")
	demand := newQueueDemand()
	ctx, cancel := context.WithCancel(fixture.ctx)
	defer cancel()
	go func() {
		select {
		case <-fixture.jitRequested:
			cancel()
		case <-ctx.Done():
		}
	}()
	for job := range queue.Serve(ctx, demand) {
		t.Error("delivered a job after failed registration")
		job.Close(context.Background())
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("registration prepared a workspace: %v", err)
	}
	if available, _ := demand.State(); available != 1 {
		t.Fatalf("registration leaked capacity: %d", available)
	}
}

func TestQueueReopeningPreservesDeliveredHandle(t *testing.T) {
	runner, completed, _, _, _ := queueFixture(t, "success")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var sessions, sessionDeletes, polls, jits atomic.Int32
	failSession := make(chan struct{})
	var capacities sync.Map // Advertised X-ScaleSetMaxCapacity values.
	var api *httptest.Server
	api = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.URL.Path == "/api/v3/orgs/org/actions/runners/registration-token":
			w.WriteHeader(http.StatusCreated)
			io.WriteString(w, `{"token":"registration"}`)
		case r.URL.Path == "/api/v3/actions/runner-registration":
			claims, _ := json.Marshal(map[string]int64{"exp": time.Now().Add(time.Hour).Unix()})
			token := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"HS256"}`)) + "." + base64.RawURLEncoding.EncodeToString(claims) + ".c2ln"
			json.NewEncoder(w).Encode(map[string]string{"url": api.URL + "/", "token": token})
		case r.URL.Path == "/_apis/runtime/runnerscalesets":
			io.WriteString(w, `{"count":1,"value":[{"id":42,"name":"smoke","runnerGroupId":1,"labels":[{"type":"System","name":"smoke"}]}]}`)
		case r.URL.Path == "/_apis/runtime/runnerscalesets/42/sessions" && r.Method == http.MethodPost:
			n := sessions.Add(1)
			if n == 1 {
				// A previous controller instance still owns the session.
				w.WriteHeader(http.StatusConflict)
				io.WriteString(w, `{"typeName":"GitHub.DistributedTask.WebApi.TaskAgentSessionConflictException","message":"conflict"}`)
				return
			}
			json.NewEncoder(w).Encode(map[string]any{
				"sessionId":               fmt.Sprintf("00000000-0000-0000-0000-00000000000%d", n),
				"messageQueueUrl":         api.URL + "/queue",
				"messageQueueAccessToken": "queue-token",
				"statistics":              map[string]int{"totalAssignedJobs": 1},
			})
		case strings.HasPrefix(r.URL.Path, "/_apis/runtime/runnerscalesets/42/sessions/") && r.Method == http.MethodDelete:
			sessionDeletes.Add(1)
			w.WriteHeader(http.StatusNoContent)
		case r.URL.Path == "/queue":
			capacities.Store(r.Header.Get(scaleset.HeaderScaleSetMaxCapacity), true)
			if polls.Add(1) == 1 {
				// Fail only after the Handle has transferred to the caller.
				select {
				case <-failSession:
				case <-r.Context().Done():
					return
				}
				w.WriteHeader(http.StatusBadRequest)
				io.WriteString(w, `{"typeName":"QueueFailure"}`)
				return
			}
			<-r.Context().Done()
		case r.URL.Path == "/_apis/runtime/runnerscalesets/42/generatejitconfig":
			jits.Add(1)
			json.NewEncoder(w).Encode(map[string]string{"encodedJITConfig": runner.jit})
		default:
			t.Errorf("unexpected endpoint: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer api.Close()

	// The poll blocked during the job must be restarted when its slot is
	// released; otherwise the third poll arrives only after ctx expires.
	go func() {
		for ctx.Err() == nil && (completed.Load() == 0 || polls.Load() < 3) {
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
	}()
	demand := newQueueDemand()
	configuration := runner.config
	configuration.NewClient = func() (*scaleset.Client, error) {
		return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
			GitHubConfigURL: api.URL + "/org", PersonalAccessToken: "pat",
		})
	}
	queue, err := New(configuration)
	if err != nil {
		t.Fatal(err)
	}
	// Session failure closes the stream. Reopening must preserve the
	// count and connections of jobs delivered by an earlier invocation.
	if job, ok := <-queue.Serve(ctx, demand); ok {
		job.Close(ctx)
		t.Fatal("initial conflicting session delivered a job")
	}
	if ctx.Err() != nil {
		t.Fatal("failed idle queue did not close promptly")
	}
	jobs := queue.Serve(ctx, demand)
	job, ok := <-jobs
	if !ok {
		t.Fatal("queue closed before delivering its job")
	}
	close(failSession)
	if extra, ok := <-jobs; ok {
		extra.Close(ctx)
		t.Fatal("failed session delivered an extra job")
	}
	// There must be no provider-owned retry, even with an active Handle.
	if sessions.Load() != 2 {
		t.Fatalf("queue retried its own session: %d", sessions.Load())
	}
	resumed := queue.Serve(ctx, demand)
	for sessions.Load() < 3 && ctx.Err() == nil {
		time.Sleep(time.Millisecond)
	}
	if err := job.Finish(ctx, provider.Failed, nil); err != nil {
		t.Fatal(err)
	}
	if err := job.Close(ctx); err != nil {
		t.Fatal(err)
	}
	for extra := range resumed {
		extra.Close(ctx)
		t.Error("reopened session acquired a duplicate job")
	}
	if completed.Load() != 1 {
		t.Errorf("job interrupted by session failure: complete=%d", completed.Load())
	}
	if jits.Load() != 1 {
		t.Errorf("runner started %d times for one assigned job", jits.Load())
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Error("slot release did not restart the long poll")
	}
	capacities.Range(func(value, _ any) bool {
		if value != "1" {
			t.Errorf("advertised capacity %q beyond the host's single slot", value)
		}
		return true
	})
	if sessions.Load() != 3 || sessionDeletes.Load() < 2 {
		t.Errorf("sessions created=%d deleted=%d; want 3 created, failed session deleted", sessions.Load(), sessionDeletes.Load())
	}
}

func testRunnerOS() string {
	switch runtime.GOOS {
	case "windows":
		return "Windows"
	case "darwin":
		return "macOS"
	default:
		return "Linux"
	}
}

// This integration fixture supplies the execution side without a scheduler
// process. cmd/hrcs tests exercise that process and IPC independently.

func TestHandleLifecycleWithoutExecution(t *testing.T) {
	for _, mode := range []string{"success", "failure", "crash", "cancel", "lease", "refresh", "retry", "recover"} {
		t.Run(mode, func(t *testing.T) {
			fixture, completed, deleted, renewed, _ := queueFixture(t, mode)
			queue, err := New(fixture.config)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(fixture.ctx)
			defer cancel()
			demand := newQueueDemand()
			jobs := queue.Serve(ctx, demand)
			var handle provider.Handle
			select {
			case handle = <-jobs:
				if handle == nil {
					t.Fatal("queue stopped without a job")
				}
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			defer handle.Close(context.Background())
			fixture.markStarted()
			result := provider.Failed
			switch mode {
			case "success":
				select {
				case <-fixture.renewed:
				case <-ctx.Done():
					t.Fatal("missing periodic renewal")
				}
			case "cancel", "recover", "lease":
				select {
				case event := <-handle.Events():
					want := provider.Cancellation
					if mode == "lease" {
						want = provider.LeaseLost
					}
					if event.Type != want {
						t.Fatalf("event=%v, want %v: %v", event.Type, want, event.Err)
					}
					if mode == "lease" && !errors.Is(event.Err, provider.ErrLeaseLost) {
						t.Fatal("lease error lost")
					}
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				result = provider.Canceled
			}
			if err := handle.Finish(ctx, result, nil); err != nil {
				t.Fatal(err)
			}
			if err := handle.Close(ctx); err != nil {
				t.Fatal(err)
			}
			cancel()
			for extra := range jobs {
				extra.Close(context.Background())
				t.Error("unexpected job")
			}
			if completed.Load() != 1 || deleted.Load() != 1 || renewed.Load() < 1 {
				t.Fatalf("complete=%d delete=%d renew=%d", completed.Load(), deleted.Load(), renewed.Load())
			}
		})
	}
}

// One reservation is enough for the protocol fixture. Scheduling is tested in hrcd.
type queueDemand struct {
	mu       sync.Mutex
	reserved bool
	changed  chan struct{}
}

func newQueueDemand() *queueDemand { return &queueDemand{changed: make(chan struct{})} }
func (d *queueDemand) notify()     { close(d.changed); d.changed = make(chan struct{}) }
func (d *queueDemand) Reserve() provider.Reservation {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.reserved {
		panic("fixture received overlapping reservations")
	}
	d.reserved = true
	d.notify()
	return &queueReservation{d: d}
}
func (d *queueDemand) State() (int, <-chan struct{}) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.reserved {
		return 0, d.changed
	}
	return 1, d.changed
}
func (*queueDemand) Close() error { return nil }

type queueReservation struct {
	d    *queueDemand
	once sync.Once
}

func (*queueReservation) Wait(ctx context.Context) error { return ctx.Err() }
func (r *queueReservation) Close() error {
	r.once.Do(func() { r.d.mu.Lock(); defer r.d.mu.Unlock(); r.d.reserved = false; r.d.notify() })
	return nil
}
