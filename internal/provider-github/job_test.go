// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package github

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"dimidiumlabs.io/hrc/internal/provider"
	"dimidiumlabs.io/hrc/internal/provider-github/listener"
)

func TestRunPayloadDoesNotCarryListenerCredentials(t *testing.T) {
	h := &handle{
		registration: &runnerRegistration{
			settings:    runnerSettings{AgentID: 1, AgentName: "runner"},
			credentials: listener.Credentials{Scheme: "listener-secret"},
		},
		raw: &listener.RunJob{JobID: "job", Raw: json.RawMessage(`{"jobId":"job","future":{"x":42},"token":"job-token"}`)},
	}
	j := h.Job()
	data, err := j.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("listener-secret")) || !bytes.Contains(data, []byte("job-token")) {
		t.Fatal("wrong execution credentials in payload")
	}
	var decoded job
	if err := decoded.Unmarshal(data); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.Payload, h.raw.Raw) {
		t.Fatal("provider discarded unknown job fields")
	}
	again, err := j.Marshal()
	if err != nil || !bytes.Equal(data, again) {
		t.Fatal("serialization changed Job state")
	}
	roundtrip, err := decoded.Marshal()
	if err != nil || !bytes.Equal(data, roundtrip) || decoded.ID() != j.ID() {
		t.Fatal("Job round trip changed its description")
	}
	if err := decoded.Unmarshal([]byte(`{"ID":"../bad","Job":{}}`)); !errors.Is(err, provider.ErrInvalidJob) || decoded.ID() != j.ID() {
		t.Fatal("invalid decode mutated Job")
	}
}

func TestRejectsUnsafeJIT(t *testing.T) {
	for _, name := range []string{"../escape", "bin/hrcw-github", ".env"} {
		files, _ := json.Marshal(map[string]string{name: base64.StdEncoding.EncodeToString([]byte("x"))})
		if _, err := parseRunnerRegistration(base64.StdEncoding.EncodeToString(files)); err == nil {
			t.Errorf("accepted %q", name)
		}
	}
}

func TestRunRejectsUnsafeDiagnosticPaths(t *testing.T) {
	for _, id := range []string{"", ".", "..", "../escape", "..\\escape", "C:escape", "/absolute"} {
		t.Run(id, func(t *testing.T) {
			root := t.TempDir()
			j := &job{Key: id, Payload: json.RawMessage(`{}`)}
			if _, err := j.Marshal(); !errors.Is(err, provider.ErrInvalidJob) {
				t.Fatal("unsafe job ID accepted")
			}
			if entries, err := os.ReadDir(root); err != nil || len(entries) != 0 {
				t.Fatalf("rejected job created diagnostics: %v, %v", entries, err)
			}
		})
	}
}
