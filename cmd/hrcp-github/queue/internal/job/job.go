// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

// Package job defines the private payload shared by the GitHub queue and worker.
package job

import (
	"dimidiumlabs.io/hrc/internal/provider"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type RunnerSettings struct {
	AgentID            uint64 `json:"agentId"`
	AgentName          string `json:"agentName"`
	Ephemeral          bool   `json:"ephemeral"`
	UseV2Flow          bool   `json:"useV2Flow"`
	ServerURLV2        string `json:"serverUrlV2"`
	WorkFolder         string `json:"workFolder"`
	DisableUpdate      bool   `json:"disableUpdate"`
	SkipSessionRecover bool   `json:"skipSessionRecover"`
	PoolID             int    `json:"poolId"`
	PoolName           string `json:"poolName"`
	GitHubURL          string `json:"gitHubUrl"`
	ServerURL          string `json:"serverUrl"`
	HostedServer       *bool  `json:"IsHostedServer"`
	MonitorSocket      string `json:"monitorSocketAddress"`
}

type Job struct {
	Key      string `json:"ID"`
	Settings RunnerSettings
	Payload  json.RawMessage `json:"Job"`
}

func (j *Job) ID() string { return j.Key }
func (*Job) Type() string { return "github" }

func (j *Job) Marshal() ([]byte, error) {
	if err := j.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(j)
}

func (j *Job) Unmarshal(data []byte) error {
	var decoded Job
	if err := json.Unmarshal(data, &decoded); err != nil {
		return errors.Join(provider.ErrInvalidJob, err)
	}
	if err := decoded.Validate(); err != nil {
		return err
	}
	*j = decoded
	return nil
}

func (j *Job) Validate() error {
	if err := validateJobID(j.Key); err != nil {
		return err
	}
	if len(j.Payload) == 0 || !json.Valid(j.Payload) {
		return fmt.Errorf("%w: invalid GitHub job payload", provider.ErrInvalidJob)
	}
	return nil
}

func validateJobID(id string) error {
	if id == "" || id == "." || id == ".." || strings.ContainsAny(id, "/\\\\:") {
		return fmt.Errorf("%w: unsafe job ID: %q", provider.ErrInvalidJob, id)
	}
	return nil
}
