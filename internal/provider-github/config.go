// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package github

import (
	_ "embed"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"dimidiumlabs.io/hrc/internal/config"
	"dimidiumlabs.io/hrc/internal/provider"
	"github.com/actions/scaleset"
)

//go:embed runner-version.txt
var bundledRunnerVersion string

// GitHubApp authenticates as an installation of a GitHub App. Only the
// private key is a secret.
type GitHubApp struct {
	PrivateKey     config.Secret `json:"privateKey"`
	ClientID       string        `json:"clientId"`
	InstallationID int64         `json:"installationId"`
}

func (a *GitHubApp) Validate() error {
	if a.ClientID == "" {
		return fmt.Errorf("client ID is required")
	}
	if a.InstallationID <= 0 {
		return fmt.Errorf("installation ID is required")
	}
	if err := a.PrivateKey.Validate(); err != nil {
		return fmt.Errorf("invalid private key: %w", err)
	}
	return nil
}

type GitHubJWT struct {
	InstallationID int64         `json:"installationId"`
	Token          config.Secret `json:"token"`
}

func (a *GitHubJWT) Validate() error {
	if a.InstallationID <= 0 {
		return fmt.Errorf("installation ID is required")
	}
	if err := a.Token.Validate(); err != nil {
		return fmt.Errorf("invalid JWT token: %w", err)
	}
	return nil
}

type ForgeConfig struct {
	config.Provider

	RunnerVersion string   `json:"runnerVersion"`
	Url           string   `json:"url"`
	Group         string   `json:"group"`
	Labels        []string `json:"labels"`

	Token     config.Secret `json:"pat"`
	GitHubApp GitHubApp     `json:"app"`
	GitHubJWT GitHubJWT     `json:"jwt"`
}

func (c *ForgeConfig) defaults() {
	if c.Type == "" {
		c.Type = "github"
	}
	if c.Group == "" {
		c.Group = scaleset.DefaultRunnerGroup
	}
}

func (c *ForgeConfig) Validate() (err error) {
	defer func() {
		if err != nil {
			err = errors.Join(provider.ErrInvalidConfig, err)
		}
	}()
	c.defaults()
	if c.Type != "github" {
		return fmt.Errorf("unknown forge type %q", c.Type)
	}

	u, err := url.ParseRequestURI(c.Url)
	if err != nil {
		return fmt.Errorf("invalid registration URL: %w, it should be the full URL of where you want to register your scale set, e.g. 'https://github.com/org/repo'", err)
	}
	if u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return fmt.Errorf("invalid registration URL: expected a full HTTP(S) URL with a host")
	}

	for i, label := range c.Labels {
		if strings.TrimSpace(label) == "" {
			return fmt.Errorf("label at index %d is empty", i)
		}
	}

	pat := secretConfigured(&c.Token)
	app := c.GitHubApp.ClientID != "" || c.GitHubApp.InstallationID != 0 || secretConfigured(&c.GitHubApp.PrivateKey)
	jwt := c.GitHubJWT.InstallationID != 0 || secretConfigured(&c.GitHubJWT.Token)
	count := 0
	for _, present := range []bool{pat, app, jwt} {
		if present {
			count++
		}
	}
	if count == 0 {
		return fmt.Errorf("no credentials provided: supply exactly one of app, pat or jwt")
	}
	if count != 1 {
		return fmt.Errorf("supply exactly one of app, pat or jwt")
	}
	switch {
	case pat:
		if err := c.Token.Validate(); err != nil {
			return fmt.Errorf("invalid PAT: %w", err)
		}
	case app:
		return c.GitHubApp.Validate()
	case jwt:
		return c.GitHubJWT.Validate()
	}
	return nil
}

func secretConfigured(secret *config.Secret) bool {
	return secret.Src != "" || secret.Value != "" || len(secret.Command) != 0 || secret.TTL != "" || secret.Timeout != ""
}

// NewQueue binds this Forge's connection settings to the daemon identity.
func (c *ForgeConfig) NewQueue(id, version string) (provider.Queue, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	runnerVersion := c.RunnerVersion
	if runnerVersion == "" {
		runnerVersion = strings.TrimSpace(bundledRunnerVersion)
	}
	return New(Config{
		ID: id, Name: string(c.ID), Group: c.Group, Labels: c.Labels,
		Version: version, RunnerVersion: runnerVersion,
		NewClient: func() (*scaleset.Client, error) {
			return c.newClient(scaleset.SystemInfo{System: "hrc", Version: version})
		},
	})
}
