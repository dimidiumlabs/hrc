// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

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

// auth reads the private key, which is kept only as long as the client.
func (a *GitHubApp) auth() (scaleset.GitHubAppAuth, error) {
	key, err := a.PrivateKey.Take()
	if err != nil {
		return scaleset.GitHubAppAuth{}, fmt.Errorf("read private key: %w", err)
	}
	return scaleset.GitHubAppAuth{
		PrivateKey:     key,
		ClientID:       a.ClientID,
		InstallationID: a.InstallationID,
	}, nil
}

type ForgeConfig struct {
	config.Provider

	RunnerVersion string   `json:"runnerVersion"`
	Url           string   `json:"url"`
	Group         string   `json:"group"`
	Labels        []string `json:"labels"`

	Token     config.Secret `json:"pat"`
	GitHubApp GitHubApp     `json:"app"`
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

	tknError := c.Token.Validate()
	appError := c.GitHubApp.Validate()
	if tknError != nil && appError != nil {
		return fmt.Errorf("no credentials provided: either GitHub App (client id, installation id and private key) (recommended) or a Personal Access Token are required")
	}

	return nil
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
			info := scaleset.SystemInfo{System: "hrc", Version: version}
			if c.GitHubApp.Validate() == nil {
				auth, err := c.GitHubApp.auth()
				if err != nil {
					return nil, err
				}
				return scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
					GitHubConfigURL: c.Url, GitHubAppAuth: auth, SystemInfo: info,
				})
			}
			token, err := c.Token.Take()
			if err != nil {
				return nil, fmt.Errorf("read token: %w", err)
			}
			return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
				GitHubConfigURL: c.Url, PersonalAccessToken: token, SystemInfo: info,
			})
		},
	})
}
