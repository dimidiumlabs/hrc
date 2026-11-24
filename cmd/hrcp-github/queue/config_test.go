// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"encoding/json/v2"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"dimidiumlabs.io/hrc/internal/config"
)

func checkError(t *testing.T, err error, want string) {
	t.Helper()
	if want == "" {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	} else if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("error = %v, want containing %q", err, want)
	}
}

func TestForgeValidate(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*ForgeConfig)
		want   string
	}{
		{"PAT", func(g *ForgeConfig) {}, ""},
		{"missing type", func(g *ForgeConfig) { g.Type = config.ProviderUnknown }, "unknown forge type"},
		{"wrong type", func(g *ForgeConfig) { g.Type = config.ProviderForgejo }, "unknown forge type"},
		{"app", func(g *ForgeConfig) {
			g.Token = config.Secret{}
			g.GitHubApp.ClientID = "client"
			g.GitHubApp.InstallationID = 1
			g.GitHubApp.PrivateKey = config.Secret{Src: "inline", Value: "key"}
		}, ""},
		{"URL", func(g *ForgeConfig) { g.Url = "://invalid" }, "invalid registration URL"},
		{"relative URL", func(g *ForgeConfig) { g.Url = "/org/repo" }, "invalid registration URL"},
		{"missing host", func(g *ForgeConfig) { g.Url = "https:/org/repo" }, "invalid registration URL"},
		{"unsupported scheme", func(g *ForgeConfig) { g.Url = "ftp://github.com/org/repo" }, "invalid registration URL"},
		{"HTTP URL", func(g *ForgeConfig) { g.Url = "http://github.example/org/repo" }, ""},
		{"empty label", func(g *ForgeConfig) { g.Labels = []string{"linux", ""} }, "label at index 1 is empty"},
		{"blank label", func(g *ForgeConfig) { g.Labels = []string{" \t"} }, "label at index 0 is empty"},
		{"credentials", func(g *ForgeConfig) { g.Token = config.Secret{} }, "no credentials provided"},
		{"app without key", func(g *ForgeConfig) {
			g.Token = config.Secret{}
			g.GitHubApp = GitHubApp{ClientID: "client", InstallationID: 1}
		}, "invalid private key"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			g := ForgeConfig{Provider: config.Provider{Type: config.ProviderGitHub}, Url: "https://github.com/org/repo", Token: config.Secret{Src: "inline", Value: "token"}}
			tt.change(&g)
			checkError(t, g.Validate(), tt.want)
			if g.Group != "default" {
				t.Fatalf("default group = %q, want default", g.Group)
			}
		})
	}
	t.Run("preserves group and labels", func(t *testing.T) {
		g := ForgeConfig{Provider: config.Provider{Type: config.ProviderGitHub}, Url: "https://github.com/org", Group: "custom", Labels: []string{"linux"}, Token: config.Secret{Src: "env", Value: "TOKEN"}}
		checkError(t, g.Validate(), "")
		if g.Group != "custom" || len(g.Labels) != 1 || g.Labels[0] != "linux" {
			t.Fatalf("unexpected group: %+v", &g)
		}
	})
}

func TestLoadConfigGitHubApp(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "app.pem")
	if err := os.WriteFile(key, []byte("pem\n"), 0600); err != nil {
		t.Fatal(err)
	}

	var cfg ForgeConfig
	content := `{"type":"github","url":"https://github.com/org","app":{"clientId":"Iv1.client","installationId":42,"privateKey":{"type":"file","value":` + strconv.Quote(key) + `}}}`
	if err := json.Unmarshal([]byte(content), &cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	keyValue, err := cfg.GitHubApp.PrivateKey.Take(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GitHubApp.ClientID != "Iv1.client" || cfg.GitHubApp.InstallationID != 42 || keyValue != "pem" {
		t.Error("unexpected GitHub App credentials")
	}
}

func TestNewQueueWithoutWorkerInstallation(t *testing.T) {
	t.Chdir(t.TempDir())
	t.Setenv("PATH", "")
	for _, version := range []string{"", "2.300.0"} {
		cfg := ForgeConfig{Provider: config.Provider{Type: config.ProviderGitHub}, Url: "https://github.com/org/repo", Token: config.Secret{Src: "inline", Value: "token"}, RunnerVersion: version}
		queue, err := cfg.NewQueue("instance", "test")
		if err != nil {
			t.Fatal(err)
		}
		want := version
		if want == "" {
			want = strings.TrimSpace(bundledRunnerVersion)
		}
		if want == "" || queue.(*Queue).config.RunnerVersion != want {
			t.Fatalf("runner version = %q, want %q", queue.(*Queue).config.RunnerVersion, want)
		}
	}
}
