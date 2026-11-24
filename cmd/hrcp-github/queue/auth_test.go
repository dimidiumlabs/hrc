// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json/v2"
	"encoding/pem"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dimidiumlabs.io/hrc/internal/config"
	"github.com/actions/scaleset"
	"github.com/golang-jwt/jwt/v5"
)

func testAppJWT(t *testing.T, key *rsa.PrivateKey, expires time.Time) string {
	t.Helper()
	value, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Issuer: "client", IssuedAt: jwt.NewNumericDate(time.Now().Add(-time.Minute)), ExpiresAt: jwt.NewNumericDate(expires),
	}).SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func testRSAKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

func TestSecretJWTProvider(t *testing.T) {
	key := testRSAKey(t)
	valid := testAppJWT(t, key, time.Now().Add(5*time.Minute))
	for _, tt := range []struct{ name, value, want string }{
		{"valid", valid, ""},
		{"malformed", "SENSITIVE_JWT", "invalid GitHub App JWT"},
		{"expired", testAppJWT(t, key, time.Now().Add(-time.Minute)), "expires in less"},
		{"near expiry", testAppJWT(t, key, time.Now().Add(10*time.Second)), "expires in less"},
		{"missing claims", "eyJhbGciOiJSUzI1NiJ9.e30.c2ln", "invalid GitHub App JWT"},
		{"unsigned", "eyJhbGciOiJub25lIn0.e30.", "invalid GitHub App JWT"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			secret := &config.Secret{Src: "inline", Value: tt.value, TTL: "1h"}
			provider := secretJWTProvider{secret: secret}
			value, err := provider.Token(context.Background())
			checkError(t, err, tt.want)
			if err != nil && (value != "" || strings.Contains(err.Error(), tt.value)) {
				t.Fatal("JWT leaked in error")
			}
			if err == nil && value != valid {
				t.Fatal("JWT changed")
			}
		})
	}
	t.Run("expiry overrides ttl", func(t *testing.T) {
		t.Setenv("HRC_JWT_TEST", valid)
		secret := &config.Secret{Src: "env", Value: "HRC_JWT_TEST", TTL: "1h"}
		if _, err := secret.Take(context.Background()); err != nil {
			t.Fatal(err)
		}
		provider := secretJWTProvider{secret: secret}
		if value, err := provider.Token(context.Background()); err != nil || value != valid {
			t.Fatal("valid cached JWT rejected")
		}
		secret.Invalidate()
		t.Setenv("HRC_JWT_TEST", testAppJWT(t, key, time.Now().Add(10*time.Second)))
		if _, err := secret.Take(context.Background()); err != nil {
			t.Fatal(err)
		}
		t.Setenv("HRC_JWT_TEST", valid)
		if value, err := provider.Token(context.Background()); err != nil || value != valid {
			t.Fatalf("JWT refresh failed: %v", err)
		}
	})
}

func TestForgeAuthenticationSelection(t *testing.T) {
	for _, combination := range []struct{ pat, app, jwt, incomplete bool }{
		{pat: true, app: true},
		{pat: true, jwt: true},
		{app: true, jwt: true},
		{pat: true, app: true, incomplete: true},
	} {
		cfg := &ForgeConfig{Url: "https://github.com/org"}
		if combination.pat {
			cfg.Token = config.Secret{Src: "inline", Value: "token"}
		}
		if combination.app {
			cfg.GitHubApp.ClientID = "client"
			if !combination.incomplete {
				cfg.GitHubApp.InstallationID = 1
				cfg.GitHubApp.PrivateKey = config.Secret{Src: "inline", Value: "token"}
			}
		}
		if combination.jwt {
			cfg.GitHubJWT.InstallationID = 1
			cfg.GitHubJWT.Token = config.Secret{Src: "inline", Value: "token"}
		}
		checkError(t, cfg.Validate(), "exactly one")
	}
	cfg := &ForgeConfig{Url: "https://github.com/org", GitHubJWT: GitHubJWT{InstallationID: 1, Token: config.Secret{Src: "inline", Value: "token"}}}
	checkError(t, cfg.Validate(), "")
	cfg.GitHubJWT.InstallationID = 0
	checkError(t, cfg.Validate(), "installation ID")
}

func TestLoadConfigJWT(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hrc.yaml")
	data := "url: https://github.com/org\njwt:\n  installationId: 42\n  token:\n    type: subcommand\n    command: [/usr/local/bin/jwt-helper, --ci]\n    ttl: 5m\n    timeout: 10s\n"
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	var cfg ForgeConfig
	if err := config.Load(path, &cfg); err != nil {
		t.Fatal(err)
	}
	checkError(t, cfg.Validate(), "")
	if cfg.GitHubJWT.Token.TTL != "5m" || len(cfg.GitHubJWT.Token.Command) != 2 {
		t.Fatal("JWT source not loaded")
	}
}

func TestScalesetCredentialRefresh(t *testing.T) {
	key := testRSAKey(t)
	otherKey := testRSAKey(t)
	for _, mode := range []string{"jwt", "app", "pat", "cached admin"} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "secret")
			write := func(value string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(value), 0600); err != nil {
					t.Fatal(err)
				}
			}
			firstJWT := testAppJWT(t, key, time.Now().Add(5*time.Minute))
			secondJWT := testAppJWT(t, otherKey, time.Now().Add(5*time.Minute))
			pemKey := func(key *rsa.PrivateKey) string {
				return string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
			}
			first, second := firstJWT, secondJWT
			if mode == "app" {
				first, second = pemKey(key), pemKey(otherKey)
			}
			if mode == "pat" {
				first, second = "first-pat", "second-pat"
			}
			write(first)
			var exchanges, registrations, adminCalls atomic.Int32
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/api/v3/app/installations/42/access_tokens":
					n := exchanges.Add(1)
					wantKey := &key.PublicKey
					if n > 1 {
						wantKey = &otherKey.PublicKey
					}
					value := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
					if _, err := jwt.Parse(value, func(*jwt.Token) (any, error) { return wantKey, nil }, jwt.WithValidMethods([]string{"RS256"})); err != nil {
						t.Errorf("App JWT not refreshed: %v", err)
					}
					w.WriteHeader(http.StatusCreated)
					fmt.Fprint(w, `{"token":"installation-token","expires_at":"2099-01-01T00:00:00Z"}`)
				case "/api/v3/orgs/org/actions/runners/registration-token":
					n := registrations.Add(1)
					want := "Bearer installation-token"
					if mode == "pat" {
						want = "Bearer first-pat"
						if n > 1 {
							want = "Bearer second-pat"
						}
					}
					if r.Header.Get("Authorization") != want {
						t.Error("wrong registration credential")
					}
					w.WriteHeader(http.StatusCreated)
					fmt.Fprint(w, `{"token":"registration-token"}`)
				case "/api/v3/actions/runner-registration":
					n := adminCalls.Add(1)
					if r.Header.Get("Authorization") != "RemoteAuth registration-token" {
						t.Error("registration token overwritten")
					}
					expires := time.Now().Add(10 * time.Second)
					if mode == "cached admin" {
						expires = time.Now().Add(time.Hour)
					}
					claims, _ := json.Marshal(map[string]int64{"exp": expires.Unix()})
					admin := "eyJhbGciOiJSUzI1NiJ9." + base64.RawURLEncoding.EncodeToString(claims) + ".c2ln"
					if err := json.MarshalWrite(w, map[string]string{"url": server.URL, "token": admin}); err != nil {
						t.Error(err)
					}
					if n == 1 {
						write(second)
					}
				case "/_apis/runtime/runnerscalesets/7":
					if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer eyJhbGciOiJSUzI1NiJ9.") {
						t.Error("admin token overwritten")
					}
					fmt.Fprint(w, `{"id":7,"name":"test"}`)
				default:
					t.Errorf("unexpected path %s", r.URL.Path)
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			cfg := ForgeConfig{Url: server.URL + "/org"}
			switch mode {
			case "app":
				cfg.GitHubApp = GitHubApp{ClientID: "client", InstallationID: 42, PrivateKey: config.Secret{Src: "file", Value: path}}
			case "pat":
				cfg.Token = config.Secret{Src: "file", Value: path}
			default:
				cfg.GitHubJWT = GitHubJWT{InstallationID: 42, Token: config.Secret{Src: "file", Value: path}}
			}
			client, err := cfg.newClient(scaleset.SystemInfo{})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			for range 2 {
				if _, err := client.GetRunnerScaleSetByID(ctx, 7); err != nil {
					t.Fatal(err)
				}
			}
			want := int32(2)
			if mode == "cached admin" {
				want = 1
			}
			if adminCalls.Load() != want || registrations.Load() != want {
				t.Fatalf("refresh calls = %d/%d, want %d", adminCalls.Load(), registrations.Load(), want)
			}
			if mode != "pat" && exchanges.Load() != want {
				t.Fatalf("JWT exchanges = %d, want %d", exchanges.Load(), want)
			}
		})
	}
}
