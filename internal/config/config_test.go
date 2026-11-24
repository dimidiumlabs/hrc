// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestSecretValidate(t *testing.T) {
	for _, tt := range []struct {
		secret *Secret
		want   string
	}{
		{&Secret{Src: "inline", Value: "token"}, ""},
		{&Secret{Src: "file", Value: "/token"}, ""},
		{&Secret{Src: "env", Value: "TOKEN"}, ""},
		{&Secret{Src: "unknown", Value: "token"}, "secret type"},
		{&Secret{Src: "inline", Value: ""}, "secret value"},
	} {
		t.Run(tt.secret.Src+"/"+tt.secret.Value, func(t *testing.T) {
			checkError(t, tt.secret.Validate(), tt.want)
		})
	}
}

func TestByteSizeString(t *testing.T) {
	for size, want := range map[ByteSize]string{0: "0", 1000: "1000", 1 << 10: "1KiB", 3 << 29: "1536MiB", 8 << 30: "8GiB", 2 << 40: "2TiB"} {
		if got := size.String(); got != want {
			t.Errorf("ByteSize(%d) = %q, want %q", uint64(size), got, want)
		}
	}
}

func TestProviderType(t *testing.T) {
	for _, tc := range []struct {
		value ProviderType
		name  string
	}{
		{ProviderGitHub, "github"},
		{ProviderForgejo, "forgejo"},
		{ProviderProcess, "process"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data, err := json.Marshal(tc.value)
			if err != nil {
				t.Fatal(err)
			}
			if string(data) != `"`+tc.name+`"` {
				t.Fatalf("JSON = %s", data)
			}
			var decoded ProviderType
			if err := json.Unmarshal(data, &decoded); err != nil {
				t.Fatal(err)
			}
			if decoded != tc.value {
				t.Fatalf("decoded = %v", decoded)
			}
		})
	}
	for _, data := range []string{`""`, `"other"`, `"GitHub"`, `1`, `true`, `{}`} {
		var decoded ProviderType
		if err := json.Unmarshal([]byte(data), &decoded); err == nil {
			t.Errorf("accepted invalid provider type %s", data)
		}
	}
	for _, value := range []ProviderType{ProviderUnknown, ProviderType(255)} {
		if _, err := json.Marshal(value); err == nil {
			t.Errorf("marshaled invalid provider type %v", value)
		}
	}
	value := ProviderForgejo
	if err := value.UnmarshalText([]byte("other")); err == nil || value != ProviderForgejo {
		t.Fatal("failed decoding must preserve the previous value")
	}
}

func TestSecretTake(t *testing.T) {
	dir := t.TempDir()
	key := filepath.Join(dir, "key.pem")
	if err := os.WriteFile(key, []byte("-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----\n"), 0600); err != nil {
		t.Fatal(err)
	}
	blank := filepath.Join(dir, "blank")
	if err := os.WriteFile(blank, []byte("\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("HRC_TEST_SECRET", " token\n")
	for _, tt := range []struct {
		secret    *Secret
		want, err string
	}{
		{secret: &Secret{Src: "inline", Value: "token"}, want: "token"},
		{secret: &Secret{Src: "env", Value: "HRC_TEST_SECRET"}, want: "token"},
		{secret: &Secret{Src: "env", Value: "HRC_TEST_UNSET_SECRET"}, err: "is empty"},
		{secret: &Secret{Src: "file", Value: key}, want: "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----"},
		{secret: &Secret{Src: "file", Value: blank}, err: "is empty"},
		{secret: &Secret{Src: "file", Value: filepath.Join(dir, "missing")}},
	} {
		t.Run(tt.secret.Src+"/"+filepath.Base(tt.secret.Value), func(t *testing.T) {
			got, err := tt.secret.Take(context.Background())
			if filepath.Base(tt.secret.Value) == "missing" {
				// The message differs between platforms; the error does not.
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("error = %v, want os.ErrNotExist", err)
				}
				return
			}
			checkError(t, err, tt.err)
			if got != tt.want {
				t.Errorf("Take = %q, want %q", got, tt.want)
			}
		})
	}
}
