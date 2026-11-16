// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadFormats(t *testing.T) {
	type settings struct {
		ID      ID       `json:"id"`
		Count   uint64   `json:"count"`
		CPU     float64  `json:"cpu"`
		Memory  ByteSize `json:"memory"`
		Enabled bool     `json:"enabled"`
	}
	want := settings{ID: "build_01-x", Count: 9007199254740993, CPU: 0.5, Memory: 4 << 30, Enabled: true}
	for ext, input := range map[string]string{
		".json": `{"id":"build_01-x","count":9007199254740993,"cpu":0.5,"memory":"4GiB","enabled":true}`,
		".yaml": "id: build_01-x\ncount: 9007199254740993\ncpu: 0.5\nmemory: 4GiB\nenabled: true\n",
		".yml":  "{<<: {id: build_01-x, count: 9007199254740993}, cpu: 0.5, memory: 4GiB, enabled: true}",
	} {
		t.Run(ext, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config"+ext)
			if err := os.WriteFile(path, []byte(input), 0600); err != nil {
				t.Fatal(err)
			}
			var got settings
			if err := Load(path, &got); err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Fatalf("got %+v, want %+v", got, want)
			}
		})
	}
}

func TestLoadRejectsInvalid(t *testing.T) {
	for _, tt := range []struct{ ext, input string }{
		{".txt", `{}`},
		{".json", `{"memory":"8GB"}`},
		{".json", `{"memory":1024}`},
		{".yaml", "memory: 8GB"},
		{".yaml", "memory: 1024"},
		{".json", `{"id":"a","id":"b"}`},
		{".yaml", "id: a\nid: b\n"},
		{".json", `{"typo":1}`},
		{".yaml", "typo: 1"},
		{".yaml", "nested: {x: 1, x: 2}"},
		{".yaml", "42: a"},
		{".yaml", "id: 2026-01-01"},
		{".yaml", "id: a\n---\nid: b"},
		{".yaml", "nested: &self {x: *self}"},
		{".yaml", "number: .inf"},
		{".json", `{id:'a'}`},
		{".yaml", "id: 123"},
		{".yaml", "id: ../escape"},
	} {
		t.Run(tt.ext+tt.input, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config"+tt.ext)
			if err := os.WriteFile(path, []byte(tt.input), 0600); err != nil {
				t.Fatal(err)
			}
			var target struct {
				ID     ID             `json:"id"`
				Memory ByteSize       `json:"memory"`
				Number float64        `json:"number"`
				Nested map[string]any `json:"nested"`
			}
			if err := Load(path, &target); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestID(t *testing.T) {
	for _, value := range []string{"a", "Z", "build_01-x", "a0"} {
		if err := ID(value).Validate(); err != nil {
			t.Fatalf("%q: %v", value, err)
		}
	}
	for _, value := range []string{"", "0abc", "_abc", "-abc", "../a", "a/b", "a.b", "a b", "a\\b", "аbc", "aé", "a\n"} {
		var id ID = "valid"
		if err := id.UnmarshalText([]byte(value)); err == nil {
			t.Errorf("accepted %q", value)
		}
		if id != "valid" {
			t.Fatal("failed decode changed ID")
		}
	}
}

func TestLoadMissingFile(t *testing.T) {
	var target any
	if err := Load(filepath.Join(t.TempDir(), "missing.json"), &target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("expected missing file error, got %v", err)
	}
}
