// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package main

import (
	"encoding/json/v2"
	"os"
	"path/filepath"
	"reflect"
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

const forgeJSON = `{"type":"github","url":"https://github.com/org","pat":{"type":"inline","value":"token"}}`
const poolConfigJSON = `{"instanceId":"host","spaces":[{"id":"build","type":"host","dir":"work"}],"slots":[{"id":"workers","space":"build","type":"pgroup","quota":{"cpu":2,"memory":"8GiB"},"ephemeral":true}],"forges":[{"id":"forge","slot":"workers","type":"github","url":"https://github.com/org","pat":{"type":"inline","value":"token"}}]}`

func TestConfigValidate(t *testing.T) {
	cfg := Config{}
	checkError(t, cfg.Validate(), "instanceId: id is required")
	cfg.InstanceID = "host"
	checkError(t, cfg.Validate(), "")
	if cfg.Directories != (Directories{Runtime: "./runner", Spaces: "./hrc/build", Logs: "./hrc/log"}) {
		t.Fatal("invalid defaults")
	}
	cfg.Directories.Spaces = "/builds"
	if err := json.Unmarshal([]byte(poolConfigJSON), &cfg); err != nil {
		t.Fatal(err)
	}
	checkError(t, cfg.Validate(), "")
	if cfg.Directories.Spaces != "/builds" {
		t.Fatal("custom directory changed")
	}
	// Several forges share one pool and its budget.
	var second ForgeConfig
	if err := json.Unmarshal([]byte(`{"id":"second","slot":"workers",`+forgeJSON[1:]), &second); err != nil {
		t.Fatal(err)
	}
	cfg.Forges = append(cfg.Forges, second)
	checkError(t, cfg.Validate(), "")
	cfg.Forges[1].Base().ID = "forge"
	checkError(t, cfg.Validate(), "duplicate forge id")
	cfg.Forges[1].Base().ID = "second"
	cfg.Forges[1].Base().Slot = "missing"
	checkError(t, cfg.Validate(), "unknown slot pool")
	cfg.Forges[1].Base().Slot = "workers"
	cfg.Slots[0].Base().Space = "missing"
	checkError(t, cfg.Validate(), "unknown space")
}

func TestConfigVariants(t *testing.T) {
	yaml := `instanceId: host
spaces:
  - id: build
    type: host
    dir: work
slots:
  - id: workers
    space: build
    type: pgroup
    quota: {cpu: 2, memory: 8GiB}
    ephemeral: true
forges:
  - id: forge
    slot: workers
    type: github
    url: https://github.com/org
    pat: {type: inline, value: token}
`
	for _, tt := range []struct{ name, ext, data, want string }{
		{"json", ".json", poolConfigJSON, ""},
		{"yaml", ".yaml", yaml, ""},
		{"unknown forge", ".json", strings.Replace(poolConfigJSON, `"type":"github"`, `"type":"other"`, 1), "unknown configuration type"},
		{"unknown space", ".json", strings.Replace(poolConfigJSON, `"type":"host"`, `"type":"other"`, 1), "unknown configuration type"},
		{"unknown slot", ".json", strings.Replace(poolConfigJSON, `"type":"pgroup"`, `"type":"other"`, 1), "unknown configuration type"},
		{"unknown slot field", ".json", strings.Replace(poolConfigJSON, `"type":"pgroup"`, `"type":"pgroup","image":"x"`, 1), "unknown"},
		{"opaque forge field", ".json", strings.Replace(poolConfigJSON, `"type":"github"`, `"type":"github","future_field":"x"`, 1), ""},
		{"default forge type", ".json", strings.Replace(poolConfigJSON, `"type":"github",`, ``, 1), ""},
		{"null forge", ".json", strings.Replace(poolConfigJSON, `"forges":[`, `"forges":[null,`, 1), "object"},
		{"no public provider registry", ".json", strings.Replace(poolConfigJSON, `"instanceId"`, `"providers":{},"instanceId"`, 1), "unknown"},
		{"old nested slot", ".json", strings.Replace(poolConfigJSON, `"type":"host"`, `"type":"host","slot":{}`, 1), "unknown"},
		{"bad quota", ".json", strings.Replace(poolConfigJSON, `"cpu":2`, `"cpu":-1`, 1), "cpu"},
		{"bad pool id", ".json", strings.Replace(poolConfigJSON, `"id":"workers"`, `"id":"../bad"`, 1), "id"},
		{"unknown pool", ".json", strings.Replace(poolConfigJSON, `"slot":"workers"`, `"slot":"missing"`, 1), "unknown slot pool"},
		{"unknown placement", ".json", strings.Replace(poolConfigJSON, `"space":"build"`, `"space":"missing"`, 1), "unknown space"},
		{"duplicate space", ".json", strings.Replace(poolConfigJSON, `"spaces":[`, `"spaces":[{"id":"build"},`, 1), "duplicate space"},
		{"duplicate pool", ".json", strings.Replace(poolConfigJSON, `"slots":[`, `"slots":[{"id":"workers","space":"build"},`, 1), "duplicate slot pool"},
		{"two pools", ".json", strings.Replace(poolConfigJSON, `"slots":[`, `"slots":[{"id":"other","space":"build"},`, 1), "multiple slot pools"},
		{"null pool", ".json", strings.Replace(poolConfigJSON, `"slots":[`, `"slots":[null,`, 1), "object"},
		{"duplicate discriminator", ".json", strings.Replace(poolConfigJSON, `"type":"pgroup"`, `"type":"pgroup","type":"pgroup"`, 1), "duplicate"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config"+tt.ext)
			if err := os.WriteFile(path, []byte(tt.data), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := LoadConfig(path)
			checkError(t, err, tt.want)
			if tt.want != "" {
				return
			}
			if cfg.Forges[0].Base().Slot != "workers" || cfg.Slots[0].Base().Space != "build" || cfg.Slots[0].Base().Quota.Memory != 8<<30 || !cfg.Slots[0].Base().Ephemeral {
				t.Fatal("lost pool settings or placement")
			}
			data, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			var restored Config
			if err := json.Unmarshal(data, &restored); err != nil {
				t.Fatal(err)
			}
			checkError(t, restored.Validate(), "")
			again, err := json.Marshal(&restored)
			if err != nil {
				t.Fatal(err)
			}
			var before, after any
			if err := json.Unmarshal(data, &before); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(again, &after); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(before, after) {
				t.Fatal("round trip lost configuration")
			}
		})
	}
}
