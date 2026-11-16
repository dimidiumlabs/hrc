// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"encoding/json/v2"
	"errors"
	"testing"
)

type sumTypes struct {
	File  *fileVariant  `variant:"file"`
	Count *countVariant `variant:"count"`
}

type SumBase struct {
	Type string `json:"type"`
}

type fileVariant struct {
	SumBase
	Path string `json:"path"`
}

func (*fileVariant) Validate() error { return nil }

type countVariant struct {
	SumBase
	Count int `json:"count"`
}

func (c *countVariant) Validate() error {
	if c.Count <= 0 {
		return errors.New("count must be positive")
	}
	return nil
}

func TestSumSwitchesVariants(t *testing.T) {
	var value Sum[SumBase, sumTypes]
	if value.Base() != nil {
		t.Fatal("empty sum has a base")
	}
	if err := value.Validate(); err == nil {
		t.Fatal("accepted empty sum")
	}
	if err := json.Unmarshal([]byte(`{"type":"file","path":"work"}`), &value); err != nil {
		t.Fatal(err)
	}
	if file, ok := value.Value().(*fileVariant); !ok || file.Path != "work" {
		t.Fatal("wrong file variant")
	}

	if err := json.Unmarshal([]byte(`{"type":"count","count":0}`), &value); err != nil {
		t.Fatal(err)
	}
	if _, ok := value.Value().(*countVariant); !ok {
		t.Fatal("variant was not replaced")
	}
	if err := value.Validate(); err == nil {
		t.Fatal("variant validation was skipped")
	}
	before := value.Value()
	base := value.Base()
	if base != &before.(*countVariant).SumBase {
		t.Fatal("Base is not the active variant's embedded field")
	}
	base.Type = "changed"
	if before.(*countVariant).Type != "changed" {
		t.Fatal("Base mutation did not reach variant")
	}
	base.Type = "count"
	for _, data := range []string{
		`{"type":"missing"}`,
		`{"count":1}`,
		`{"type":"count","path":"wrong variant"}`,
		`{"type":"count","count":"wrong type"}`,
		`null`,
	} {
		if err := json.Unmarshal([]byte(data), &value); err == nil {
			t.Fatalf("accepted %s", data)
		}
		if value.Value() != before || value.Base() != base {
			t.Fatal("failed decode replaced the previous variant")
		}
	}
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"type":"count","count":0}` {
		t.Fatalf("unexpected variant encoding: %s", data)
	}
}

func TestSumInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name  string
		value interface{ UnmarshalJSON([]byte) error }
	}{
		{"missing base", &Sum[Resources, sumTypes]{}},
		{"not a struct", &Sum[SumBase, string]{}},
		{"empty", &Sum[SumBase, struct{}]{}},
		{"missing tag", &Sum[SumBase, struct{ File *fileVariant }]{}},
		{"not a pointer", &Sum[SumBase, struct {
			File fileVariant `variant:"file"`
		}]{}},
		{"missing validation", &Sum[SumBase, struct {
			File *struct{} `variant:"file"`
		}]{}},
		{"duplicate tag", &Sum[SumBase, struct {
			First  *fileVariant  `variant:"file"`
			Second *countVariant `variant:"file"`
		}]{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.value.UnmarshalJSON([]byte(`{"type":"file"}`)); err == nil {
				t.Fatal("accepted invalid variant definition")
			}
		})
	}
}
