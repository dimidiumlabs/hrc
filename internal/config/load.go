// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"bytes"
	"encoding/json/v2"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Load selects the syntax by extension and applies the same JSON field and type
// rules to every format. Semantic validation belongs to the concrete config.
func Load(path string, target any) error {
	ext := strings.ToLower(filepath.Ext(path))
	if ext != ".json" && ext != ".yaml" && ext != ".yml" {
		return fmt.Errorf("unsupported config extension %q", ext)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read the config file: %w", err)
	}
	switch ext {
	case ".yaml", ".yml":
		data, err = yamlJSON(data)
	}
	if err == nil {
		err = json.Unmarshal(data, target, json.RejectUnknownMembers(true))
	}
	if err != nil {
		return fmt.Errorf("failed to parse config %q: %w", path, err)
	}
	return nil
}

func yamlJSON(data []byte) ([]byte, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("expected one YAML document")
	}
	return json.Marshal(value)
}
