// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import "fmt"

// ID is an ASCII identifier suitable for configuration references and paths.
type ID string

func (id ID) Validate() error {
	for i, ch := range id {
		if ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' {
			continue
		}
		if i > 0 && (ch >= '0' && ch <= '9' || ch == '_' || ch == '-') {
			continue
		}
		return fmt.Errorf("invalid id %q: expected [A-Za-z][A-Za-z0-9_-]*", id)
	}
	if id == "" {
		return fmt.Errorf("id is required")
	}
	return nil
}

func (id *ID) UnmarshalText(text []byte) error {
	value := ID(text)
	if err := value.Validate(); err != nil {
		return err
	}
	*id = value
	return nil
}
