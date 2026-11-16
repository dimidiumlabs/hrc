// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"fmt"
	"os"
	"strings"
)

type Secret struct {
	Src   string `json:"type"`
	Value string `json:"value"`
}

func (c *Secret) Validate() error {
	if c.Src != "file" && c.Src != "env" && c.Src != "inline" {
		return fmt.Errorf("secret type must one of: 'inline', 'file', 'env'")
	}

	if c.Value == "" {
		return fmt.Errorf("secret value could not be empty")
	}

	return nil
}

// Take returns the secret without surrounding whitespace, such as the newline
// that ends a secret file. An empty secret is an error.
func (c *Secret) Take() (string, error) {
	var value string

	switch c.Src {
	case "env":
		value = os.Getenv(c.Value)

	case "file":
		data, err := os.ReadFile(c.Value)
		if err != nil {
			return "", err
		}
		value = string(data)

	case "inline":
		value = c.Value

	default:
		return "", fmt.Errorf("unknown secret type")
	}

	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("%s secret %q is empty", c.Src, c.Value)
	}

	return value, nil
}
