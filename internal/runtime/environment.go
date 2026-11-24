// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import (
	"errors"
	"os"
	"runtime"
	"strings"
)

func environment(file string, overrides []string) ([]string, error) {
	env := make(map[string]string)
	key := func(k string) string {
		if runtime.GOOS == "windows" {
			return strings.ToUpper(k)
		}
		return k
	}
	put := func(entry string) {
		if k, _, ok := strings.Cut(entry, "="); ok && k != "" {
			env[key(k)] = entry
		}
	}
	for _, entry := range os.Environ() {
		put(entry)
	}
	data, err := os.ReadFile(file)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	for _, entry := range strings.Split(string(data), "\n") {
		put(strings.TrimSuffix(entry, "\r"))
	}
	for _, entry := range overrides {
		if k, _, ok := strings.Cut(entry, "="); !ok || k == "" || strings.ContainsRune(entry, 0) {
			return nil, errors.New("invalid environment entry")
		}
	}
	for _, entry := range overrides {
		put(entry)
	}
	result := make([]string, 0, len(env))
	for _, entry := range env {
		result = append(result, entry)
	}
	return result, nil
}
