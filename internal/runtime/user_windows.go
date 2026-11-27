// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import "errors"

func lookupUser(string) (*User, error) {
	return nil, errors.New("switching users is not yet supported on Windows")
}
func userEnvironment(*User) []string { return nil }
