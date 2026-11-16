// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package scheduleripc

// Config is sent once, before Ready. Concrete slot and provider configurations
// are decoded only by the composition root in cmd/hrcs.
type Config struct {
	Capacity   int
	Ephemeral  bool
	Slot       string
	SlotConfig []byte
	Providers  map[string][]byte
}
