// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ByteSize is a memory amount written as a JSON string: plain bytes or an
// integer with a KiB, MiB, GiB or TiB suffix, for example "8GiB".
type ByteSize uint64

var byteUnits = []struct {
	suffix string
	size   ByteSize
}{{"TiB", 1 << 40}, {"GiB", 1 << 30}, {"MiB", 1 << 20}, {"KiB", 1 << 10}}

func (b *ByteSize) UnmarshalText(text []byte) error {
	value, unit := string(text), ByteSize(1)
	for _, u := range byteUnits {
		if number, ok := strings.CutSuffix(value, u.suffix); ok {
			value, unit = number, u.size
			break
		}
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil || n > math.MaxUint64/uint64(unit) {
		return fmt.Errorf("invalid memory size %q: expected bytes or an integer with KiB, MiB, GiB or TiB", text)
	}
	*b = ByteSize(n) * unit
	return nil
}

// MarshalText preserves the string representation required by UnmarshalText.
func (b ByteSize) MarshalText() ([]byte, error) { return []byte(b.String()), nil }

func (b ByteSize) String() string {
	for _, u := range byteUnits {
		if b >= u.size && b%u.size == 0 {
			return strconv.FormatUint(uint64(b/u.size), 10) + u.suffix
		}
	}
	return strconv.FormatUint(uint64(b), 10)
}
