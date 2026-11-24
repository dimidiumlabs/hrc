// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

//go:build darwin

package runtime

type pgroupPlatform struct{}

func (g *Group) configure() error { return nil }

func (g *Group) release() error { return nil }
