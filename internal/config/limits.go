// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package config

// GRPCMaxMessageSize bounds one serialized message in either direction on local
// daemon and monitor connections.
const GRPCMaxMessageSize = 64 << 20
