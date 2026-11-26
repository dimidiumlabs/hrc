// SPDX-FileCopyrightText: 2026 Nikolay Govorov
// SPDX-License-Identifier: BSD-3-Clause

package runtime

import "os"

// UserSignal1 returns nil: Windows has no SIGUSR1 equivalent. SCM user-defined
// control codes are not OS signals and WinSW does not forward them to its child.
func UserSignal1() os.Signal { return nil }
