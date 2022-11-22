// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package utils

import (
	"time"
)

func CurrentMS() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
