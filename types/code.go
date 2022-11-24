// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

import (
	"errors"
	"fmt"
)

var (
	ErrBuiltBuilder        = errors.New("Builder has been built, use new one or reset")
	ErrRowEmptyMetric      = errors.New("Builder has been built, use new one or reset")
	ErrRowEmptyTimestamp   = errors.New("Timestamp shoud not be empty")
	ErrRowEmptyFields      = errors.New("Fields should not be empty")
	ErrRowInvalidFieldType = errors.New("Filed invalid type")
	ErrNullRows            = errors.New("Null rows")
	ErrNullRouteMetrics    = errors.New("Null route metrics")
	ErrNullRequestMetrics  = errors.New("Null request metrics")
	ErrEmptyRoute          = errors.New("Empty route")
)

const (
	CodeSuccess      = 200
	CodeInvalidRoute = 302
	CodeShouldRetry  = 310
	CodeFlowControl  = 503
)

type CeresdbError struct {
	Code uint32
	Err  string
}

func (e *CeresdbError) Error() string {
	return fmt.Sprintf("ceresdb rpc failed, code:%d, err:%s", e.Code, e.Err)
}

// TODO: may retry in sdk while code is 302 or 310
func (e *CeresdbError) ShouldRetry() bool {
	return false
}

func (e *CeresdbError) ShouldClearRoute() bool {
	return e.Code == CodeInvalidRoute
}
