// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

import (
	"errors"
	"fmt"
)

var (
	ErrPointEmptyTable     = errors.New("point table should not be empty")
	ErrPointEmptyTimestamp = errors.New("point timestamp should not be empty")
	ErrPointEmptyFields    = errors.New("point fields should not be empty")
	ErrNullRows            = errors.New("null rows")
	ErrNullRouteTables     = errors.New("null route tables")
	ErrNullRequestTables   = errors.New("null request tables")
	ErrEmptyRoute          = errors.New("empty route")
	ErrOnlyArrowSupport    = errors.New("only arrow support now")
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
