// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"errors"
	"fmt"
)

var (
	ErrNoDatabaseSelected  = errors.New("no database selected, you can use database in client initial options or WriteRequest/SqlQueryRequest")
	ErrPointEmptyTable     = errors.New("point's table is not set")
	ErrPointEmptyTimestamp = errors.New("point's timestamp is not set")
	ErrPointEmptyTags      = errors.New("point's tags should not be empty")
	ErrPointEmptyFields    = errors.New("point's fields should not be empty")
	ErrNullRows            = errors.New("null rows")
	ErrNullRouteTables     = errors.New("null route tables")
	ErrNullRequestTables   = errors.New("null request tables")
	ErrEmptyRoute          = errors.New("empty route")
	ErrOnlyArrowSupport    = errors.New("only arrow support now")
	ErrResponseHeaderMiss  = errors.New("response header miss")
)

const (
	codeSuccess      = 200
	codeInvalidRoute = 302
	codeShouldRetry  = 310
	codeInternal     = 500
	codeFlowControl  = 503
)

type Error struct {
	Code uint32
	Err  string
}

func (e *Error) Error() string {
	return fmt.Sprintf("ceresdb rpc failed, code:%d, err:%s", e.Code, e.Err)
}

// TODO: may retry in sdk while code is 302 or 310
func (e *Error) ShouldRetry() bool {
	return false
}

func (e *Error) ShouldClearRoute() bool {
	return e.Code == codeInvalidRoute
}
