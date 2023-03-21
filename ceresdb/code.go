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
)

const (
	codeSuccess      = 200
	codeInvalidRoute = 302
	codeShouldRetry  = 310
	codeFlowControl  = 503
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
	return e.Code == codeInvalidRoute
}

type CeresdbWriteError struct {
	SuccessTables [][]string
	SuccessOk     []WriteResponse
	FailedTables  [][]string
	Errors        []error
}

func (e *CeresdbWriteError) Error() string {
	errMsg := "ceresdb write failed, "
	for i := 0; i < len(e.FailedTables); i++ {
		errMsg += fmt.Sprintf("write %v failed with err [%v];", e.FailedTables[i], e.Errors[i])
	}
	return errMsg
}
