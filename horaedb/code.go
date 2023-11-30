/*
 * Copyright 2022 The HoraeDB Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package horaedb

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
	return fmt.Sprintf("HoraeDB RPC failed, code:%d, err:%s", e.Code, e.Err)
}

// TODO: may retry in sdk while code is 302 or 310
func (e *Error) ShouldRetry() bool {
	return false
}

func (e *Error) ShouldClearRoute() bool {
	return e.Code == codeInvalidRoute
}
