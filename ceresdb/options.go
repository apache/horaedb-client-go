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

package ceresdb

import (
	"io"
	"os"
)

type Option interface {
	apply(*options)
}

type options struct {
	Database          string
	Logger            io.Writer
	LoggerDebug       bool
	RPCMaxRecvMsgSize int
	RouteMaxCacheSize int
}

type funcOption struct {
	f func(*options)
}

func (fdo *funcOption) apply(do *options) {
	fdo.f(do)
}

func newFuncOption(f func(*options)) *funcOption {
	return &funcOption{
		f: f,
	}
}

func defaultOptions() *options {
	return &options{
		Database:          "",
		Logger:            os.Stdout,
		LoggerDebug:       false,
		RPCMaxRecvMsgSize: 1024 * 1024 * 1024,
		RouteMaxCacheSize: 10 * 1000,
	}
}

func WithDefaultDatabase(database string) Option {
	return newFuncOption(func(o *options) {
		o.Database = database
	})
}

func WithLoggerWriter(writer io.Writer) Option {
	return newFuncOption(func(o *options) {
		o.Logger = writer
	})
}

func EnableLoggerDebug(enable bool) Option {
	return newFuncOption(func(o *options) {
		o.LoggerDebug = enable
	})
}

func WithRPCMaxRecvMsgSize(size int) Option {
	return newFuncOption(func(o *options) {
		o.RPCMaxRecvMsgSize = size
	})
}

func WithRouteMaxCacheSize(size int) Option {
	return newFuncOption(func(o *options) {
		o.RouteMaxCacheSize = size
	})
}
