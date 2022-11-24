// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"io"
	"os"
)

type Option interface {
	apply(*options)
}

type options struct {
	Logger            io.Writer
	LoggerDebug       bool
	RpcMaxRecvMsgSize int
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
		Logger:            os.Stdout,
		LoggerDebug:       false,
		RpcMaxRecvMsgSize: 1024 * 1024 * 1024,
		RouteMaxCacheSize: 10 * 1000,
	}
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

func WithRpcMaxRecvMsgSize(size int) Option {
	return newFuncOption(func(o *options) {
		o.RpcMaxRecvMsgSize = size
	})
}

func WithRouteMaxCacheSize(size int) Option {
	return newFuncOption(func(o *options) {
		o.RouteMaxCacheSize = size
	})
}
