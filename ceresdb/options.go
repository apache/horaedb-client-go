

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
