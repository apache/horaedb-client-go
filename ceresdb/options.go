// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

type Option interface {
	apply(*options)
}

type options struct {
	RpcMaxRecvMsgSize int
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
		RpcMaxRecvMsgSize: 1024 * 1024 * 1024,
	}
}

func WithRpcMaxRecvMsgSize(size int) Option {
	return newFuncOption(func(o *options) {
		o.RpcMaxRecvMsgSize = size
	})
}
