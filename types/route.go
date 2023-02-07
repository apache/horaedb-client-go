// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package types

type RouteMode string

const (
	Direct RouteMode = "direct"
	Proxy  RouteMode = "proxy"
)

type Route struct {
	Table    string
	Endpoint string
	Ext      []byte
}
