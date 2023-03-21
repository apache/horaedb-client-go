// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"
	"fmt"
)

type clientImpl struct {
	rpcClient   *rpcClient
	routeClient routeClient
}

func newClient(endpoint string, routeMode RouteMode, opts options) (Client, error) {
	rpcClient := newRPCClient(opts)
	routeClient, err := newRouteClient(endpoint, routeMode, rpcClient, opts)
	if err != nil {
		return nil, err
	}
	return &clientImpl{
		rpcClient:   rpcClient,
		routeClient: routeClient,
	}, nil
}

func (c *clientImpl) SQLQuery(ctx context.Context, req SQLQueryRequest) (SQLQueryResponse, error) {
	if err := c.withDefaultRequestContext(&req.ReqCtx); err != nil {
		return SQLQueryResponse{}, err
	}

	if len(req.Tables) == 0 {
		return SQLQueryResponse{}, ErrNullRequestTables
	}

	routes, err := c.routeClient.RouteFor(req.ReqCtx, req.Tables)
	if err != nil {
		return SQLQueryResponse{}, fmt.Errorf("route tables failed, tables:%v, err:%v", req.Tables, err)
	}
	for _, route := range routes {
		queryResponse, err := c.rpcClient.SQLQuery(ctx, route.Endpoint, req)
		if ceresdbErr, ok := err.(*CeresdbError); ok && ceresdbErr.ShouldClearRoute() {
			c.routeClient.ClearRouteFor(req.Tables)
		}
		return queryResponse, err
	}
	return SQLQueryResponse{}, ErrEmptyRoute
}

func (c *clientImpl) Write(ctx context.Context, req WriteRequest) (WriteResponse, error) {
	if err := c.withDefaultRequestContext(&req.ReqCtx); err != nil {
		return WriteResponse{}, err
	}

	if len(req.Points) == 0 {
		return WriteResponse{}, ErrNullRows
	}

	tables := getTablesFromPoints(req.Points)

	routes, err := c.routeClient.RouteFor(req.ReqCtx, tables)
	if err != nil {
		return WriteResponse{}, err
	}

	pointsByRoute, err := splitPointsByRoute(req.Points, routes)
	if err != nil {
		return WriteResponse{}, err
	}

	// TODO
	// Convert to parallel write
	combinedResponse := WriteResponse{}
	combinedError := CeresdbWriteError{
		SuccessTables: make([][]string, 0, len(pointsByRoute)),
		SuccessOk:     make([]WriteResponse, 0, len(pointsByRoute)),
		FailedTables:  make([][]string, 0, len(pointsByRoute)),
		Errors:        make([]error, 0, len(pointsByRoute)),
	}
	for endpoint, partPoints := range pointsByRoute {
		// TODO
		// Get part tables from splitPointsByRoute
		partTables := getTablesFromPoints(partPoints)

		response, err := c.rpcClient.Write(ctx, endpoint, req.ReqCtx, partPoints)
		if err != nil {
			if ceresdbErr, ok := err.(*CeresdbError); ok && ceresdbErr.ShouldClearRoute() {
				c.routeClient.ClearRouteFor(partTables)
			}
			combinedError = combineWriteError(combinedError,
				CeresdbWriteError{FailedTables: [][]string{partTables}, Errors: []error{err}})
			continue
		}

		combinedResponse = combineWriteResponse(combinedResponse, response)
		combinedError = combineWriteError(combinedError,
			CeresdbWriteError{SuccessTables: [][]string{partTables}, SuccessOk: []WriteResponse{response}})
	}

	if len(combinedError.FailedTables) != 0 {
		return WriteResponse{}, &combinedError
	}
	return combinedResponse, nil
}

func (c *clientImpl) withDefaultRequestContext(reqCtx *RequestContext) error {
	// use default
	if reqCtx.Database == "" {
		reqCtx.Database = c.rpcClient.opts.Database
	}

	// check Request Context
	if reqCtx.Database == "" {
		return ErrNoDatabaseSelected
	}
	return nil
}
