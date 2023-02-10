// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdb-client-go/utils"
	"github.com/CeresDB/ceresdbproto/golang/pkg/storagepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rpcClient struct {
	opts     options
	mutex    sync.Mutex // protect grpc conn init
	connPool sync.Map   // endpoint -> *grpc.ClientConn
}

func newRPCClient(opts options) *rpcClient {
	return &rpcClient{
		opts:     opts,
		connPool: sync.Map{},
	}
}

func (c *rpcClient) SQLQuery(ctx context.Context, endpoint string, req types.SQLQueryRequest) (types.SQLQueryResponse, error) {
	database, err := c.getDatabase(req.Database)
	if err != nil {
		return types.SQLQueryResponse{}, err
	}

	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return types.SQLQueryResponse{}, err
	}
	grpcClient := storagepb.NewStorageServiceClient(grpcConn)

	queryRequest := &storagepb.SqlQueryRequest{
		Context: &storagepb.RequestContext{
			Database: database,
		},
		Tables: req.Tables,
		Sql:    req.SQL,
	}
	queryResponse, err := grpcClient.SqlQuery(ctx, queryRequest)
	if err != nil {
		return types.SQLQueryResponse{}, err
	}
	if queryResponse.Header.Code != types.CodeSuccess {
		return types.SQLQueryResponse{}, &types.CeresdbError{
			Code: queryResponse.Header.Code,
			Err:  queryResponse.Header.Error,
		}
	}

	if affectedPayload, ok := queryResponse.Output.(*storagepb.SqlQueryResponse_AffectedRows); ok {
		return types.SQLQueryResponse{
			SQL:          req.SQL,
			AffectedRows: affectedPayload.AffectedRows,
		}, nil
	}

	rows, err := utils.ParseQueryResponse(queryResponse)
	if err != nil {
		return types.SQLQueryResponse{}, err
	}
	return types.SQLQueryResponse{
		SQL:          req.SQL,
		AffectedRows: queryResponse.GetAffectedRows(),
		Rows:         rows,
	}, nil
}

func (c *rpcClient) Write(ctx context.Context, endpoint, database string, points []types.Point) (types.WriteResponse, error) {
	database, err := c.getDatabase(database)
	if err != nil {
		return types.WriteResponse{}, err
	}

	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return types.WriteResponse{}, err
	}
	grpcClient := storagepb.NewStorageServiceClient(grpcConn)

	writeRequest, err := utils.BuildPbWriteRequest(points)
	if err != nil {
		return types.WriteResponse{}, err
	}
	writeRequest.Context = &storagepb.RequestContext{
		Database: database,
	}
	writeResponse, err := grpcClient.Write(ctx, writeRequest)
	if err != nil {
		return types.WriteResponse{}, err
	}
	if writeResponse.Header.Code != types.CodeSuccess {
		return types.WriteResponse{}, &types.CeresdbError{
			Code: writeResponse.Header.Code,
			Err:  writeResponse.Header.Error,
		}
	}
	return types.WriteResponse{
		Success: writeResponse.Success,
		Failed:  writeResponse.Failed,
	}, nil
}

func (c *rpcClient) Route(endpoint, database string, tables []string) (map[string]types.Route, error) {
	database, err := c.getDatabase(database)
	if err != nil {
		return nil, err
	}

	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return nil, err
	}
	grpcClient := storagepb.NewStorageServiceClient(grpcConn)

	routeRequest := &storagepb.RouteRequest{
		Context: &storagepb.RequestContext{
			Database: database,
		},
		Tables: tables,
	}
	routeResponse, err := grpcClient.Route(context.Background(), routeRequest)
	if err != nil {
		return nil, err
	}
	if routeResponse.Header.Code != types.CodeSuccess {
		return nil, &types.CeresdbError{
			Code: routeResponse.Header.Code,
			Err:  routeResponse.Header.Error,
		}
	}

	routes := make(map[string]types.Route, len(routeResponse.Routes))
	for _, r := range routeResponse.Routes {
		endpoint := fmt.Sprintf("%s:%d", r.Endpoint.Ip, r.Endpoint.Port)
		routes[r.Table] = types.Route{
			Table:    r.Table,
			Endpoint: endpoint,
			Ext:      r.Ext,
		}
	}
	return routes, nil
}

func (c *rpcClient) getGrpcConn(endpoint string) (*grpc.ClientConn, error) {
	if conn, ok := c.connPool.Load(endpoint); ok {
		return conn.(*grpc.ClientConn), nil
	}

	return c.newGrpcConn(endpoint)
}

func (c *rpcClient) newGrpcConn(endpoint string) (*grpc.ClientConn, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if conn, ok := c.connPool.Load(endpoint); ok {
		return conn.(*grpc.ClientConn), nil
	}

	conn, err := grpc.Dial(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.opts.RPCMaxRecvMsgSize)))
	if err != nil {
		return nil, err
	}
	c.connPool.Store(endpoint, conn)
	return conn, nil
}

func (c *rpcClient) getDatabase(database string) (string, error) {
	if database != "" {
		return database, nil
	}
	if c.opts.Database != "" {
		return c.opts.Database, nil
	}
	return "", types.ErrNoDatabaseSelected
}
