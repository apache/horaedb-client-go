// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"
	"fmt"
	"sync"

	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdb-client-go/utils"
	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type rpcClient struct {
	opts     options
	mutex    sync.Mutex // protect grpc conn init
	connPool sync.Map   // endpoint -> *grpc.ClientConn
}

func newRpcClient(opts options) *rpcClient {
	return &rpcClient{
		opts:     opts,
		connPool: sync.Map{},
	}
}

func (c *rpcClient) Query(endpoint string, ctx context.Context, req types.QueryRequest) (types.QueryResponse, error) {
	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return types.QueryResponse{}, err
	}
	grpcClient := ceresdbproto.NewStorageServiceClient(grpcConn)

	queryRequest := &ceresdbproto.QueryRequest{
		Metrics: req.Metrics,
		Ql:      req.Ql,
	}
	queryResponse, err := grpcClient.Query(ctx, queryRequest)
	if err != nil {
		return types.QueryResponse{}, err
	}
	if queryResponse.Header.Code != types.CodeSuccess {
		return types.QueryResponse{}, &types.CeresdbError{
			Code: queryResponse.Header.Code,
			Err:  queryResponse.Header.Error,
		}
	}

	rows, err := utils.ParseQueryResponse(queryResponse)
	if err != nil {
		return types.QueryResponse{}, err
	}
	return types.QueryResponse{
		Ql:       req.Ql,
		RowCount: uint32(len(rows)),
		Rows:     rows,
	}, nil
}

func (c *rpcClient) Write(endpoint string, ctx context.Context, rows []*types.Row) (types.WriteResponse, error) {
	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return types.WriteResponse{}, err
	}
	grpcClient := ceresdbproto.NewStorageServiceClient(grpcConn)

	writeRequest, err := utils.BuildPbWriteRequest(rows)
	if err != nil {
		return types.WriteResponse{}, err
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

func (c *rpcClient) Route(endpoint string, metrics []string) (map[string]types.Route, error) {
	grpcConn, err := c.getGrpcConn(endpoint)
	if err != nil {
		return nil, err
	}
	grpcClient := ceresdbproto.NewStorageServiceClient(grpcConn)

	routeRequest := &ceresdbproto.RouteRequest{
		Metrics: metrics,
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
		routes[r.Metric] = types.Route{
			Metric:   r.Metric,
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
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(c.opts.RpcMaxRecvMsgSize)))
	if err != nil {
		return nil, err
	}
	c.connPool.Store(endpoint, conn)
	return conn, nil
}
