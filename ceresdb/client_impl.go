// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package ceresdb

import (
	"context"
	"errors"
	"fmt"

	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdb-client-go/utils"
	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	codeOk = 200
)

type clientImpl struct {
	inner ceresdbproto.StorageServiceClient
	conn  *grpc.ClientConn
}

func newClient(endpoint string, opts *options) (CeresDBClient, error) {
	conn, err := grpc.Dial(endpoint,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(opts.RpcMaxRecvMsgSize)))
	if err != nil {
		return nil, err
	}

	c := ceresdbproto.NewStorageServiceClient(conn)

	return &clientImpl{
		inner: c,
		conn:  conn,
	}, nil
}

// TODO
// https://github.com/CeresDB/ceresdb-client-go/issues/1
// next pr support
func (c *clientImpl) Query(ctx context.Context, req types.QueryRequest) (types.QueryResponse, error) {
	return types.QueryResponse{}, errors.New("not implemented")
}

func (c *clientImpl) Write(ctx context.Context, rows []*types.Row) (types.WriteResponse, error) {
	writeRequest, err := utils.BuildRowsToPb(rows)
	if err != nil {
		return types.WriteResponse{}, err
	}

	writeResponse, err := c.inner.Write(ctx, writeRequest)
	if err != nil {
		return types.WriteResponse{}, err
	}

	if writeResponse.Header.Code != codeOk {
		return types.WriteResponse{}, fmt.Errorf("write failed, code: %d, err: %s",
			writeResponse.Header.Code, writeResponse.Header.Error)
	}
	return types.WriteResponse{
		Success: writeResponse.Success,
		Failed:  writeResponse.Failed,
	}, nil
}

func (c *clientImpl) Close() error {
	return c.conn.Close()
}
