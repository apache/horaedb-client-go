package ceresdb

import (
	"context"
	"errors"

	"github.com/CeresDB/ceresdbproto/go/ceresdbproto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	codeOk = 200
)

type Client struct {
	inner ceresdbproto.StorageServiceClient
	conn  *grpc.ClientConn
}

func NewClient(addr string) (*Client, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1024*1024*1024)))
	if err != nil {
		return nil, err
	}

	c := ceresdbproto.NewStorageServiceClient(conn)

	return &Client{
		inner: c,
		conn:  conn,
	}, nil
}

func (c *Client) Write(ctx context.Context, points []Point) (int, error) {
	req := writeRequest{
		points: points,
	}

	resp, err := c.inner.Write(ctx, req.toPb())
	if err != nil {
		return 0, err
	}

	if codeOk == resp.Header.Code {
		return int(resp.Success), nil
	}

	return 0, errors.New(resp.Header.GetError())
}

func (c *Client) Close() error {
	return c.conn.Close()
}

func (c *Client) Query(ctx context.Context, sql string) (string, error) {
	req := ceresdbproto.QueryRequest{
		Metrics: []string{""},
		Ql:      sql,
	}
	resp, err := c.inner.Query(ctx, &req)
	if err != nil {
		return "", err
	}

	return resp.SchemaContent, nil
}
