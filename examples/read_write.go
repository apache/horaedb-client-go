// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_ADDR"); v != "" {
		endpoint = v
	}
}

func existsTable(client ceresdb.Client) error {
	req := ceresdb.SQLQueryRequest{
		Tables: []string{"demo"},
		SQL:    "EXISTS TABLE demo",
	}
	resp, err := client.SQLQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("exists table fail, err: %v\n", err)
		return err
	}
	fmt.Printf("exists table success, resp: %+v\n", resp)
	return nil
}

func createTable(client ceresdb.Client) error {
	createTableSQL := `CREATE TABLE IF NOT EXISTS demo (
	name string TAG,
	value double,
	t timestamp NOT NULL,
	TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl=false)`

	req := ceresdb.SQLQueryRequest{
		Tables: []string{"demo"},
		SQL:    createTableSQL,
	}
	resp, err := client.SQLQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("create table fail, err: %v\n", err)
		return err
	}
	fmt.Printf("create table success, resp: %+v\n", resp)
	return nil
}

func dropTable(client ceresdb.Client) error {
	dropTableSQL := `DROP TABLE demo`
	req := ceresdb.SQLQueryRequest{
		Tables: []string{"demo"},
		SQL:    dropTableSQL,
	}
	resp, err := client.SQLQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("drop table fail, err: %v\n", err)
		return err
	}
	fmt.Printf("drop table success, resp: %+v\n", resp)
	return nil
}

func writeTable(client ceresdb.Client, count int) error {
	nowInMs := time.Now().UnixNano() / int64(time.Millisecond)
	points := make([]ceresdb.Point, 0, count)
	for i := 0; i < count; i++ {
		point, err := ceresdb.NewPointBuilder("demo").
			SetTimestamp(nowInMs).
			AddTag("name", ceresdb.NewStringValue("test_tag"+strconv.Itoa(i))).
			AddField("value", ceresdb.NewDoubleValue(0.4242+float64(i))).
			Build()
		if err != nil {
			return err
		}
		points = append(points, point)
	}
	req := ceresdb.WriteRequest{
		Points: points,
	}
	resp, err := client.Write(context.Background(), req)
	if err != nil {
		fmt.Printf("write table fail, err: %v\n", err)
		return err
	}
	if resp.Success != uint32(count) {
		fmt.Printf("write table fail, upexpected response Success: %v\n", resp)
		return fmt.Errorf("upexpected write response: %+v", resp)
	}
	fmt.Printf("write table success, response: %+v\n", resp)
	return nil
}

func queryTable(client ceresdb.Client, count int) error {
	querySQL := `SELECT * FROM demo`
	req := ceresdb.SQLQueryRequest{
		Tables: []string{"demo"},
		SQL:    querySQL,
	}
	resp, err := client.SQLQuery(context.Background(), req)
	if err != nil {
		fmt.Printf("query table fail, err:%v\n", err)
		return err
	}
	if len(resp.Rows) != count {
		fmt.Printf("query table fail, unexpected rows count, expected: %d, real: %d", count, len(resp.Rows))
		return fmt.Errorf("unexpected query response: %+v", resp)
	}
	fmt.Printf("query table success, rows:%+v\n", resp.Rows)
	return nil
}

func main() {
	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### new client:")
	client, err := ceresdb.NewClient(endpoint, ceresdb.Direct,
		ceresdb.WithDefaultDatabase("public"),
		ceresdb.EnableLoggerDebug(true),
	)
	if err != nil {
		fmt.Printf("new ceresdb client fail, err: %v\n", err)
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### exists table:")
	if err := existsTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### create table:")
	if err := createTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### write table:")
	if err := writeTable(client, 5); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### query table:")
	if err := queryTable(client, 5); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### drop table:")
	if err := dropTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
}
