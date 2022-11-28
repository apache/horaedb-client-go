// Copyright 2022 CeresDB Project Authors. Licensed under Apache-2.0.

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/CeresDB/ceresdb-client-go/ceresdb"
	"github.com/CeresDB/ceresdb-client-go/types"
	"github.com/CeresDB/ceresdb-client-go/utils"
)

var endpoint = "127.0.0.1:8831"

func init() {
	if v := os.Getenv("CERESDB_ADDR"); v != "" {
		endpoint = v
	}
}

func createTable(client ceresdb.Client) error {
	createTableSQL := `CREATE TABLE IF NOT EXISTS demo (
	name string TAG,
	value double,
	t timestamp NOT NULL,
	TIMESTAMP KEY(t)) ENGINE=Analytic with (enable_ttl=false)`

	req := types.QueryRequest{
		Metrics: []string{"demo"},
		Ql:      createTableSQL,
	}
	_, err := client.Query(context.Background(), req)
	if err != nil {
		fmt.Printf("create table fail, err:%v\n", err)
		return err
	}
	fmt.Printf("create table success\n")
	return nil
}

func dropTable(client ceresdb.Client) error {
	dropTableSQL := `DROP TABLE demo`
	req := types.QueryRequest{
		Metrics: []string{"demo"},
		Ql:      dropTableSQL,
	}
	_, err := client.Query(context.Background(), req)
	if err != nil {
		fmt.Printf("drop table fail, err:%v\n", err)
		return err
	}
	fmt.Printf("drop table success\n")
	return nil
}

func writeTable(client ceresdb.Client) error {
	builder := ceresdb.NewRowBuilder("demo")
	row, err := builder.
		SetTimestamp(utils.CurrentMS()).AddTag("name", "test_tag1").AddField("value", 0.4242).Build()
	if err != nil {
		fmt.Printf("write table build row fail, err:%v\n", err)
		return err
	}
	resp, err := client.Write(context.Background(), []*types.Row{row})
	if err != nil {
		fmt.Printf("write table fail, err:%v\n", err)
		return err
	}
	if resp.Success != 1 {
		fmt.Printf("write table fail, upexpected response Success:%v\n", resp)
		return fmt.Errorf("upexpected response:%v", resp)
	}
	fmt.Printf("write table success\n")
	return nil
}

func queryTable(client ceresdb.Client) error {
	querySQL := `SELECT * FROM demo`
	req := types.QueryRequest{
		Metrics: []string{"demo"},
		Ql:      querySQL,
	}
	resp, err := client.Query(context.Background(), req)
	if err != nil {
		fmt.Printf("query table fail, err:%v\n", err)
		return err
	}
	fmt.Printf("query table success, rows:%+v\n", resp.Rows)
	return nil
}

func main() {
	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### new client:")
	client, err := ceresdb.NewClient(endpoint,
		ceresdb.EnableLoggerDebug(true),
	)
	if err != nil {
		fmt.Printf("new ceresdb client fail, err:%v\n", err)
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### create table:")
	if err := createTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### write table:")
	if err := writeTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### query table:")
	if err := queryTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
	fmt.Println("### drop table:")
	if err := dropTable(client); err != nil {
		return
	}

	fmt.Println("------------------------------------------------------------------")
}
