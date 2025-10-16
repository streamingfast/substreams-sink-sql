package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/proto"
)

type Top struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Level1 *Level1 `json:"level1"`
}

type Level1 struct {
	Name        string `json:"name"`
	Description string `json:"description"`
}

func main() {
	ctx := context.Background()

	// Connect to ClickHouse
	client, err := ch.Dial(ctx, ch.Options{
		Address:  "localhost:9000",
		Database: "default",
		User:     "default",
		Password: "",
	})
	if err != nil {
		log.Fatalf("failed to connect to ClickHouse: %v", err)
	}
	defer client.Close()

	fmt.Println("Connected to ClickHouse")

	// Create database "scratch"
	if err := client.Do(ctx, ch.Query{
		Body: "CREATE DATABASE IF NOT EXISTS scratch",
	}); err != nil {
		log.Fatalf("failed to create database: %v", err)
	}
	fmt.Println("Database 'scratch' created")

	//Reconnect to the scratch database
	client.Close()
	client, err = ch.Dial(ctx, ch.Options{
		Address:  "localhost:9000",
		Database: "scratch",
		User:     "default",
		Password: "",
	})
	if err != nil {
		log.Fatalf("failed to reconnect to scratch database: %v", err)
	}
	defer client.Close()

	//Set flatten_nested to 1 (default) for flattened nested columns
	if err := client.Do(ctx, ch.Query{
		Body: "SET flatten_nested = 1",
	}); err != nil {
		log.Fatalf("failed to set flatten_nested: %v", err)
	}

	// Drop table if exists to ensure clean state
	if err := client.Do(ctx, ch.Query{
		Body: "DROP TABLE IF EXISTS top_table",
	}); err != nil {
		log.Fatalf("failed to drop table: %v", err)
	}
	fmt.Println("Table 'top_table' dropped if existed")

	// Create table with nested structures
	createTableSQL := `
	CREATE TABLE top_table (
		id String,
		name String,
		level1 Nested(
			name String,
			description String
		)
	) ENGINE = MergeTree()
	ORDER BY id
	`

	if err := client.Do(ctx, ch.Query{
		Body: createTableSQL,
	}); err != nil {
		log.Fatalf("failed to create table: %v", err)
	}
	fmt.Println("Table 'top_table' created with nested structures")

	// Prepare data for insertion
	var (
		colID                proto.ColStr
		colName              proto.ColStr
		colLevel1Name        = proto.NewArray[string](new(proto.ColStr))
		colLevel1Description = proto.NewArray[string](new(proto.ColStr))
	)

	// Insert sample data
	// Record 1
	colID.Append("1")
	colName.Append("Top One")
	colLevel1Name.Append([]string{"Level1 A"})
	colLevel1Description.Append([]string{"Description for Level1 A"})

	// Record 2
	colID.Append("2")
	colName.Append("Top Two")
	colLevel1Name.Append([]string{"Level1 B"})
	colLevel1Description.Append([]string{"Description for Level1 B"})

	// Record 3
	colID.Append("3")
	colName.Append("Top Three")
	colLevel1Name.Append([]string{"Level1 C"})
	colLevel1Description.Append([]string{"Description for Level1 C"})

	input := proto.Input{
		{Name: "id", Data: colID},
		{Name: "name", Data: colName},
		{Name: "level1.name", Data: colLevel1Name},
		{Name: "level1.description", Data: colLevel1Description},
	}

	skip := false
	if !skip {
		if err := client.Do(ctx, ch.Query{
			Body:  input.Into("top_table"),
			Input: input,
		}); err != nil {
			log.Fatalf("failed to insert data: %v", err)
		}
		fmt.Println("Data inserted successfully")
	}

	// Query data back
	var (
		resultID                proto.ColStr
		resultName              proto.ColStr
		resultLevel1Name        = proto.NewArray[string](new(proto.ColStr))
		resultLevel1Description = proto.NewArray[string](new(proto.ColStr))
	)

	if err := client.Do(ctx, ch.Query{
		Body: "SELECT id, name, level1.name, level1.description FROM top_table",
		OnResult: func(ctx context.Context, block proto.Block) error {
			return nil
		},
		Result: proto.Results{
			{Name: "id", Data: &resultID},
			{Name: "name", Data: &resultName},
			{Name: "level1.name", Data: resultLevel1Name},
			{Name: "level1.description", Data: resultLevel1Description},
		},
	}); err != nil {
		log.Fatalf("failed to query data: %v", err)
	}

	//fmt.Println("\nQueried data:")
	//for i := 0; i < resultID.Rows(); i++ {
	//	fmt.Printf("Row %d:\n", i+1)
	//	fmt.Printf("  ID: %s\n", resultID.Row(i))
	//	fmt.Printf("  Name: %s\n", resultName.Row(i))
	//	fmt.Printf("  Level1.Name: %v\n", resultLevel1Name.Row(i))
	//	fmt.Printf("  Level1.Description: %v\n", resultLevel1Description.Row(i))
	//}

	// Recreate Top structures from query results
	tops := make([]Top, 0, resultID.Rows())
	for i := 0; i < resultID.Rows(); i++ {
		level1Names := resultLevel1Name.Row(i)
		level1Descriptions := resultLevel1Description.Row(i)

		var level1 *Level1
		if len(level1Names) > 0 {
			level1 = &Level1{
				Name:        level1Names[0],
				Description: level1Descriptions[0],
			}
		}

		top := Top{
			ID:     resultID.Row(i),
			Name:   resultName.Row(i),
			Level1: level1,
		}
		tops = append(tops, top)
	}

	// Print Top structures as JSON
	fmt.Println("\nTop structures as JSON:")
	jsonData, err := json.MarshalIndent(tops, "", "  ")
	if err != nil {
		log.Fatalf("failed to marshal to JSON: %v", err)
	}
	fmt.Println(string(jsonData))

	fmt.Println("\nAll operations completed successfully!")
}
