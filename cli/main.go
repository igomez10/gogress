package main

import (
	"context"
	"fmt"
	"os"

	"github.com/igomez10/gogress/pkg/db"
	"github.com/urfave/cli/v3"
)

func main() {
	logFile, err := os.OpenFile("/tmp/gogress.log", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		// ignore
	}
	defer logFile.Close()

	localDB, err := db.NewDB(db.NewDBOptions{
		LogFile: logFile,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "error loading db: %v\n", err)
		os.Exit(1)
	}

	cmd := &cli.Command{
		Name:  "gogress-cli",
		Usage: "A simple db",
		Commands: []*cli.Command{
			{
				Name:        "get",
				Aliases:     []string{"g"},
				Usage:       "retrieve a value",
				UsageText:   "get [table] [key]",
				Description: "retrieve a value by key",
				ArgsUsage:   "[key]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					tableName := cmd.Args().Slice()[0]
					key := cmd.Args().Slice()[1]
					content, ok, err := localDB.Get([]byte(tableName), []byte(key))
					if err != nil {
						return err
					}
					if !ok {
						fmt.Println("key not found")
						return nil
					}

					fmt.Println(string(content))
					return nil
				},
			},
			{
				Name:        "put",
				Aliases:     []string{"put"},
				Usage:       "store a value",
				UsageText:   "put [table] [key] [value]",
				Description: "store a value by key",
				ArgsUsage:   "[table] [key] [value]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					tableName := cmd.Args().Slice()[0]
					key := cmd.Args().Slice()[1]
					val := cmd.Args().Slice()[2]
					if err := localDB.Put([]byte(tableName), []byte(key), []byte(val)); err != nil {
						return err
					}
					return nil
				},
			},
			{
				Name:        "list-tables",
				Aliases:     []string{"ls"},
				Usage:       "list all tables",
				UsageText:   "list",
				Description: "list all tables",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					tables := localDB.ListTables()
					for _, table := range tables {
						fmt.Println(table)
					}
					return nil
				},
			},
			{
				Name:        "create-table",
				Aliases:     []string{"ct"},
				Usage:       "create a new table",
				UsageText:   "create-table [name]",
				Description: "create a new table",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					tableName := cmd.Args().Slice()[0]
					if err := localDB.CreateTable(tableName, &db.CreateTableOptions{}); err != nil {
						return err
					}
					return nil
				},
			},
			{
				Name:        "delete-table",
				Aliases:     []string{"dt"},
				Usage:       "delete a table",
				UsageText:   "delete-table [name]",
				Description: "delete a table",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					tableName := cmd.Args().Slice()[0]
					if err := localDB.DeleteTable(tableName); err != nil {
						return err
					}
					return nil
				},
			},
			{
				Name:        "sql",
				Usage:       "execute a SQL query",
				UsageText:   "sql [query]",
				Description: "execute a SQL query",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					query := cmd.Args().Slice()[0]
					fmt.Println("Executing query:", query)
					if err := localDB.SQL(query); err != nil {
						return err
					}
					return nil
				},
			},
			{
				Name:        "scan",
				Usage:       "scan a table",
				UsageText:   "scan [table]",
				Description: "scan a table for all records",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:  "limit",
						Usage: "limit the number of records returned",
					},
					&cli.IntFlag{
						Name:  "offset",
						Usage: "skip a number of records",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					tableName := cmd.Args().Slice()[0]
					opts := db.ScanOptions{
						Limit:  cmd.Int("limit"),
						Offset: cmd.Int("offset"),
					}
					res, err := localDB.Scan(tableName, opts)
					if err != nil {
						return err
					}
					for _, record := range res {
						fmt.Printf("%s: %s\n", record.Key, record.Value)
					}
					return nil
				},
			},
		},
	}

	if err := cmd.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
