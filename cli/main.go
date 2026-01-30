package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

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
				ArgsUsage:   "[table] [key]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					tableName := cmd.Args().Slice()[0]
					key := cmd.Args().Slice()[1]
					record, ok, err := localDB.Get(tableName, []byte(key))
					if err != nil {
						return err
					}
					if !ok {
						fmt.Println("key not found")
						return nil
					}

					table := localDB.Tables[tableName]
					for _, col := range table.Schema.Columns {
						fmt.Printf("%s: %s\n", col.Name, string(record.Columns[col.Name]))
					}
					return nil
				},
			},
			{
				Name:        "put",
				Aliases:     []string{"put"},
				Usage:       "store a value",
				UsageText:   "put [table] [col=val] [col=val] ...",
				Description: "store column values: put users id=1 name=alice age=30",
				ArgsUsage:   "[table] [col=val...]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					args := cmd.Args().Slice()
					if len(args) < 2 {
						return fmt.Errorf("usage: put [table] [col=val...]")
					}
					tableName := args[0]
					colValues := map[string][]byte{}
					for _, arg := range args[1:] {
						parts := strings.SplitN(arg, "=", 2)
						if len(parts) != 2 {
							return fmt.Errorf("invalid column value %q, expected col=val", arg)
						}
						colValues[parts[0]] = []byte(parts[1])
					}
					return localDB.Put(tableName, colValues)
				},
			},
			{
				Name:        "list-tables",
				Aliases:     []string{"ls", "\\dt"},
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
				UsageText:   "create-table [name] [col:type:width] ...",
				Description: "create a new table with columns: create-table users id:string:32 name:string:32 age:int:8",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					args := cmd.Args().Slice()
					if len(args) < 1 {
						return fmt.Errorf("usage: create-table [name] [col:type:width...]")
					}
					tableName := args[0]

					opts := &db.CreateTableOptions{}
					if len(args) > 1 {
						var columns []db.Column
						for _, arg := range args[1:] {
							parts := strings.SplitN(arg, ":", 3)
							if len(parts) != 3 {
								return fmt.Errorf("invalid column definition %q, expected name:type:width", arg)
							}
							width, err := strconv.Atoi(parts[2])
							if err != nil {
								return fmt.Errorf("invalid width %q for column %q", parts[2], parts[0])
							}
							var colType db.ColumnType
							switch strings.ToLower(parts[1]) {
							case "string":
								colType = db.ColumnTypeString
							case "int":
								colType = db.ColumnTypeInt
							default:
								return fmt.Errorf("unknown column type %q", parts[1])
							}
							columns = append(columns, db.Column{
								Name:  parts[0],
								Type:  colType,
								Width: width,
							})
						}
						opts.Schema = db.Schema{Columns: columns}
					}

					return localDB.CreateTable(tableName, opts)
				},
			},
			{
				Name:        "delete-table",
				Aliases:     []string{},
				Usage:       "delete a table",
				UsageText:   "delete-table [name]",
				Description: "delete a table",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					tableName := cmd.Args().Slice()[0]
					return localDB.DeleteTable(tableName)
				},
			},
			{
				Name:        "sql",
				Usage:       "execute a SQL query",
				UsageText:   "sql [query]",
				Description: "execute a SQL query",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					query := cmd.Args().Slice()[0]
					records, err := localDB.SQL(query)
					if err != nil {
						return err
					}
					for _, record := range records {
						parts := []string{}
						for colName, colVal := range record.Columns {
							parts = append(parts, fmt.Sprintf("%s=%s", colName, string(colVal)))
						}
						fmt.Println(strings.Join(parts, " "))
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
					table := localDB.Tables[tableName]
					for _, record := range res {
						parts := []string{}
						for _, col := range table.Schema.Columns {
							parts = append(parts, fmt.Sprintf("%s=%s", col.Name, string(record.Columns[col.Name])))
						}
						fmt.Println(strings.Join(parts, " "))
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
