package main

import (
	"context"
	"fmt"
	"os"

	"github.com/igomez10/gogress/pkg/db"
	"github.com/urfave/cli/v3"
)

func main() {
	localDB, err := db.NewDB(db.NewDBOptions{})
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
				UsageText:   "get [key]",
				Description: "retrieve a value by key",
				ArgsUsage:   "[key]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					key := cmd.Args().Slice()[0]
					content, ok, err := localDB.Get([]byte(key))
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
				UsageText:   "put [key] [value]",
				Description: "store a value by key",
				ArgsUsage:   "[key] [value]",
				Action: func(ctx context.Context, cmd *cli.Command) error {
					fmt.Printf("put command %+v", cmd.Args().Slice())
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
