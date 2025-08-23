package main

import (
	"context"
	"fmt"
	"os"

	"github.com/urfave/cli/v3"
)

func main() {

	// load db

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
					fmt.Printf("get command %+v", cmd.Args().Slice())
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
