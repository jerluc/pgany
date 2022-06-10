package main

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"os"

	"github.com/urfave/cli/v2"

	"github.com/jerluc/pgany/pg"
)

var (
	LogLevels = map[string]log.Level{
		"trace": log.TraceLevel,
		"debug": log.DebugLevel,
		"info":  log.InfoLevel,
		"warn":  log.WarnLevel,
		"error": log.ErrorLevel,
		"fatal": log.FatalLevel,
	}
)

// CLI Flags
const (
	ServerCategory = "Server"
)

var (
	BindAddress = &cli.StringFlag{
		Category: ServerCategory,
		Name:     "bind-address",
		Aliases:  []string{"B"},
		Value:    "tcp://127.0.0.1:5432",
		Usage:    "Address to binding the PG protocol server (supports TCP and Unix sockets)",
	}
	LogLevel = &cli.StringFlag{
		Category: ServerCategory,
		Name:     "log-level",
		Aliases:  []string{"L"},
		Value:    "info",
		Usage:    "Set server log levels",
	}
)

func main() {
	app := &cli.App{
		Name:  "pgany",
		Usage: "Create PostgreSQL wire protocol-compatible servers",
		Flags: []cli.Flag{
			BindAddress,
			LogLevel,
		},
		Action: func(c *cli.Context) error {
			log.SetFormatter(&log.TextFormatter{
				ForceColors:            true,
				FullTimestamp:          true,
				DisableLevelTruncation: false,
			})
			logLevelStr := LogLevel.Get(c)
			if logLevel, exists := LogLevels[logLevelStr]; exists {
				log.SetLevel(logLevel)
			} else {
				return cli.Exit(fmt.Sprintf("Invalid log level: %s", logLevelStr), 1)
			}
			addr := BindAddress.Get(c)
			server, err := pg.NewPGProtoServer(addr)
			if err != nil {
				return cli.Exit(err, 1)
			}
			return cli.Exit(server.Listen(), 2)
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
