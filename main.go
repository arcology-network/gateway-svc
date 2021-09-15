package main

import (
	"os"

	ethCli "github.com/arcology-network/3rd-party/tm/cli"
	"github.com/arcology-network/gateway-svc/service"
)

func main() {
	st := service.StartCmd

	cmd := ethCli.PrepareMainCmd(st, "BC", os.ExpandEnv("$HOME/monacos/gateway"))
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}

}
