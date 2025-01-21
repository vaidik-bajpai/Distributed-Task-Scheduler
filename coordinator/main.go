package main

import (
	"flag"

	"github.com/vaidik-bajpai/D-Scheduler/common"
)

var (
	coordinatorPort = flag.String("coordinator_port", ":8080", "server address for coordinator")
)

func main() {
	flag.Parse()
	dbConnString := common.GetDBConnectionString()
	coordinator := NewServer(*coordinatorPort, dbConnString)
	coordinator.Start()
}
