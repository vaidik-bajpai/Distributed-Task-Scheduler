package main

import (
	"flag"

	"github.com/vaidik-bajpai/distributed-task-scheduler/pkg/common"
	"github.com/vaidik-bajpai/distributed-task-scheduler/pkg/coordinator"
)

var (
	coordinatorPort = flag.String("coordinator_port", ":8080", "server address for coordinator")
)

func main() {
	flag.Parse()
	dbConnString := common.GetDBConnectionString()
	coordinator := coordinator.NewServer(*coordinatorPort, dbConnString)
	coordinator.Start()
}
