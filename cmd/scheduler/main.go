package main

import (
	"flag"
	"log"

	"github.com/vaidik-bajpai/distributed-task-scheduler/pkg/common"
	"github.com/vaidik-bajpai/distributed-task-scheduler/pkg/scheduler"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "server address for scheduler")
)

func main() {
	flag.Parse()
	dbConnString := common.GetDBConnectionString()
	scheduler := scheduler.NewSchedulerServer(*schedulerPort, dbConnString)
	if err := scheduler.Start(); err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
