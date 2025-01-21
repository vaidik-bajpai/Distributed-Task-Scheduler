package main

import (
	"context"
	"flag"
	"log"

	"github.com/vaidik-bajpai/D-Scheduler/common"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "server address for scheduler")
)

func main() {
	flag.Parse()
	dbConnString := common.GetDBConnectionString()

	dbPool, err := common.CreateDatebaseConnectionPool(context.Background(), dbConnString)
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}

	store := NewDB(dbPool)

	scheduler := NewSchedulerServer(*schedulerPort, store)
	if err := scheduler.Start(); err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
