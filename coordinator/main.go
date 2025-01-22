package main

import (
	"context"
	"flag"
	"fmt"
	"log"

	"github.com/vaidik-bajpai/D-Scheduler/common"
	"go.uber.org/zap"
)

var (
	coordinatorPort = flag.String("coordinator_port", ":8080", "server address for coordinator")
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	flag.Parse()
	dbConnString := common.GetDBConnectionString()

	ctx := context.Background()
	dbPool, err := common.CreateDatebaseConnectionPool(ctx, dbConnString)
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}

	store := NewDB(dbPool)

	coordinator := NewServer(*coordinatorPort, dbConnString, logger, store)

	logger.Info(fmt.Sprintf("Starting coordinator service on port %s", *coordinatorPort))
	coordinator.Start()
}
