package main

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"

	"github.com/joho/godotenv"
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
	envPath := filepath.Join("..", ".env")
	if err := godotenv.Load(envPath); err != nil {
		log.Fatal("Error loading .env file")
	}
	dbConnString := common.GetDBConnectionString()
	if dbConnString == "" {
		log.Fatalf("db connection string is empty.")
	}

	ctx := context.Background()
	dbPool, err := common.CreateDatebaseConnectionPool(ctx, dbConnString)
	if err != nil {
		logger.Info("error occured while creating a db connection pool", zap.Error(err))
		os.Exit(1)
	}

	store := NewDB(dbPool)

	coordinator := NewServer(*coordinatorPort, dbConnString, logger, store)

	logger.Info("Starting coordinator service", zap.String("port", coordinator.serverPort))
	coordinator.Start()
}
