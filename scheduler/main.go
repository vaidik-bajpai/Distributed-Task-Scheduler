package main

import (
	"context"
	"flag"
	"log"
	"path/filepath"

	"github.com/joho/godotenv"
	_ "github.com/joho/godotenv/autoload"
	"github.com/vaidik-bajpai/D-Scheduler/common"
	"go.uber.org/zap"
)

var (
	schedulerPort = flag.String("scheduler_port", ":8081", "server address for scheduler")
)

func main() {
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	envPath := filepath.Join("..", ".env")
	if err := godotenv.Load(envPath); err != nil {
		log.Fatal("Error loading .env file")
	}
	dbConnString := common.GetDBConnectionString()
	if dbConnString == "" {
		log.Fatalf("db connection string is empty.")
	}

	dbPool, err := common.CreateDatebaseConnectionPool(context.Background(), dbConnString)
	if err != nil {
		log.Fatalf("failed to connect to the database: %v", err)
	}

	store := NewDB(dbPool)

	scheduler := NewSchedulerServer(*schedulerPort, store, logger)
	log.Printf("Starting the scheduler server at port: %s\n", scheduler.serverPort)
	if err := scheduler.Start(); err != nil {
		log.Fatalf("Error while starting server: %+v", err)
	}
}
