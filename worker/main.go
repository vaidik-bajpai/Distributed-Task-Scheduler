package main

import (
	"flag"

	"go.uber.org/zap"
)

var (
	workerPort      = flag.String("worker_port", ":8082", "server address for worker server")
	coordinatorPort = flag.String("coordinator_port", ":8080", "coordinator server address")
)

func main() {
	flag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	worker := NewServer(*workerPort, *coordinatorPort, logger)
	worker.Start()
}
