package main

import (
	"flag"
)

var (
	workerPort      = flag.String("worker_port", ":8082", "server address for worker server")
	coordinatorPort = flag.String("coordinator_port", ":8080", "coordinator server address")
)

func main() {
	flag.Parse()

	worker := NewServer(*workerPort, *coordinatorPort)
	worker.Start()
}
