package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	pb "github.com/vaidik-bajpai/D-Scheduler/common/grpcapi"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestWorkersExecPython(t *testing.T) {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cAddr := ":7000"
	srvPort := ":3333"

	srv := NewServer(srvPort, cAddr, logger)
	go func() {
		err := srv.Start()
		assert.NoError(t, err, "error while starting the worker server")
	}()

	time.Sleep(5 * time.Second)

	conn, err := grpc.NewClient(srvPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err, "error while dialing to the worker server")

	client := pb.NewWorkerServiceClient(conn)

	taskID := uuid.New().String()
	_, err = client.SubmitTask(context.Background(), &pb.TaskRequest{
		TaskID: taskID,
		Data:   `python3 -c "print('Hello from Python!')"`,
	})
	assert.NoError(t, err, "error while submiting task to the worker")

	time.Sleep(5 * time.Second)
}
