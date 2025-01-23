package main

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	pb "github.com/vaidik-bajpai/D-Scheduler/common/grpcapi"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func SpinUpServer(t *testing.T) (*CoordinatorServer, pb.CoordinatorServiceClient, *Mockstorer) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockstorer(ctrl)

	port := ":7000"
	dbConnString := "some random url"
	logger, _ := zap.NewProduction()
	defer logger.Sync()
	coordSrv := NewServer(port, dbConnString, logger, mockStore)
	go func() {
		err := coordSrv.Start()
		assert.NoError(t, err, "error while starting the coordinator server.")
	}()

	time.Sleep(100 * time.Second)

	conn, err := grpc.NewClient(port, grpc.WithTransportCredentials(insecure.NewCredentials()))
	assert.NoError(t, err)

	client := pb.NewCoordinatorServiceClient(conn)

	return coordSrv, client, mockStore
}

func TestCoordinaorUpdateTask(t *testing.T) {
	_, client, mockStore := SpinUpServer(t)

	taskID := uuid.New().String()

	mockStore.EXPECT().
		updateTaskStatus(gomock.Any(), gomock.Any()).
		Return(nil)

	res, err := client.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskID: taskID,
		Status: pb.TaskStatus_STARTED,
	})
	assert.NoError(t, err, "could not update task")
	assert.True(t, res.Success, "update task status successfull check")
}

func TestWorkerHeartbeat(t *testing.T) {
	coordSrv, client, _ := SpinUpServer(t)
	wID := 1
	res, err := client.SendHeartBeat(context.Background(), &pb.HeartbeatRequest{
		WorkerID: uint32(wID),
		Address:  "localhost:8080",
	})
	assert.NoError(t, err, "error when sending a heartbeat")
	assert.True(t, res.Acknowledged, "heart beat acknowledged")
	_, ok := coordSrv.WorkerPool[uint32(wID)]
	assert.True(t, ok, "worker present inside worker pool")
}

/* func TestWorkerHeartbeatDeregister(t *testing.T) {
	coordSrv, client, mockStorer := SpinUpServer(t)
	mockStorer.EXPECT().withTransaction(gomock.Any(), gomock.Any()).Return(nil)

	wID := 1
	res, err := client.SendHeartBeat(context.Background(), &pb.HeartbeatRequest{
		WorkerID: uint32(wID),
		Address:  "localhost:8080",
	})
	assert.NoError(t, err, "error when sending a heartbeat")
	assert.True(t, res.Acknowledged, "heart beat acknowledged")
	_, ok := coordSrv.WorkerPool[uint32(wID)]
	assert.True(t, ok, "worker present inside worker pool")
	time.Sleep(11 * time.Second)
	_, ok = coordSrv.WorkerPool[uint32(wID)]
	log.Println(coordSrv.WorkerPool)
	assert.False(t, ok, "worker deregistered")
} */
