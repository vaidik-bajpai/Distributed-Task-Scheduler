package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestStartSchedulerServer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	mockStore := NewMockstorer(ctrl)
	port := ":8080"
	schSrv := NewSchedulerServer(port, mockStore, logger)

	go func() {
		err := schSrv.Start()
		assert.NoError(t, err, "error starting the test scheduler server.")
	}()

	time.Sleep(5 * time.Second)

	res, err := http.Get("http://localhost:8080/health")
	assert.NoError(t, err, "error while checking health")
	assert.Equal(t, 200, res.StatusCode, "status not OK (200)")
}

func TestScheduleTask(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	mockStore := NewMockstorer(ctrl)
	id := uuid.New().String()

	mockStore.EXPECT().
		insertTaskIntoDB(gomock.Any(), gomock.Any()).
		Return(id, nil)

	port := ":8080"
	schSrv := NewSchedulerServer(port, mockStore, logger)

	go func() {
		err := schSrv.Start()
		assert.NoError(t, err, "error starting the test scheduler server.")
	}()

	time.Sleep(5 * time.Second)

	cmdReq := &CommandRequest{
		Command:     "some command",
		ScheduledAt: time.Now().Add(time.Hour),
	}
	payload, err := json.Marshal(cmdReq)
	assert.NoError(t, err, "error marshalling the command request")
	res, err := http.Post("http://localhost:8080/schedule", "application/json", bytes.NewBuffer(payload))
	assert.NoError(t, err, "error ocurred while scheduling the task")
	assert.Equal(t, http.StatusOK, res.StatusCode, "unexpected status code")

	var jsonResponse CreateTaskResponse
	err = json.NewDecoder(res.Body).Decode(&jsonResponse)
	assert.NoError(t, err)
	assert.Equal(t, id, jsonResponse.ID)
}

func TestGetTaskStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	logger, _ := zap.NewProduction()
	defer logger.Sync()

	mockStore := NewMockstorer(ctrl)

	mockTask := &Task{
		ID:          "1",
		Command:     "echo 'Hello, World!'",
		ScheduledAt: time.Now(),
	}

	mockStore.EXPECT().
		getTaskFromDB(gomock.Any(), gomock.Any()).
		Return(mockTask, nil).Times(1)

	port := ":8080"
	schSrv := NewSchedulerServer(port, mockStore, logger)

	go func() {
		err := schSrv.Start()
		assert.NoError(t, err, "error starting the test scheduler server.")
	}()

	time.Sleep(5 * time.Second)

	res, err := http.Get("http://localhost:8080/status?id=1")
	assert.NoError(t, err, "error ocurred while scheduling the task")
	assert.Equal(t, http.StatusOK, res.StatusCode, "unexpected status code")

	var taskResponse Task
	err = json.NewDecoder(res.Body).Decode(&taskResponse)
	assert.NoError(t, err, "error decoding the response body")
	assert.Equal(t, mockTask.ID, taskResponse.ID, "unexpected task ID")
	assert.Equal(t, mockTask.Command, taskResponse.Command, "unexpected command")
}
