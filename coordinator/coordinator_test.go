package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	pb "github.com/vaidik-bajpai/D-Scheduler/common/grpcapi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

func TestCoordinatorServer_UpdateSubmitTask(t *testing.T) {
	// server initialisation
	lis := bufconn.Listen(1024 * 1024)

	t.Cleanup(func() {
		lis.Close()
	})

	srv := grpc.NewServer()
	t.Cleanup(func() {
		srv.Stop()
	})

	svc := CoordinatorServer{}
	pb.RegisterCoordinatorServiceServer(srv, &svc)

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Fatal("could not start the server")
		}
	}()

	//test

	dialer := func(context.Context, string) (net.Conn, error) {
		return lis.Dial()
	}

	/* ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	t.Cleanup(func() {
		cancel()
	}) */

	conn, err := grpc.DialContext(
		context.Background(),
		"bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	t.Cleanup(func() {
		conn.Close()
	})
	assert.NoError(t, err, "could not dial to the grpc server")

	client := pb.NewCoordinatorServiceClient(conn)

	taskID := uuid.New()
	res, err := client.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskID: taskID.String(),
		Status: pb.TaskStatus_COMPLETE,
	})
	assert.NoError(t, err, "error while making a request to update task status")

	fmt.Println(res)
}
