package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vaidik-bajpai/D-Scheduler/common"
	pb "github.com/vaidik-bajpai/D-Scheduler/common/grpcapi"
	"go.uber.org/zap"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	taskProcessTime = 5 * time.Second
	workerPoolSize  = 5
)

type WorkerServer struct {
	pb.UnimplementedWorkerServiceServer
	id                       uint32
	logger                   *zap.Logger
	serverPort               string
	coordinatorAddr          string
	listener                 net.Listener
	grpcServer               *grpc.Server
	coordinatorConn          *grpc.ClientConn
	coordinatorServiceClient pb.CoordinatorServiceClient
	heartbeatInterval        time.Duration
	taskQueue                chan *pb.TaskRequest
	ReceivedTasks            map[string]*pb.TaskRequest
	ReceivedTaskMutex        sync.Mutex
	ctx                      context.Context
	cancel                   context.CancelFunc
	wg                       sync.WaitGroup
}

func NewServer(serverPort, coordinatorAddr string, logger *zap.Logger) *WorkerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &WorkerServer{
		id:                uuid.New().ID(),
		logger:            logger,
		serverPort:        serverPort,
		coordinatorAddr:   coordinatorAddr,
		heartbeatInterval: common.DefaulHeartbeatInterval,
		taskQueue:         make(chan *pb.TaskRequest, 100),
		ReceivedTasks:     make(map[string]*pb.TaskRequest),
		ctx:               ctx,
		cancel:            cancel,
	}
}

func (s *WorkerServer) Start() error {
	s.startWorkerPool(workerPoolSize)

	if err := s.connectToCoordinator(); err != nil {
		return fmt.Errorf("could not connect to coordinator service: %w", err)
	}

	defer s.closeGRPCConnection()

	go s.periodicHeartbeat()

	if err := s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	return s.awaitShutDown()
}

func (s *WorkerServer) connectToCoordinator() error {
	log.Println("Connecting to coordinator....")
	var err error
	s.coordinatorConn, err = grpc.NewClient(s.coordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}

	s.coordinatorServiceClient = pb.NewCoordinatorServiceClient(s.coordinatorConn)
	log.Println("Connected to coordinator!")
	return nil

}

func (s *WorkerServer) periodicHeartbeat() {
	s.wg.Add(1)
	defer s.wg.Done()

	ticker := time.NewTicker(s.heartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := s.sendHeartbeat(); err != nil {
				log.Printf("Failed to send heart beat to the coordinator: %v", err)
				return
			}
		case <-s.ctx.Done():
			return
		}
	}
}

func (s *WorkerServer) sendHeartbeat() error {
	workerAddress := os.Getenv("WORKER_ADDR")
	if workerAddress == "" {
		workerAddress = s.listener.Addr().String()
	} else {
		workerAddress += s.serverPort
	}
	_, err := s.coordinatorServiceClient.SendHeartBeat(context.Background(), &pb.HeartbeatRequest{
		WorkerID: s.id,
		Address:  workerAddress,
	})
	return err
}

func (s *WorkerServer) startGRPCServer() error {
	var err error
	if s.serverPort == "" {
		s.listener, err = net.Listen("tcp", ":0")
		s.serverPort = fmt.Sprintf(":%d", s.listener.Addr().(*net.TCPAddr).Port)
	} else {
		s.listener, err = net.Listen("tcp", s.serverPort)
	}
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.serverPort, err)
	}

	log.Printf("listener listening on port %s\n", s.serverPort)

	s.grpcServer = grpc.NewServer()
	pb.RegisterWorkerServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			log.Fatalf("gRPC server failed: %v", err)
		}
	}()

	return nil
}

func (s *WorkerServer) awaitShutDown() error {
	quitCh := make(chan os.Signal, 1)
	signal.Notify(quitCh, syscall.SIGTERM, syscall.SIGINT)
	<-quitCh

	return s.Stop()
}

func (s *WorkerServer) Stop() error {
	s.cancel()

	s.wg.Wait()

	s.closeGRPCConnection()
	log.Printf("the worker has been stopped")
	return nil
}

func (s *WorkerServer) closeGRPCConnection() {
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			log.Printf("error while closing the listener for the worker server: %v\n", err)
		}
	}

	if err := s.coordinatorConn.Close(); err != nil {
		log.Printf("error while closing the client client connection with the coordinator: %v", err)
	}
}

func (s *WorkerServer) SubmitTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	log.Printf("received task: %v", req)

	s.ReceivedTaskMutex.Lock()
	s.ReceivedTasks[req.GetTaskID()] = req
	s.ReceivedTaskMutex.Unlock()

	s.taskQueue <- req

	return &pb.TaskResponse{
		Message: "Task was submitted",
		Success: true,
		TaskID:  req.TaskID,
	}, nil
}

func (s *WorkerServer) startWorkerPool(workerPoolSize uint32) {
	for i := 0; i < int(workerPoolSize); i++ {
		s.wg.Add(1)
		go s.worker()
	}
}

func (s *WorkerServer) worker() {
	defer s.wg.Done()

	for task := range s.taskQueue {
		go s.updateTaskStatus(task, pb.TaskStatus_STARTED)
		err := s.processTask(task)
		if err != nil {
			go s.updateTaskStatus(task, pb.TaskStatus_FAILED)
		} else {
			go s.updateTaskStatus(task, pb.TaskStatus_COMPLETE)
		}
	}
}

func (s *WorkerServer) updateTaskStatus(task *pb.TaskRequest, status pb.TaskStatus) {
	s.coordinatorServiceClient.UpdateTaskStatus(context.Background(), &pb.UpdateTaskStatusRequest{
		TaskID:      task.TaskID,
		Status:      status,
		StartedAt:   time.Now().Unix(),
		CompletedAt: time.Now().Unix(),
	})
}

func (s *WorkerServer) processTask(task *pb.TaskRequest) error {
	s.logger.Info("worker executing python command for the task",
		zap.String("taskID", task.GetTaskID()),
		zap.Uint32("workerID", s.id),
	)

	pythonScript := strings.TrimPrefix(task.Data, "python3 -c ")
	pythonScript = pythonScript[1 : len(pythonScript)-1]
	pythonScript = strings.ReplaceAll(pythonScript, `\"`, `"`)

	s.logger.Info("python script to be executed", zap.String("Python Script :", pythonScript))

	tmpFile, err := os.CreateTemp("", "script-*.py")
	if err != nil {
		s.logger.Error("Worker failed to create the temporary directory.", zap.Uint32("worker id", s.id), zap.Error(err))
		return err
	}
	defer tmpFile.Close()

	_, err = tmpFile.WriteString(pythonScript)
	if err != nil {
		s.logger.Error("Worker could not write to the script file", zap.Uint32("worker id", s.id), zap.Error(err))
		return err
	}

	if err := tmpFile.Close(); err != nil {
		s.logger.Error("Worker could n't close the temp file (python script file)", zap.Uint32("worker id", s.id), zap.Error(err))
		return err
	}

	cmd := exec.Command("python3", tmpFile.Name())
	output, err := cmd.CombinedOutput()
	if err != nil {
		s.logger.Error("worker failed to execute python script for the task",
			zap.String("task id", task.GetTaskID()),
			zap.Uint32("worker id", s.id),
		)
		return err
	}

	s.logger.Info("worker successfully executed the python script of the task",
		zap.String("task id", task.GetTaskID()),
		zap.Uint32("worker id", s.id),
	)

	s.logger.Info("output of the task",
		zap.String("Output", string(output)),
	)
	return nil
}
