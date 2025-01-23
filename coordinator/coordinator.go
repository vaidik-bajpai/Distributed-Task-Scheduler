package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	pb "github.com/vaidik-bajpai/D-Scheduler/common/grpcapi"
)

var (
	maxHeartbeatMisses    = 1
	heartbeatTimeInterval = 5 * time.Second
	scanTimeInterval      = 10 * time.Second
)

var (
	ErrWorkerPoolEmpty = errors.New("no workers present inside of the worker pool")
)

type CoordinatorServer struct {
	pb.UnimplementedCoordinatorServiceServer
	logger                *zap.Logger
	store                 storer
	serverPort            string
	listener              net.Listener
	grpcServer            *grpc.Server
	WorkerPool            map[uint32]*workerInfo
	WorkerPoolMutex       sync.RWMutex
	WorkerPoolKeys        []uint32
	WorkerPoolKeysMutex   sync.RWMutex
	maxHeartbeatMisses    uint8
	heartbeatTimeInterval time.Duration
	roundRobinIndex       uint32
	dbConnString          string
	ctx                   context.Context
	cancel                context.CancelFunc
	wg                    sync.WaitGroup
}

type workerInfo struct {
	heartbeatMisses     uint8
	address             string
	grpcConn            *grpc.ClientConn
	workerServiceClient pb.WorkerServiceClient
}

type UpdateTask struct {
	taskID    string
	column    string
	timestamp time.Time
}

func NewServer(serverPort, dbConnString string, logger *zap.Logger, store storer) *CoordinatorServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &CoordinatorServer{
		logger:                logger,
		store:                 store,
		serverPort:            serverPort,
		dbConnString:          dbConnString,
		WorkerPool:            make(map[uint32]*workerInfo),
		maxHeartbeatMisses:    uint8(maxHeartbeatMisses),
		heartbeatTimeInterval: heartbeatTimeInterval,
		ctx:                   ctx,
		cancel:                cancel,
	}
}

func (s *CoordinatorServer) Start() error {
	var err error
	go s.manageWorkerPool()

	if err = s.startGRPCServer(); err != nil {
		return fmt.Errorf("gRPC server start failed: %w", err)
	}

	s.logger.Info("Launching the scan database thread")
	go s.scanDatabase()

	return s.awaitShutDown()
}

func (s *CoordinatorServer) UpdateTaskStatus(ctx context.Context, in *pb.UpdateTaskStatusRequest) (*pb.UpdateTaskStatusResponse, error) {
	status := in.GetStatus()
	taskID := in.GetTaskID()
	var column string
	var timestamp time.Time
	switch status {
	case pb.TaskStatus_STARTED:
		column = "started_at"
		timestamp = time.Unix(in.GetStartedAt(), 0)
	case pb.TaskStatus_COMPLETE:
		column = "completed_at"
		timestamp = time.Unix(in.GetCompletedAt(), 0)
	case pb.TaskStatus_FAILED:
		column = "failed_at"
		timestamp = time.Unix(in.GetFailedAt(), 0)
	default:
		log.Println("invalid status")
		return nil, errors.ErrUnsupported
	}

	if err := s.store.updateTaskStatus(ctx, &UpdateTask{
		taskID:    taskID,
		column:    column,
		timestamp: timestamp,
	}); err != nil {
		return &pb.UpdateTaskStatusResponse{Success: false}, err
	}

	return &pb.UpdateTaskStatusResponse{Success: true}, nil
}

func (s *CoordinatorServer) SendHeartBeat(ctx context.Context, in *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	workerID := in.GetWorkerID()

	if worker, ok := s.WorkerPool[workerID]; ok {
		worker.heartbeatMisses = 0
	} else {
		s.logger.Info("First heartbeat from the worker")
		address := in.GetAddress()
		conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			s.logger.Error(fmt.Sprintf("could not establish a grpc connection with the worker %d", workerID))
			return nil, err
		}

		s.WorkerPoolMutex.Lock()

		s.WorkerPool[workerID] = &workerInfo{
			heartbeatMisses:     0,
			address:             address,
			grpcConn:            conn,
			workerServiceClient: pb.NewWorkerServiceClient(conn),
		}

		s.WorkerPoolKeysMutex.Lock()
		defer s.WorkerPoolKeysMutex.Unlock()

		s.WorkerPoolKeys = make([]uint32, 0, len(s.WorkerPool))
		for k := range s.WorkerPool {
			s.WorkerPoolKeys = append(s.WorkerPoolKeys, k)
		}

		s.logger.Info("new worker registered")
	}

	return &pb.HeartbeatResponse{Acknowledged: true}, nil
}

func (s *CoordinatorServer) SubmitTask(ctx context.Context, in *pb.ClientTaskRequest) (*pb.ClientTaskResponse, error) {
	data := in.GetData()
	taskID := uuid.New().String()
	task := &pb.TaskRequest{
		TaskID: taskID,
		Data:   data,
	}

	if err := s.submitTaskToWorker(task); err != nil {
		s.logger.Error(fmt.Sprintf("could not submit task to a worker %v", err))
		return nil, err
	}

	return &pb.ClientTaskResponse{
		Message: "task submitted successfully",
		TaskID:  taskID,
	}, nil
}

func (s *CoordinatorServer) manageWorkerPool() {
	log.Println("manage worker pool called")
	ticker := time.NewTicker(time.Duration(maxHeartbeatMisses) * s.heartbeatTimeInterval)
	defer ticker.Stop()

	select {
	case <-ticker.C:
		s.removeDeadWorkers()
	case <-s.ctx.Done():
		return
	}
}

func (s *CoordinatorServer) removeDeadWorkers() {
	log.Println("remove worker called")
	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	for workerID, worker := range s.WorkerPool {
		if worker.heartbeatMisses > s.maxHeartbeatMisses {
			worker.grpcConn.Close()
			delete(s.WorkerPool, workerID)

			s.WorkerPoolKeysMutex.Lock()

			workerCount := len(s.WorkerPool)
			s.WorkerPoolKeys = make([]uint32, 0, workerCount)
			for k := range s.WorkerPool {
				s.WorkerPoolKeys = append(s.WorkerPoolKeys, k)
			}

			s.WorkerPoolKeysMutex.Unlock()
		} else {
			worker.heartbeatMisses++
		}
	}
}

func (s *CoordinatorServer) startGRPCServer() error {
	var err error
	s.listener, err = net.Listen("tcp", s.serverPort)
	if err != nil {
		return err
	}

	s.grpcServer = grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(s.grpcServer, s)

	go func() {
		if err := s.grpcServer.Serve(s.listener); err != nil {
			s.logger.Error(fmt.Sprintf("gRPC server failed : %v", err))
		}
	}()

	return nil
}

func (s *CoordinatorServer) scanDatabase() {
	ticker := time.NewTicker(scanTimeInterval)
	defer ticker.Stop()

	select {
	case <-ticker.C:
		s.executeAllScheduledTasks()
	case <-s.ctx.Done():
		log.Println("Shutting down database scanner.")
		return
	}
}

type Task struct {
	ID      string `json:"id"`
	Command string `json:"command"`
}

func (s *CoordinatorServer) executeAllScheduledTasks() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := s.store.withTransaction(ctx, func(tx pgx.Tx) error {
		tasks, err := s.store.getScheduledTasksFromDB(ctx, tx)
		if err != nil {
			return fmt.Errorf("error fetching the scheduled tasks for execution: %v", err)
		}

		for _, task := range tasks {
			if err := s.submitTaskToWorker(task); err != nil {
				log.Printf("could not submit task {id:%s} to worker.", task.GetTaskID())
				continue
			}

			if err := s.store.updatePickedAtStatus(ctx, tx, task.GetTaskID()); err != nil {
				log.Printf("could update the picked at status of the task {id:%s}", task.GetTaskID())
			}
		}

		return nil
	}); err != nil {
		log.Printf("error while executing the scheduled tasks: %v", err)
	}
}

func (s *CoordinatorServer) submitTaskToWorker(task *pb.TaskRequest) error {
	worker := s.getNextWorker()
	if worker == nil {
		return ErrWorkerPoolEmpty
	}

	_, err := worker.workerServiceClient.SubmitTask(context.Background(), task)
	return err
}

func (s *CoordinatorServer) getNextWorker() *workerInfo {
	s.WorkerPoolKeysMutex.RLock()
	defer s.WorkerPoolKeysMutex.RUnlock()

	workerCount := len(s.WorkerPoolKeys)
	if workerCount == 0 {
		return nil
	}

	worker := s.WorkerPool[s.WorkerPoolKeys[s.roundRobinIndex%uint32(workerCount)]]
	s.roundRobinIndex++
	return worker
}

func (s *CoordinatorServer) awaitShutDown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	return s.Stop()
}

func (s *CoordinatorServer) Stop() error {
	s.cancel()

	s.wg.Wait()

	s.WorkerPoolMutex.Lock()
	defer s.WorkerPoolMutex.Unlock()

	for _, worker := range s.WorkerPool {
		if worker.grpcConn != nil {
			worker.grpcConn.Close()
		}
	}

	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}

	if s.listener != nil {
		s.listener.Close()
	}

	s.store.Close()
	return nil
}
