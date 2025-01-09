package scheduler

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/vaidik-bajpai/distributed-task-scheduler/pkg/common"
)

var (
	QueryTimeDuration = 5 * time.Second
)

type CommandRequest struct {
	Command     string    `json:"command"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

type CreateTaskResponse struct {
	ID          string    `json:"id"`
	Command     string    `json:"command"`
	ScheduledAt time.Time `json:"scheduled_at"`
}

type Task struct {
	ID          string
	Command     string
	ScheduledAt time.Time
	PickedAt    time.Time
	CompletedAt time.Time
	StartedAt   time.Time
	FailedAt    time.Time
}

type SchedulerServer struct {
	serverPort   string
	dbConnString string
	dbPool       *pgxpool.Pool
	ctx          context.Context
	cancel       context.CancelFunc
	httpServer   *http.Server
}

func NewSchedulerServer(serverPort, dbConnString string) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		serverPort:   serverPort,
		dbConnString: dbConnString,
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (s *SchedulerServer) Start() error {
	var err error
	s.dbPool, err = common.CreateDatebaseConnectionPool(context.Background(), s.dbConnString)
	if err != nil {
		return err
	}

	http.HandleFunc("POST /schedule", s.handleScheduleTask)
	http.HandleFunc("GET /status", s.handleGetTaskStatus)

	s.httpServer = &http.Server{
		Addr: s.serverPort,
	}

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("Server error: %v\n", err)
		}
	}()

	return s.awaitShutDown()
}

func (s *SchedulerServer) handleScheduleTask(w http.ResponseWriter, r *http.Request) {
	var task CommandRequest
	if err := json.NewDecoder(r.Body).Decode(&task); err != nil {
		http.Error(w, "could n't decode the request payload", http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	taskID, err := s.insertTaskIntoDB(ctx, &task)
	if err != nil {
		http.Error(w, "could n't create the task requested", http.StatusInternalServerError)
		return
	}

	response := &CreateTaskResponse{
		ID:          taskID,
		Command:     task.Command,
		ScheduledAt: task.ScheduledAt,
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(jsonResponse))
}

func (s *SchedulerServer) handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ctx := r.Context()
	task, err := s.getTaskFromDB(ctx, id)
	if err != nil {
		http.Error(w, "could n't fetch the task from the database", http.StatusInternalServerError)
		return
	}

	jsonResponse, err := json.Marshal(task)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte(jsonResponse))
}

func (s *SchedulerServer) awaitShutDown() error {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	<-stop

	return s.Stop()
}

func (s *SchedulerServer) Stop() error {
	defer s.dbPool.Close()

	if s.httpServer != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		return s.httpServer.Shutdown(ctx)
	}
	log.Println("Scheduler server and database pool stopped")
	return nil
}

func (s *SchedulerServer) insertTaskIntoDB(ctx context.Context, task *CommandRequest) (string, error) {
	query := `INSERT INTO tasks (command, scheduled_at) VALUES (?, ?) RETURNING id`

	ctx, cancel := context.WithTimeout(ctx, QueryTimeDuration)
	defer cancel()

	var insertedId string

	err := s.dbPool.QueryRow(
		ctx,
		query,
		task.Command,
		task.ScheduledAt,
	).Scan(&insertedId)
	if err != nil {
		return "", err
	}

	return insertedId, nil
}

func (s *SchedulerServer) getTaskFromDB(ctx context.Context, taskID string) (*Task, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*QueryTimeDuration)
	defer cancel()

	query := `SELECT * FROM tasks WHERE $1`

	var task Task

	err := s.dbPool.QueryRow(ctx, query, taskID).Scan(
		&task.ID,
		&task.Command,
		&task.ScheduledAt,
		&task.PickedAt,
		&task.StartedAt,
		&task.CompletedAt,
		&task.FailedAt,
	)
	if err != nil {
		return nil, err
	}

	return &task, nil
}
