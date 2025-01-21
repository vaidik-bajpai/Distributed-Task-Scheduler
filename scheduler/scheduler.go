package main

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
	serverPort string
	store      storer
	dbPool     *pgxpool.Pool
	ctx        context.Context
	cancel     context.CancelFunc
	httpServer *http.Server
}

func NewSchedulerServer(serverPort string, store storer) *SchedulerServer {
	ctx, cancel := context.WithCancel(context.Background())
	return &SchedulerServer{
		serverPort: serverPort,
		store:      store,
		ctx:        ctx,
		cancel:     cancel,
	}
}

func (s *SchedulerServer) Start() error {

	http.HandleFunc("GET /health", s.handleHandleHealth)
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
	taskID, err := s.store.insertTaskIntoDB(ctx, &task)
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
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(jsonResponse))
}

func (s *SchedulerServer) handleGetTaskStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ctx := r.Context()
	task, err := s.store.getTaskFromDB(ctx, id)
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
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(jsonResponse))
}

func (s *SchedulerServer) handleHandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("healthy"))
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
