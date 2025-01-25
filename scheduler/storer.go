package main

import (
	"context"

	"github.com/jackc/pgx/v4/pgxpool"
)

type storer interface {
	insertTaskIntoDB(ctx context.Context, task *CommandRequest) (string, error)
	getTaskFromDB(ctx context.Context, taskID string) (*Task, error)
}

type DB struct {
	dbPool *pgxpool.Pool
}

func NewDB(connPool *pgxpool.Pool) *DB {
	return &DB{connPool}
}

func (d *DB) insertTaskIntoDB(ctx context.Context, task *CommandRequest) (string, error) {
	query := `INSERT INTO tasks (command, scheduled_at) VALUES ($1, $2) RETURNING id`

	ctx, cancel := context.WithTimeout(ctx, QueryTimeDuration)
	defer cancel()

	var insertedId string

	err := d.dbPool.QueryRow(
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

func (d *DB) getTaskFromDB(ctx context.Context, taskID string) (*Task, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*QueryTimeDuration)
	defer cancel()

	query := `SELECT * FROM tasks WHERE id = $1`

	var task Task

	err := d.dbPool.QueryRow(ctx, query, taskID).Scan(
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
