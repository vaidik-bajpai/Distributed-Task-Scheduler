package main

import (
	"context"
	"fmt"
	"log"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	pb "github.com/vaidik-bajpai/D-Scheduler/common/grpcapi"
)

type storer interface {
	updateTaskStatus(ctx context.Context, updateTask *UpdateTask) error
	updatePickedAtStatus(ctx context.Context, tx pgx.Tx, taskID string) error
	getScheduledTasksFromDB(ctx context.Context, tx pgx.Tx) ([]*pb.TaskRequest, error)
	withTransaction(context.Context, func(pgx.Tx) error) error
	Close()
}

type DB struct {
	dbPool *pgxpool.Pool
}

func NewDB(connPool *pgxpool.Pool) *DB {
	return &DB{
		dbPool: connPool,
	}
}

func (d *DB) Close() {
	d.dbPool.Close()
}

func (d *DB) updatePickedAtStatus(ctx context.Context, tx pgx.Tx, taskID string) error {
	updateQuery := `UPDATE tasks SET picked_at = NOW() WHERE id = $1`

	_, err := tx.Exec(ctx, updateQuery, taskID)
	if err != nil {
		return fmt.Errorf("could not update the picked at field of the task with taskID:%s: %v", taskID, err)
	}
	return nil
}

func (d *DB) getScheduledTasksFromDB(ctx context.Context, tx pgx.Tx) ([]*pb.TaskRequest, error) {
	getQuery := `
		SELECT id, command FROM tasks 
		WHERE scheduled_at < (NOW() + INTERVAL '30 seconds') AND picked_at IS NULL 
		ORDER BY scheduled_at 
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.Query(ctx, getQuery)
	if err != nil {
		return nil, fmt.Errorf("could not fetch the tasks scheduled for execution: %v", err)
	}

	defer rows.Close()

	var tasks []*pb.TaskRequest
	for rows.Next() {
		var id, command string
		if err := rows.Scan(&id, &command); err != nil {
			return nil, fmt.Errorf("could not scan the row: %v", err)
		}

		tasks = append(tasks, &pb.TaskRequest{
			TaskID: id,
			Data:   command,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error while iterating over tasks: %v", err)
	}

	return tasks, nil
}

func (d *DB) withTransaction(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := d.dbPool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("could not start a transaction: %v", err)
	}

	defer func() {
		if err := tx.Rollback(ctx); err != nil {
			log.Println("could not rollback the transaction")
		}
	}()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (d *DB) updateTaskStatus(ctx context.Context, updateTask *UpdateTask) error {
	updateQuery := fmt.Sprintf("UPDATE tasks SET %s = $1 WHERE id = $2", updateTask.column)
	_, err := d.dbPool.Exec(ctx, updateQuery, updateTask.timestamp, updateTask.taskID)
	if err != nil {
		return err
	}

	return nil
}
