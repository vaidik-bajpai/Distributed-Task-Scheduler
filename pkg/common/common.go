package common

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	maxRetries = 5
	retryAfter = 5 * time.Second
)

func CreateDatebaseConnectionPool(ctx context.Context, dbConnString string) (*pgxpool.Pool, error) {
	var dbPool *pgxpool.Pool
	var err error
	for i := 0; i < maxRetries; i++ {
		dbPool, err = pgxpool.Connect(context.Background(), dbConnString)
		if err == nil {
			break
		}
		time.Sleep(retryAfter)
	}

	if err != nil {
		return nil, fmt.Errorf("all tries exhausted, yet a connection with the database could n't be established")
	}

	return dbPool, nil
}
