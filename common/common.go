package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
)

const (
	DefaulHeartbeatInterval = 5 * time.Second
)

var (
	maxRetries = 5
	retryAfter = 5 * time.Second
)

func GetDBConnectionString() string {
	dbConnString, ok := os.LookupEnv("DB_CONN_STRING")
	if !ok {
		return ""
	}

	return dbConnString
}

func CreateDatebaseConnectionPool(ctx context.Context, dbConnString string) (*pgxpool.Pool, error) {
	var dbPool *pgxpool.Pool
	var err error
	for i := 0; i < maxRetries; i++ {
		dbPool, err = pgxpool.Connect(context.Background(), dbConnString)
		if err == nil {
			break
		}
		log.Printf("error happened while trying to connect to db (retries left [%d]): %v", maxRetries-(i+1), err)
		time.Sleep(retryAfter)
	}

	if err != nil {
		return nil, fmt.Errorf("all tries exhausted, yet a connection with the database could n't be established")
	}

	return dbPool, nil
}
