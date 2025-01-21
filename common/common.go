package common

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
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
	var missingEnvVars []string

	checkEnvVar := func(envVar, envVarName string) {
		if envVar == "" {
			missingEnvVars = append(missingEnvVars, envVarName)
		}
	}

	dbUser := os.Getenv("POSTGRES_USER")
	checkEnvVar(dbUser, "POSTGRES_USER")

	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	checkEnvVar(dbPassword, "POSTGRES_PASSWORD")

	dbName := os.Getenv("POSTGRES_DB")
	checkEnvVar(dbName, "POSTGRES_DB")

	dbHost := os.Getenv("POSTGRES_HOST")
	if dbHost == "" {
		dbHost = "localhost"
	}

	if len(missingEnvVars) > 0 {
		log.Fatalf("The following required environment variables are not set: %s",
			strings.Join(missingEnvVars, ", "))
	}

	return fmt.Sprintf("postgres://%s:%s@%s:5432/%s", dbUser, dbPassword, dbHost, dbName)
}
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
