ifneq (,$(wildcard .env))
    include .env
    export $(shell sed 's/=.*//' .env)
endif

MIG_PATH := ./scheduler/migrations

gen:
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative ./common/grpcapi/api.proto

mig_create:
	migrate create -ext sql -dir $(MIG_PATH) -seq $(name)

mig_up:
	migrate -database $(DB_CONN_STRING) -path $(MIG_PATH) up

mig_down:
	migrate -database $(DB_CONN_STRING) -path $(MIG_PATH) down

mig_force:
	migrate -database $(DB_CONN_STRING) -path $(MIG_PATH) force $(version)

mig_drop:
	migrate -database $(DB_CONN_STRING) -path $(MIG_PATH) drop

mig_status:
	migrate -database $(DB_CONN_STRING) -path $(MIG_PATH) version