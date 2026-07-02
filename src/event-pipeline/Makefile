PROTOC := ~/local_protoc/bin/protoc
PROTO_FILES := $(wildcard ./proto/*.proto)

build:
	$(PROTOC) -I proto --go_out=./proto/build/ --go_opt=paths=source_relative --go-grpc_out=./proto/build/ --go-grpc_opt=paths=source_relative $(PROTO_FILES)

run-all:
	@echo "Starting gRPC Ingestion server..."
	go run ./cmd/ingestion/main.go & \
	sleep 2; \
	echo "Spawning 3 background Kafka consumer workers..."; \
	go run ./cmd/broker/main.go & \
	go run ./cmd/broker/main.go & \
	go run ./cmd/broker/main.go & \
	wait

initialise:
	go run ./cmd/database/main.go
