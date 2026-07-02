# Asynchronous-High-Throughput-Event-Pipeline

# Asynchronous High-Throughput Event Pipeline

An asynchronous, highly resilient code submission and evaluation ingestion engine built in Go. This pipeline shifts traditional HTTP REST ingress over to **gRPC (HTTP/2 binary framing)**, persists state transactionally in **PostgreSQL**, and streams payloads smoothly into a multi-partitioned **Kafka cluster** to balance execution workloads across a distributed worker pool.

---

## 🏗 Architecture Overview

The system decouples fast client ingestion from heavy, un-trusted code evaluation using an event-driven architecture:

1. **Ingress (gRPC):** Client sends a strongly-typed binary `SubmissionRequest` payload to the Go server on port `:8080`.
2. **Persistence (Postgres):** The controller opens a secure database transaction, writes the submission metadata as `PENDING`, and retrieves a sequential ID.
3. **Event Streaming (Kafka):** The payload is serialized into a JSON byte array and pushed into the multi-broker `submissions` Kafka topic using a hashed partition balancer.
4. **Resiliency:** If Kafka experiences brief leader elections, the ingestion engine applies automated context-aware backoff retries before committing or rolling back the database state.
5. **Worker Pool Consumption:** Decoupled background broker instances scale independently, continuously pulling hashed keys from partitions to process target code structures concurrently without starving resources.

---

## 📂 Project Directory Layout

```text
├── cmd
│   ├── broker/          # Independent background Kafka consumer worker binaries
│   └── ingestion/       # Main gRPC server entry point application
├── controllers/         # gRPC server implementation and business route logic
├── internal/
│   ├── database/        # Database driver initialization and connection handlers
│   ├── ingestion/       # Resilient Kafka writer engines and retry mechanics
│   └── models/          # Native internal Go database structures and JSON schemas
├── proto/
│   ├── build/
│   └── submission.proto # Core gRPC interface message service contracts
├── Makefile             # Automation tooling scripts for compilation 
└── docker-compose.yml   # Multi-broker Kafka cluster and Postgres stack

---

## How to use it 

- To up all the containers
```cmd
docker-compose up -d 
```

- To build the proto files 
```cmd
make build
```

- To initialise 
```cmd
make initialise
```

- To run project
```cmd
make run-all
```
