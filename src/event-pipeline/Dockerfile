### Building stage
FROM golang:1.26-alpine as builder
WORKDIR /app
COPY . .
RUN go mod download
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/ingestion ./cmd/ingestion
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/broker ./cmd/broker
RUN CGO_ENABLED=0 GOOS=linux go build -o /app/bin/database ./cmd/database


### Runtime stage
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/bin/ingestion /app/ingestion
COPY --from=builder /app/bin/broker /app/broker
COPY --from=builder /app/bin/database /app/database
 

EXPOSE 8080
CMD ["./ingestion"]
