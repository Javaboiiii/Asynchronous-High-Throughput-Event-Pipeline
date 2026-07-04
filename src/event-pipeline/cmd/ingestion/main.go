package main

import (
	"log"
	"net"

	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/controllers"
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/database"
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/ingestion"
	pb "github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/proto/build"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
)

func main() {
	port := ":8080"

	lis, err := net.Listen("tcp", port)

	if err != nil {
		log.Fatal(err)
	}

	db, err := database.EstablishConnection(database.ConnStr)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	var kafkaWriter *kafka.Writer = ingestion.GetWriter("submissions")
	defer kafkaWriter.Close()

	submissionOpt := &controllers.Server{
		DB:          db,
		KafkaWriter: kafkaWriter,
	}

	if err != nil {
		log.Fatal("Failed to listen on port", port)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterSubmissionServiceServer(grpcServer, submissionOpt)
	grpcServer.Serve(lis)
}
