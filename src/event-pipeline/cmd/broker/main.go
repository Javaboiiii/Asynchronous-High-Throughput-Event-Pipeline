package main

import (
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/broker"
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/database"

	"log"
)

func main() {
	reader := broker.GetReader("submissions")
	db, err := database.EstablishConnection(database.ConnStr)

	if err != nil {
		log.Print("Failed to establish connection with Database")
		return
	}

	defer db.Close()
	defer reader.Close()

	broker.ProcessMessages(db, reader)
}
