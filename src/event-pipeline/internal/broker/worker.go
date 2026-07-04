package broker

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/models"
	"github.com/segmentio/kafka-go"
)

func GetReader(topic string) *kafka.Reader {
	brokers := kafkaBrokers()
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     brokers,
		Topic:       topic,
		GroupID:     "submission-workers",
		MinBytes:    1,
		MaxBytes:    10e6,
		StartOffset: kafka.FirstOffset,
	})

	return r
}

func kafkaBrokers() []string {
	if raw := os.Getenv("KAFKA_BROKERS"); raw != "" {
		parts := strings.Split(raw, ",")
		brokers := make([]string, 0, len(parts))
		for _, broker := range parts {
			broker = strings.TrimSpace(broker)
			if broker != "" {
				brokers = append(brokers, broker)
			}
		}
		if len(brokers) > 0 {
			return brokers
		}
	}

	return []string{"localhost:29092", "localhost:39092", "localhost:49092"}
}

func ProcessMessages(db *sql.DB, reader *kafka.Reader) {
	log.Println("Worker engine is online")

	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Print("Not able to read Messages")
			break
		}
		processSingleMessage(db, m)
	}
}

func processSingleMessage(db *sql.DB, m kafka.Message) {
	var err error
	var tx *sql.Tx

	tx, err = db.Begin()
	if err != nil {
		log.Print("Not able to create Transaction")
		return
	}
	defer tx.Rollback()

	var submission models.KafkaPayload
	json.Unmarshal(m.Value, &submission)
	log.Printf("processed %v", submission.CodePayload)

	var results models.EvaluationResult

	// faking for now
	results.Status = "ACCEPTED"
	results.Stdout = "FINE"
	results.Stderr = "NO ERROR"
	results.ExecutionTime = 42
	results.MemoryUsed = 23

	updateQuery := `UPDATE code_submissions 
	SET status=$1, stdout=$2, stderr=$3, execution_time=$4, memory_used=$5
	WHERE id=$6`

	_, err = tx.Exec(updateQuery, results.Status, results.Stdout, results.Stderr, results.ExecutionTime, results.MemoryUsed, submission.Id)

	if err != nil {
		log.Print("Failed to updateQuery", err)
	}

	err = tx.Commit()

	if err != nil {
		log.Print("Failed to Commit", err)
	}
}
