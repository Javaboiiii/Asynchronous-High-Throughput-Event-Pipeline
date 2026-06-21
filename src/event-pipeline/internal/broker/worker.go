package broker 

import(
	"github.com/segmentio/kafka-go"
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/models" 

	"context"
	"log"
	"encoding/json"
	"database/sql"
)

func GetReader(topic string) *kafka.Reader {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:29092", "localhost:39092", "localhost:49092"},
		Topic: topic,
		GroupID: "submission-workers",
		MinBytes: 1,
		MaxBytes: 10e6,
		StartOffset: kafka.FirstOffset,
	})

	return r 
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
	defer tx.Rollback()

	if err != nil {
		log.Print("Not able to create Transaction")
		return 
	}

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

	_, err = tx.Exec(updateQuery, results.Status, results.Stdout, results.Stderr, results.ExecutionTime, results.MemoryUsed, submissdion.Id)

	if err != nil {
		log.Print("Failed to updateQuery", err)
	}

	err = tx.Commit() 

	if err != nil {
		log.Print("Failed to Commit", err)
	}
}
