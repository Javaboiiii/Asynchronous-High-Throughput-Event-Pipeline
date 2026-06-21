package controllers

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/ingestion" 
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/models" 
)



func SubmitHandler(db *sql.DB, Writer *kafka.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// 1. defining the variables 
		var err error
		var reqBody []byte
		var kafkaValueBytes []byte 
		
		// 2. reading payload 
		reqBody, err = io.ReadAll(r.Body)

		if err != nil {
			http.Error(w, "Failed to read request", http.StatusBadRequest)
			return 
		}

		var submission models.SubmissionRequest
		tx, _ := db.Begin()
		json.Unmarshal(reqBody, &submission)

		// 3. inserting the data in database 
		insertQuery := `
			INSERT INTO code_submissions(user_id, problem_id, language, code_payload, status)
			VALUES ($1, $2, $3, $4, 'PENDING')
			RETURNING id;
		`
		
		var submissionId int
		err = tx.QueryRow(insertQuery, submission.UserId, submission.ProblemId, submission.Language, submission.CodePayload).Scan(&submissionId)
		if err != nil {
			http.Error(w, "Failed to fulfil your request try again", http.StatusInternalServerError)
		}
		defer tx.Rollback() // roolback if our kafka fails 

		// 4. inserting in the kafka 
		ctime := time.Now().Format(time.RFC3339)
		idString := strconv.Itoa(submission.UserId) + "-" + ctime

		payload := models.KafkaPayload{
			Id: submissionId,
			UserId: submission.UserId,
			ProblemId: submission.ProblemId,
			Language: submission.Language, 
			CodePayload: submission.CodePayload,
			SubmittedAt: ctime,
		}

		kafkaValueBytes, err = json.Marshal(payload)

		message := []kafka.Message{
			kafka.Message{
				Key: []byte(idString),
				Value: kafkaValueBytes,
			},
		}
		err = ingestion.WriteMessage(Writer, message)

		if err != nil {
			log.Print("Error is", err)
			http.Error(w, "Failed to persit message in event pipeline", http.StatusInternalServerError)
			return 
		}
		
		// 5. Commiting in the database 
		err = tx.Commit()
		if err != nil {
			http.Error(w, "Failed to commit", http.StatusInternalServerError)
		}

		// 6. Return response if successful 
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status": "QUEUED", "message": "Request accepted"}`))
	}
}
