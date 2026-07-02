package controllers

import (
	"context"
	"database/sql"
	"encoding/json"
	"strconv"
	"time"

	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/ingestion"
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/models"
	pb "github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/proto/build"
	"github.com/segmentio/kafka-go"
)


type Server struct 
{
	pb.UnimplementedSubmissionServiceServer
	DB *sql.DB
	KafkaWriter *kafka.Writer
}

func (s *Server) SubmitHandler(ctx context.Context, req *pb.SubmissionRequest) (*pb.EvaluationResult, error) {
	var err error
	tx, _ := s.DB.Begin()
	insertQuery := `
	INSERT INTO code_submissions(user_id, problem_id, language, code_payload, status)
	VALUES ($1, $2, $3, $4, 'PENDING')
	RETURNING id;
	`

	var submissionId int
	err = tx.QueryRow(insertQuery, req.UserId, req.ProblemId, req.Language, req.CodePayload).Scan(&submissionId)

	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	ctime := time.Now().Format("RFC3339")
	idString := strconv.Itoa(int(req.UserId)) + "-" + ctime

	payload := models.KafkaPayload{
		Id: submissionId,
		UserId: int(req.UserId),
		ProblemId: int(req.ProblemId),
		Language: req.Language,
		CodePayload: req.CodePayload,
		SubmittedAt: ctime,
	}

	kafkaValueBytes, err := json.Marshal(payload)

	message := []kafka.Message{
		kafka.Message{
			Key: []byte(idString),
			Value: kafkaValueBytes,
		},
	}
	err = ingestion.WriteMessage(s.KafkaWriter, message)

	if err != nil {
		return nil, err
	}
	
	err = tx.Commit()
	if err != nil {
		return nil, err
	}


	return &pb.EvaluationResult{
		Status: "QUEUED",
		Stdout: "Request accepted",
	}, nil
}
