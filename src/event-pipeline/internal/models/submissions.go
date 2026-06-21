package models

type SubmissionRequest struct {
	UserId int `json:"user_id"`
	ProblemId int `json:"problem_id"`
	Language string `json:"language"`
	CodePayload string `json:"code_payload"`
}

type EvaluationResult struct {
	Status string `json:"status"`
	Stdout string `json:"stdout"`
	Stderr string `json:"stderr"`
	ExecutionTime int `json:"execution_time"`
	MemoryUsed int `json:"memory_used"`
}

type KafkaPayload struct {
	Id int `json:"id"`
	UserId      int    `json:"user_id"`
	ProblemId    int    `json:"problem_id"`
	Language     string `json:"language"`
	CodePayload  string `json:"code_payload"`
	SubmittedAt  string `json:"submitted_at"`
}
