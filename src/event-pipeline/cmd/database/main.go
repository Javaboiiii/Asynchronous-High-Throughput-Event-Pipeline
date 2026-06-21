package main 

import (
	"log"

	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/database"
)

func main() {
	DB, err := database.EstablishConnection(database.ConnStr)
	
	defer DB.Close()

	if err != nil {
		log.Print(err)
		return 
	}

	createEnum := `
	DO $$ BEGIN
		CREATE TYPE submission_status AS ENUM ('PENDING', 'RUNNING', 'ACCEPTED', 'WRONG_ANSWER', 'COMPILE_ERROR', 'TIME_LIMIT_EXCEEDED', 'RUNTIME_ERROR');
		EXCEPTION 
			WHEN duplicate_object THEN null; 
	END $$;`

	database.Query(DB, createEnum)
	
	createDatabase := `
	CREATE TABLE IF NOT EXISTS code_submissions(
		id SERIAL PRIMARY KEY,
		user_id INT NOT NULL,
		problem_id INT NOT NULL,
		language VARCHAR(20) NOT NULL,
		code_payload TEXT NOT NULL,
		status submission_status NOT NULL,
		stdout TEXT,
		stderr TEXT,
		execution_time INT,
		memory_used INT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);
	`

	database.Query(DB, createDatabase)

	createUser := `
		CREATE TABLE IF NOT EXISTS users(
		id SERIAL PRIMARY KEY,
		username VARCHAR(50),
		password VARCHAR(50),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	database.Query(DB, createUser)
}
