package database

import (
	"database/sql"
	_ "github.com/lib/pq"
	"log"
	"os"
)

var ConnStr = func() string {
	if v := os.Getenv("DB_CONN_STR"); v != "" {
		return v
	}
	return "host=localhost port=5432 user=javaboii dbname=online_judge password=supersecretpassword sslmode=disable"
}()

func EstablishConnection(connStr string) (*sql.DB, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	// FORCE A REAL NETWORK HANDSHAKE
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func Query(DB *sql.DB, query string) (*sql.Rows, error) {
	row, err := DB.Query(query)

	if err != nil {
		log.Print(err)
		return row, err
	}

	return row, nil
}

func Exec(DB *sql.DB, query string, args ...interface{}) error {
	_, err := DB.Exec(query, args...)

	if err != nil {
		log.Print(err)
		return err
	}

	return nil
}
