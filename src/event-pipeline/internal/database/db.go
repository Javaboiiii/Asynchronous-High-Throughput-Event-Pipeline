package database 

import (
	"log"
	"database/sql"
	_ "github.com/lib/pq"
)


const ConnStr string = "user=javaboii dbname=online_judge password=supersecretpassword sslmode=disable"

func EstablishConnection(connStr string) (*sql.DB, error) {
	return sql.Open("postgres", connStr)
}

func Query(DB *sql.DB, query string) (*sql.Rows, error) {
	row, err := DB.Query(query)

	if err != nil {
		log.Print(err)
		return row, err
	}

	return row, nil 
}

func Exec(DB *sql.DB, query string, args ...interface{}) (error) {
	_, err := DB.Exec(query, args...)

	if err != nil {
		log.Print(err)
		return err
	}

	return nil 
}
