package main

import (
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/ingestion"
	"github.com/gorilla/mux"
	"github.com/segmentio/kafka-go"

	"net/http"
	"io"
	"strconv"
	"encoding/json"
	"log"
)

type Submit struct {
	Id int `json:"id"`
	Submission string `json:"submission"`
}

func main() {
	r := mux.NewRouter() 
	writer := ingestion.GetWriter("submissions")
	defer writer.Close()

	r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/submit", SubmitHandler(writer)).Methods("POST")

	log.Fatal(http.ListenAndServe(":8080", r))
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	_, err := w.Write([]byte("Welcome to My first Go server"))

	if err != nil {
		http.Error(w, "Failed to serve request", http.StatusInternalServerError)
	}
}

func SubmitHandler(writer *kafka.Writer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		reqBody, err := io.ReadAll(r.Body)

		if err != nil {
			http.Error(w, "Failed to read request", http.StatusBadRequest)
			return 
		}

		var submission Submit
		json.Unmarshal(reqBody, &submission)

		idString := strconv.Itoa(submission.Id)

		message := []kafka.Message{
			kafka.Message{
				Key: []byte(idString),
				Value: []byte(submission.Submission),
			},
		}
		err = ingestion.WriteMessage(writer, message)

		if err != nil {
			log.Print("Error is", err)
			http.Error(w, "Failed to persit message in event pipeline", http.StatusInternalServerError)
			return 
		}

		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte("Operation Completed Successfully"))
	}
}
