package main

import (
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/ingestion"
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/controllers"
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/database"
	"github.com/gorilla/mux"

	"net/http"
	"log"
)

func main() {
	
	r := mux.NewRouter() 
	writer := ingestion.GetWriter("submissions")
	db, err := database.EstablishConnection(database.ConnStr)
	if err != nil {
		log.Print("Error is", err)
		return ; 
	}
	defer writer.Close()

	r.HandleFunc("/", HomeHandler)
	r.HandleFunc("/submit", controllers.SubmitHandler(db, writer)).Methods("POST")

	log.Fatal(http.ListenAndServe(":8080", r))
}

func HomeHandler(w http.ResponseWriter, r *http.Request) {
	_, err := w.Write([]byte("Welcome to My first Go server"))

	if err != nil {
		http.Error(w, "Failed to serve request", http.StatusInternalServerError)
	}
}


