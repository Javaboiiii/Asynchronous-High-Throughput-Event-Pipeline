package broker 

import(
	"github.com/segmentio/kafka-go"

	"fmt"
	"context"
	"log"
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

func ProcessMessages(reader *kafka.Reader) {
	fmt.Println("Worker engine is online")
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Print("Not able to read Messages")
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}
}
