package ingestion

import (
	"context"
	"errors"
	"time"

	"github.com/segmentio/kafka-go"
)

func GetWriter(topic string) *kafka.Writer {
	w := &kafka.Writer{
		Addr:     kafka.TCP("localhost:29092", "localhost:39092", "localhost:49092"),
		Topic:    topic,
		Balancer: &kafka.Hash{},
	}
	return w
}

func WriteMessage(w *kafka.Writer, messages []kafka.Message) error {
	const retries = 3
	var err error

	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = w.WriteMessages(ctx, messages...)
		cancel()

		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			return err
		}
		return nil 
	}
	return err 
}
