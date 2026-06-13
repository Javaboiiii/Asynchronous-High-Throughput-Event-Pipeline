package main 

import(
	"github.com/Javaboiiii/Asynchronous-High-Throughput-Event-Pipeline/internal/broker"
)

func main() {
	reader := broker.GetReader("submissions")

	defer reader.Close()

	broker.ProcessMessages(reader)
}
