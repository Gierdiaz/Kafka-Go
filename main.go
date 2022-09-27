package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

func main() {
	//PRODUCER:
	writer := &kafka.Writer{
		Addr:    kafka.TCP("localhost:9092"),
		Topic: "quickstart",
	}

	err := writer.WriteMessages(context.Background(), kafka.Message{
		Value: []byte("mensagem"),
		Headers: []protocol.Header {
			{
				Key: "session",
				Value: []byte("3373"),
			},
		},
	})

	if err != nil {
		log.Fatal("cannot write a message", err)
	}

	//CONSUMER
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		GroupID: "consumer",
		Topic: "quickstart",
		MinBytes: 0,
		MaxBytes: 10e6, //10MB
	})

	for i := 0; i < 1; i++ {
		message, err := reader.ReadMessage(context.Background())

		for _, val := range message.Headers {
			if (val.Key == "session" && string(val.Value) == "3373") {
				fmt.Println("sessão correta")
			}
		}

		if err != nil {
			log.Fatal("cannot receive a message: ", err)
			reader.Close()
		}

		fmt.Print("receive a message ", string(message.Value))
	}

	reader.Close()
}