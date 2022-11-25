package main

import (
	"context"
	"fmt"
	"log"

	"github.com/kecci/go-kafka-zookeeper/utility"
	"github.com/segmentio/kafka-go"
)

func main() {
	// make a new reader that consumes from topic-A
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     utility.KAFKA_BROKERS,
		GroupTopics: utility.KAFKA_TOPICS,
		GroupID:     "consumer-group-id",
		MinBytes:    10e3, // 10KB
		MaxBytes:    10e6, // 10MB
	})
	defer r.Close()

	go func() {
		for {
			m, err := r.ReadMessage(context.Background())
			if err != nil {
				break
			}
			fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-make(chan (bool))
}
