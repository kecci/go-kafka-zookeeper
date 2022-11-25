package main

import (
	"context"
	"log"

	"github.com/kecci/go-kafka-zookeeper/utility"
	"github.com/segmentio/kafka-go"
)

func main() {
	// Creating topic if not exist
	if err := CreateTopicsForBrokers(utility.KAFKA_BROKERS, utility.KAFKA_TOPICS); err != nil {
		panic(err.Error())
	}

	// make a writer that produces to topic-A, using the least-bytes distribution
	w := &kafka.Writer{
		Addr: kafka.TCP(utility.KAFKA_BROKERS...),
		// NOTE: When Topic is not defined here, each Message must define it instead.
		// Topic:    "topic-A",
		Balancer: &kafka.LeastBytes{},
	}

	err := w.WriteMessages(context.Background(),
		// NOTE: Each Message has Topic defined, otherwise an error is returned.
		kafka.Message{
			Topic: "topic-A",
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Topic: "topic-B",
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Topic: "topic-C",
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

// CreateTopicsForBrokers brokers and topics
func CreateTopicsForBrokers(brokers []string, topics []string) error {
	for _, broker := range brokers {
		for _, topic := range topics {
			// to create topics when auto.create.topics.enable='true'
			conn, err := kafka.DialLeader(context.Background(), "tcp", broker, topic, 0)
			if err != nil {
				return err
			}
			defer conn.Close()
		}
	}
	return nil
}
