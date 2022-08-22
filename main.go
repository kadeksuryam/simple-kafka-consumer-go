package main

import (
	"context"
	"fmt"
	"os"

	"github.com/segmentio/kafka-go"

	"encoding/base64"
)

func consume(ctx context.Context) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{os.Args[1]},
		Topic:       string(os.Args[2]),
		Partition:   3,
		StartOffset: 384636,
	})

	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			panic("Failed to read the message")
		}

		if msg.Offset == 384637 {
			LogBase64(msg)
			break
		}
	}
}

func toBase64(value []byte) string {
	return base64.StdEncoding.EncodeToString(value)
}

func LogBase64(msg kafka.Message) {
	// Metadata
	fmt.Printf("Partition: %d", msg.Partition)
	fmt.Println()
	fmt.Printf("Offset: %d", msg.Offset)
	fmt.Println()
	fmt.Printf("Headers: %s", msg.Headers)
	fmt.Println()

	// Data
	fmt.Printf("Key: %s", toBase64(msg.Key))
	fmt.Println()
	fmt.Printf("Value: %s", toBase64(msg.Value))
	fmt.Println()
}

func main() {
	ctx := context.Background()
	consume(ctx)
}
