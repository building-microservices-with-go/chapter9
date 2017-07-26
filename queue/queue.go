package queue

import (
	"time"
)

// Message represents messages stored on the queue
type Message struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

// Queue defines the interface for a message queue
type Queue interface {
	Add(messageName string, payload []byte) error
	AddMessage(message Message) error
	StartConsuming(size int, pollInterval time.Duration, callback func(Message) error)
}
