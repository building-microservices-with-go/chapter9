package main

import (
	"log"
	"runtime"
	"time"

	"github.com/nicholasjackson/building-microservices-in-go/chapter9/queue"
)

func main() {
	log.Println("Starting worker")

	q, err := queue.NewRedisQueue("redis:6379", "test_queue")
	if err != nil {
		log.Fatal(err)
	}

	q.StartConsuming(10, 100*time.Millisecond, func(message queue.Message) error {
		log.Printf("Received message: %v, %v, %v\n", message.ID, message.Name, message.Payload)

		return nil // successfully processed message
	})

	runtime.Goexit() // avoid main from exiting untill all other go routines have completed
}
