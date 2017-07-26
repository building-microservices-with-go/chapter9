package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/nicholasjackson/building-microservices-in-go/chapter9/queue"
)

type Product struct {
	SKU  string `json:"sku"`
	Name string `json:"name"`
}

func main() {
	q, err := queue.NewRedisQueue("redis:6379", "test_queue")
	if err != nil {
		log.Fatal(err)
	}

	http.HandleFunc("/", func(rw http.ResponseWriter, r *http.Request) {
		data, _ := ioutil.ReadAll(r.Body)
		err := q.Add("new.product", data)
		if err != nil {
			log.Println(err)
			rw.WriteHeader(http.StatusInternalServerError)
			return
		}
	})

	http.ListenAndServe(":8080", http.DefaultServeMux)
}
