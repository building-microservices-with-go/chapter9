package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"time"

	memdb "github.com/hashicorp/go-memdb"
	"github.com/nats-io/nats"
)

type productInsertedEvent struct {
	Name string `json:"name"`
	SKU  string `json:"sku"`
}

type product struct {
	Name        string `json:"name"`
	Code        string `json:"code"`
	LastUpdated string `json:"last_updated"`
}

func (p product) FromProductInsertedEvent(e productInsertedEvent) product {
	p.Name = e.Name
	p.Code = e.SKU
	p.LastUpdated = time.Now().Format(time.RFC3339)

	return p
}

var schema *memdb.DBSchema
var db *memdb.MemDB

var natsClient *nats.Conn

var natsServer = flag.String("nats", "", "NATS server URI")

func init() {
	flag.Parse()

	schema = &memdb.DBSchema{
		Tables: map[string]*memdb.TableSchema{
			"product": &memdb.TableSchema{
				Name: "product",
				Indexes: map[string]*memdb.IndexSchema{
					"id": &memdb.IndexSchema{
						Name:    "id",
						Unique:  true,
						Indexer: &memdb.StringFieldIndex{Field: "SKU"},
					},
				},
			},
		},
	}

	err := schema.Validate()
	if err != nil {
		log.Fatal(err)
	}

	db, err = memdb.NewMemDB(schema)
	if err != nil {
		log.Fatal(err)
	}

	natsClient, err = nats.Connect("nats://" + *natsServer)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	log.Println("Subscribing to events")
	natsClient.Subscribe("product.inserted", func(m *nats.Msg) {
		log.Println("New event")
		productMessage(m)
	})

	http.DefaultServeMux.HandleFunc("/product", getProducts)

	log.Println("Starting product read service on port 8081")
	log.Fatal(http.ListenAndServe(":8081", http.DefaultServeMux))
}

func getProducts(rw http.ResponseWriter, r *http.Request) {
	log.Println("/get handler called")

	txn := db.Txn(false)
	results, err := txn.Get("product", "id")
	if err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}

	products := make([]product, 0)
	for {
		obj := results.Next()
		if obj == nil {
			break
		}

		products = append(products, obj.(product))
	}

	encoder := json.NewEncoder(rw)
	encoder.Encode(products)
}

func productMessage(m *nats.Msg) {
	pie := productInsertedEvent{}
	err := json.Unmarshal(m.Data, &pie)
	if err != nil {
		log.Println("Unable to unmarshal event object")
		return
	}

	p := product{}.FromProductInsertedEvent(pie)

	txn := db.Txn(true)
	if err := txn.Insert("product", p); err != nil {
		log.Println(err)
		return
	}
	txn.Commit()

	log.Println("Saved product: ", p)
}
