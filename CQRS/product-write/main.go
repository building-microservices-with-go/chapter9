package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	memdb "github.com/hashicorp/go-memdb"
	nats "github.com/nats-io/go-nats"
)

type product struct {
	Name       string `json:"name"`
	SKU        string `json:"sku"`
	StockCount int    `json:"stock_count"`
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

	txn := db.Txn(true)

	if err := txn.Insert("product", product{"Test1", "ABC232323", 100}); err != nil {
		log.Fatal(err)
	}

	if err := txn.Insert("product", product{"Test2", "ABC883388", 100}); err != nil {
		log.Fatal(err)
	}

	txn.Commit()

	natsClient, err = nats.Connect("nats://" + *natsServer)
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	http.DefaultServeMux.HandleFunc("/product", productsHandler)
	http.DefaultServeMux.HandleFunc("/stock", stockHandler)

	log.Println("Starting product write service on port 8080")
	log.Fatal(http.ListenAndServe(":8080", http.DefaultServeMux))
}

func productsHandler(rw http.ResponseWriter, r *http.Request) {
	if r.Method == "POST" {
		insertProduct(rw, r)
	}
}

func stockHandler(rw http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	txn := db.Txn(false)
	obj, err := txn.First("product", "id", id)
	if err != nil {
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	p := obj.(product)
	fmt.Fprintf(rw, `{"quantity": %v}`, p.StockCount)
}

func insertProduct(rw http.ResponseWriter, r *http.Request) {
	log.Println("/insert handler called")

	p := &product{}

	data, err := ioutil.ReadAll(r.Body)
	if err != nil {
		rw.WriteHeader(http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = json.Unmarshal(data, p)
	if err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusBadRequest)
		return
	}

	txn := db.Txn(true)
	if err := txn.Insert("product", p); err != nil {
		log.Println(err)
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
	txn.Commit()

	natsClient.Publish("product.inserted", data)
}
