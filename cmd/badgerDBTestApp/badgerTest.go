package main

import (
	badger "github.com/dgraph-io/badger/v4"
	"log"
)

func main() {
	db, err := badger.Open(badger.DefaultOptions("tmp/test"))
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

}
