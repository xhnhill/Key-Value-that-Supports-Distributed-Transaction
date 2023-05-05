package main

import (
	badger "github.com/dgraph-io/badger/v4"
	"log"
)

func main() {
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	if err != nil {
		log.Fatal(err)
	}
	db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte("hello1"), []byte("fine1"))
		if err != nil {
			return err
		}
		return nil
	})
	var items []*badger.Item
	db.View(func(txn *badger.Txn) error {
		// Your code here…
		item, err := txn.Get([]byte("hello1"))
		if err != nil {
			return err
		}
		items = append(items, item)
		return nil
	})

	defer db.Close()

}
