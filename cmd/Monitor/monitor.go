package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"log"
	"os"
	"strings"
)

var (
	numDB      = flag.Int("numDB", 6, "Num of monitored local db")
	pathPrefix = flag.String("pathPrefix", "tmp/", "Prefix of the db")
)

// Author: Haining Xie
// Monitor the status of local db
func printResMap(resMap map[int][]byte) {
	for k, v := range resMap {
		log.Printf("From db%d , get val as %s", k, v)
	}

}
func main() {

	var dbs []*badger.DB
	for i := 1; i <= *numDB; i++ {
		dbPath := fmt.Sprintf("%s%d", *pathPrefix, i)
		db, err := badger.Open(badger.DefaultOptions(dbPath))
		if err != nil {
			log.Fatalf("Failed to open DB at %s: %v", dbPath, err)
		}
		defer db.Close()

		dbs = append(dbs, db)
	}
	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("Enter the Key: ")
		scanner.Scan()
		input := strings.TrimSpace(scanner.Text())

		results := make(map[int][]byte)
		for idx, db := range dbs {
			err := db.View(func(txn *badger.Txn) error {
				item, err := txn.Get([]byte(input))
				if err != nil {
					return err
				}
				val, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}

				results[idx+1] = val
				return nil
			})
			if err != nil {
				log.Printf("Failed to read key from DB %d: %v", idx+1, err)
			}
		}
		printResMap(results)

		if input == "quit" {
			break
		}
	}

}
