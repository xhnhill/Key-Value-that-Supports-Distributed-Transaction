package test

import (
	"log"
	"testing"
)

type Node struct {
	nodeId int
	addr   string
	cmt    string
}

func addConn(nodes []Node) []Node {
	// Pay attention to the for range structure
	// which pass the value instead the original data
	for i := 0; i < len(nodes); i++ {
		nodes[i].cmt = "xhn"
	}
	return nodes
}

func TestArgPass(t *testing.T) {
	var nodes []Node
	for i := 0; i < 5; i++ {
		nodes = append(nodes,
			Node{
				nodeId: i,
				addr:   "hi",
			})
	}
	newN := addConn(nodes)

	log.Printf("Finshed %d", len(newN))
}
