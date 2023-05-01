package main

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"flag"
)

var (
	addr = flag.String("addr", "localhost:50070", "the address of client Node")

	//TODO replace this place and use round robin to select server later
	server = flag.String("ser", "localhost:50030", "the address of connected server")
)

// TODO get transaction from command input

func generateRead(keys []string) []*pb.ReadOp {
	var reads []*pb.ReadOp
	for i := 0; i < len(keys); i++ {
		reads = append(reads, &pb.ReadOp{Key: keys[i]})
	}
	return reads
}
func generateWrite(keys []string, vals []string) []*pb.WriteOp {
	var writes []*pb.WriteOp
	for i := 0; i < len(keys); i++ {
		writes = append(writes, &pb.WriteOp{Key: keys[i]})
	}
}
func main() {

}
