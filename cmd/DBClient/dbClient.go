package main

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"context"
	"flag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"log"
	"strconv"
	"time"
)

var (
	addr = flag.String("addr", "localhost:50070", "the address of client Node")

	//TODO replace this place and use round robin to select server later
	server = flag.String("ser", "localhost:50031", "the address of connected server")
)

const (
	TIMEOUT = 120
)

type DbClient struct {
	nodeinfo pb.NodeInfo
}

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
		writes = append(writes, &pb.WriteOp{
			Key: keys[i],
			Val: vals[i],
		})
	}
	return writes
}
func generateTrans(rKeys []string, wKeys []string, wVals []string, clt *pb.NodeInfo) *pb.Trans {
	reads := generateRead(rKeys)
	writes := generateWrite(wKeys, wVals)
	return &pb.Trans{

		Reads:      reads,
		Writes:     writes,
		St:         pb.TranStatus_New,
		ClientInfo: clt,
	}
}

// Test cases
func generateRandomTrans(clt *pb.NodeInfo) *pb.Trans {
	var rKeys []string
	var wKeys []string
	var wVals []string
	for i := 0; i < 2; i++ {
		rKeys = append(rKeys, "rk"+strconv.Itoa(i))
		wKeys = append(wKeys, "rk"+strconv.Itoa(i))
		wVals = append(wVals, "val "+strconv.Itoa(i))
	}
	return generateTrans(rKeys, wKeys, wVals, clt)
}

// TODO considering receiving the results, Or we need to wait on another channel
// TODO because maybe not the same node response
// TODO discuss
func sendMsg(data []byte, tar pb.CoordinateClient) {
	msg := pb.Message{
		Type: pb.MsgType_SubmitTrans,
		Data: data,
		From: 1000,
		To:   0,
	}
	//TODO if we could reuse the context?
	// TODO when should we use the cancel? The second return from the following code!!
	ctx, _ := context.WithTimeout(context.Background(), TIMEOUT*time.Second)
	_, err := tar.SendReq(ctx, &msg)
	if err != nil {
		// TODO we may need extra info??
		log.Fatal("Sending failed")
		return
	}
	log.Printf("Received the resp")
	return
}

// Just a helper function which helps to test
func getServerClient(serAddr string) pb.CoordinateClient {
	conn, err := grpc.Dial(serAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v which add is %s", err, server)
	}
	return pb.NewCoordinateClient(conn)
}

// TODO optimize the client to be thread safe
func main() {
	flag.Parse()
	clt := &DbClient{nodeinfo: pb.NodeInfo{Addr: *addr}}
	rdTrans := generateRandomTrans(&clt.nodeinfo)
	rawMsg, _ := proto.Marshal(rdTrans)
	ser := getServerClient(*server)
	sendMsg(rawMsg, ser)

}
