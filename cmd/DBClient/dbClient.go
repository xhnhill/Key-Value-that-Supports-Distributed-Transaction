package main

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"flag"
)

import (
	"context"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"log"
	"net"
	"strconv"
	"sync"
	"time"
)

// Author: Haining Xie
// Used to test massive concurrent operations on cluster
var (
	addr = flag.String("addr", "localhost:50072"+
		"", "the address of client Node")

	server = flag.String("ser", "localhost:50032", "the address of connected server")
)

const (
	TIMEOUT = 120
)

type DbClient struct {
	nodeinfo pb.NodeInfo
}

func generateRead(keys []string) []*pb.ReadOp {
	reads := make([]*pb.ReadOp, 0, 3)
	for i := 0; i < len(keys); i++ {
		reads = append(reads, &pb.ReadOp{Key: keys[i]})
	}
	return reads
}
func generateWrite(keys []string, vals []string) []*pb.WriteOp {
	writes := make([]*pb.WriteOp, 0, 3)
	for i := 0; i < len(keys); i++ {
		writes = append(writes, &pb.WriteOp{
			Key: keys[i],
			Val: vals[i],
		})
	}
	return writes
}
func genUUID() string {
	id := uuid.New()
	return id.String()

}
func generateTrans(rKeys []string, wKeys []string, wVals []string, clt *pb.NodeInfo) *pb.Trans {
	reads := generateRead(rKeys)
	writes := generateWrite(wKeys, wVals)
	return &pb.Trans{
		CId:        genUUID(),
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
		wVals = append(wVals, "val "+strconv.Itoa(i)+genUUID())
		wKeys = append(wKeys, "rk"+genUUID())
		wVals = append(wVals, "val "+genUUID())
	}
	return generateTrans(rKeys, wKeys, wVals, clt)
}
func generateFixedTrans(clt *pb.NodeInfo) *pb.Trans {
	var rKeys []string
	wKeys := make([]string, 0, 1)
	wVals := make([]string, 0, 1)
	for i := 0; i < 1; i++ {
		rKeys = append(rKeys, "rk"+strconv.Itoa(i))
	}
	return generateTrans(rKeys, wKeys, wVals, clt)
}

func sendMsg(data []byte, tar pb.CoordinateClient) {
	msg := pb.Message{
		Type: pb.MsgType_SubmitTrans,
		Data: data,
		From: 1000,
		To:   0,
	}

	ctx, _ := context.WithTimeout(context.Background(), TIMEOUT*time.Second)
	_, err := tar.SendReq(ctx, &msg)
	if err != nil {

		log.Fatal("Sending failed, %v", err)
		return
	}
	//log.Printf("Received the resp")
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

type cltServer struct {
	pb.UnimplementedCoordinateServer
	transMap map[string]chan []*pb.SingleResult
	mu       sync.Mutex
}

func convertRes2Str(res []*pb.SingleResult) string {
	str := ""
	for i := 0; i < len(res); i++ {
		str = str + "key: " + res[i].Key + " Val: " + res[i].Val + "\n"
	}
	return str
}
func (s *cltServer) blockRead(trans *pb.Trans, clt pb.CoordinateClient) {
	rawMsg, _ := proto.Marshal(trans)
	sendMsg(rawMsg, clt)
	s.mu.Lock()
	waitCh := make(chan []*pb.SingleResult, 1)
	s.transMap[trans.CId] = waitCh
	s.mu.Unlock()
	res := <-waitCh
	log.Printf(convertRes2Str(res))
}
func (s *cltServer) SendReq(ctx context.Context, in *pb.Message) (*emptypb.Empty, error) {
	finalRes := &pb.FinalRes{}
	proto.Unmarshal(in.Data, finalRes)
	cId := finalRes.CId
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := s.transMap[cId]
	ch <- finalRes.Res
	return &emptypb.Empty{}, nil

}
func (localServer *cltServer) MassiveConcurrent(clt *DbClient, ser pb.CoordinateClient) {
	rdTrans := generateRandomTrans(&clt.nodeinfo)
	localServer.blockRead(rdTrans, ser)
}

// Fixed version read
func (localServer *cltServer) fixedRead(clt *DbClient, ser pb.CoordinateClient) {
	rdTrans := generateFixedTrans(&clt.nodeinfo)
	localServer.blockRead(rdTrans, ser)
}
func (localServer *cltServer) concurrentOp(clt *DbClient, ser pb.CoordinateClient) {
	for i := 0; i < 5; i++ {
		go localServer.MassiveConcurrent(clt, ser)
	}
	time.Sleep(300 * time.Second)
}

func main() {
	flag.Parse()
	//Start receiving server
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on client: %v", err)
	}
	localServer := &cltServer{
		transMap: make(map[string]chan []*pb.SingleResult),
		mu:       sync.Mutex{},
	}
	s := grpc.NewServer()
	pb.RegisterCoordinateServer(s, localServer)
	log.Printf("server listening at %v", lis.Addr())
	f := func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}
	go f()
	// calling part
	clt := &DbClient{nodeinfo: pb.NodeInfo{Addr: *addr}}
	ser := getServerClient(*server)
	localServer.concurrentOp(clt, ser)
	//localServer.fixedRead(clt, ser)

}
