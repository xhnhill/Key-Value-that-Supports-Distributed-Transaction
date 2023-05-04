package main

import (
	"Distributed_Key_Value_Store/cmd/gRPCService"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"time"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
	// name = flag.String("name", defaultName, "Name to greet")
)

func basic_syn() {
	flag.Parse()
	// Later we will read in the config file and connect to each node
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	client := gRPCService.NewGetHeartBeatClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ts := &timestamppb.Timestamp{
		Seconds: time.Now().Unix(),
		Nanos:   int32(time.Now().Nanosecond()),
	}
	r, err := client.GetTimestamp(ctx, ts)
	if err != nil {
		log.Fatalf(" Could not process: %v", err)
	}
	log.Printf("Greeting: %s", r.AsTime().Format("2006-01-02 15:04:05.999999999 -0700 MST"))

}
func dial(clt gRPCService.GetHeartBeatClient, tm *timestamppb.Timestamp, c chan *timestamppb.Timestamp) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	res, err := clt.GetTimestamp(ctx, tm)
	if err != nil {
		log.Fatalf("Dial failed: %v", err)
	}
	c <- res
}
func basic_asyn() {
	flag.Parse()
	// Later we will read in the config file and connect to each node
	var adds []string
	for i := 51; i <= 53; i++ {
		adds = append(adds, fmt.Sprintf("localhost:500%d", i))
	}
	// var conns []*grpc.ClientConn
	var cls []gRPCService.GetHeartBeatClient
	ts := &timestamppb.Timestamp{
		Seconds: time.Now().Unix(),
		Nanos:   int32(time.Now().Nanosecond()),
	}
	for _, add := range adds {
		conn, err := grpc.Dial(add, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect: %v which add is %s", err, add)
		}
		defer conn.Close()
		cls = append(cls, gRPCService.NewGetHeartBeatClient(conn))
	}
	c := make(chan *timestamppb.Timestamp)
	for i, clt := range cls {
		log.Printf("Try to dial %d", i)
		go dial(clt, ts, c)
	}
	st := time.Now()
	tar := st.Add(time.Second)
	for i := 0; i < len(cls); i++ {
		sub := tar.Sub(time.Now())
		if sub < 0 {
			sub = 0 * time.Second
		}
		select {
		case r := <-c:
			log.Printf("Greeting: %s", r.AsTime().Format("2006-01-02 15:04:05.999999999 -0700 MST"))
		case <-time.After(sub):
			log.Printf("Timeout in idx %d", i)
		}

	}
	log.Printf("Finished.")

}
func main() {
	basic_asyn()
}
