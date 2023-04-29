package main

import (
	"Distributed_Key_Value_Store/cmd/gRPCService"
	"context"
	"flag"
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

func main() {
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
	log.Printf("Greeting: %s", r.GetMessage())

}
