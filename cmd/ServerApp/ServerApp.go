package main

import (
	"Distributed_Key_Value_Store/cmd/gRPCService"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"net"
	"time"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

type server struct {
	gRPCService.UnimplementedGetHeartBeatServer
}

func (s *server) GetTimestamp(ctx context.Context, in *timestamppb.Timestamp) (*timestamppb.Timestamp, error) {
	if *port == 50053 {
		time.Sleep(2 * time.Second)
	}
	log.Printf("Received: %s", in.AsTime().Format("2006-01-02 15:04:05.999999999 -0700 MST"))
	return &timestamppb.Timestamp{
		Seconds: time.Now().Unix(),
		Nanos:   int32(time.Now().Nanosecond()),
	}, nil
}
func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	gRPCService.RegisterGetHeartBeatServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
