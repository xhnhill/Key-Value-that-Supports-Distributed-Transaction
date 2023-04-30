package Replica

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"bufio"
	"flag"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	path       = flag.String("path", "config", "the path to config file")
	selfConfig = flag.String("self", "self", "the path to self configuration file")
	mode       = flag.Bool("mode", false, "True indicates in the debug mode")
)

type Node struct {
	nodeId int
	addr   string
	client pb.CoordinateClient
}
type Server struct {
	node  *Node
	inCh  chan *pb.Request
	outCh chan *pb.Response
}

func connectCluster() {

}

func readNodeConfig() (int, string, int, error) {
	nodeFile, err := os.Open(*selfConfig)
	if err != nil {
		log.Fatalf("Error to load node configuration file.")
		return 0, "", 0, err
	}
	defer nodeFile.Close()
	scanner := bufio.NewScanner(nodeFile)
	lineNumber := 0
	var nodeId int
	var ip string
	var port int
	for scanner.Scan() {
		line := scanner.Text()
		if lineNumber == 0 {
			nodeId, _ = strconv.Atoi(line)
		} else {
			parts := strings.Split(line, ":")
			ip = parts[0]
			port, _ = strconv.Atoi(parts[1])
		}
		lineNumber++
	}
	return nodeId, ip, port, nil
}
func readClusterConfig() ([]Node, error) {
	// The format of config file:
	// repeated nodeId-Ip:port
	config, err := os.Open(*path)
	if err != nil {
		log.Fatalf("Error to load cluster configuration file.")
		return nil, err
	}
	defer config.Close()
	scanner := bufio.NewScanner(config)
	lineNumber := 0
	var nodes []Node
	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, "-")
		nodeId, _ := strconv.Atoi(parts[0])
		nodes = append(nodes, Node{
			nodeId: nodeId,
			addr:   parts[1],
		})
		lineNumber++
	}
	return nodes, nil
}

func startTicker(inCh chan *pb.Request) *time.Ticker {
	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-ticker.C:
			tickMsg := pb.TickMsg{
				TimeStamp: &timestamppb.Timestamp{
					Seconds: time.Now().Unix(),
					Nanos:   int32(time.Now().Nanosecond()),
				},
			}
			data, _ := proto.Marshal(&tickMsg)
			req := pb.Request{
				Type: pb.ReqType_Tick,
				Data: data,
			}
			inCh <- &req
		}
	}

}

func createClients(nodes []Node) []Node {
	for i := 0; i < len(nodes); i++ {
		conn, err := grpc.Dial(nodes[i].addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("Node %d may fail, could not connect", nodes[i].nodeId)
		} else {
			nodes[i].client = pb.NewCoordinateClient(conn)
		}

	}
	return nodes
}

// TODO close the connections
func main() {
	flag.Parse()
	nodeId, ip, port, err := readNodeConfig()
	if err != nil {
		return
	}
	log.Printf("Node %d, begin to load and address is %s"+": %d", nodeId, ip, port)
	cur := &Node{
		nodeId: nodeId,
		addr:   ip + strconv.Itoa(port),
	}

	nodes, err := readClusterConfig()
	if err != nil {
		return
	}
	localServer := &Server{
		node:  cur,
		inCh:  make(chan *pb.Request, 0),
		outCh: make(chan *pb.Response, 0),
	}
	// Start the timer here
	if !*mode {
		go startTicker(localServer.inCh)
	}
	nodes = createClients(nodes)

}
