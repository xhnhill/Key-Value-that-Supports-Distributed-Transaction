package main

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"bufio"
	"context"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	path       = flag.String("path", "config", "the path to config file")
	selfConfig = flag.String("self", "self", "the path to self configuration file")
	shardsNum  = flag.Int("shards", 2, "Sharding num")
	mode       = flag.Bool("mode", false, "True indicates in the debug mode")
)

var msgTypeToString = map[pb.MsgType]string{
	pb.MsgType_PreAccept:   "PreAccept",
	pb.MsgType_Accept:      "Accept",
	pb.MsgType_Commit:      "Commit",
	pb.MsgType_Read:        "Read",
	pb.MsgType_Apply:       "Apply",
	pb.MsgType_Recover:     "Recover",
	pb.MsgType_Tick:        "Tick",
	pb.MsgType_PreAcceptOk: "PreAcceptOk",
	pb.MsgType_AcceptOk:    "AcceptOk",
	pb.MsgType_CommitOk:    "CommitOk",
	pb.MsgType_ReadOk:      "ReadOk",
	pb.MsgType_ApplyOk:     "ApplyOk",
	pb.MsgType_RecoverOk:   "RecoverOk",
	pb.MsgType_SubmitTrans: "SubmitTrans",
	pb.MsgType_HeartBeat:   "HeartBeat",
}

type Node struct {
	nodeId int
	addr   string
	client pb.CoordinateClient
}
type Server struct {
	node  *Node
	inCh  chan *pb.Message
	outCh chan *pb.Message

	peers map[int]*Node // Connections to peers
	//clients are indexed by addrs
	clients map[string]*Node // Connected clients TODO maybe lazy deletion about non connected one?
	pb.UnimplementedCoordinateServer
	stateMachine *StateMachine
}

// Easy function ,just put received raw msg into input channel
// TODO how will the context object be used?
func (s *Server) SendReq(ctx context.Context, in *pb.Message) (*emptypb.Empty, error) {
	s.inCh <- in
	return &emptypb.Empty{}, nil

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
		log.Fatalf("Error to load cluster configuration file %v.", err)
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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Allocate shards
func (ser *Server) generateShards(n int) {
	// Generate array full of bytes, n indicates the array length
	arr := make([]byte, n)
	for i := 0; i < n; i++ {
		arr[i] = 0xFF
	}
	var shards []*pb.ShardInfo
	x := new(big.Int).SetBytes(arr)
	q := new(big.Int).Div(x, big.NewInt(int64(*shardsNum)))
	sum := big.NewInt(0)
	nodesNum := len(ser.peers)
	unit := nodesNum / (*shardsNum)
	for i := 0; i < *shardsNum; i++ {
		var replicas []int32
		for j := i * unit; j < min((i+1)*unit, nodesNum); j++ {
			replicas = append(replicas, int32(j))
		}
		if i == n-1 {

		}
		st := sum.Bytes()
		sum.Add(sum, q)
		ed := sum.Bytes()
		if i == n-1 {
			ed = arr
		}
		shard := pb.ShardInfo{
			Start:    st,
			End:      ed,
			ShardId:  int32(i),
			Replicas: replicas,
		}
		shards = append(shards, &shard)
	}
	ser.stateMachine.shards = shards
	ser.stateMachine.curShard = shards[ser.node.nodeId/(*shardsNum)]

}

func (ser *Server) startTicker(inCh chan *pb.Message) *time.Ticker {
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
			req := pb.Message{
				Type: pb.MsgType_Tick,
				Data: data,
			}
			inCh <- &req
			log.Printf("Send Tick Msg on Node %d", ser.node.nodeId)
		}
	}

}

// Create clients for peer nodes
func createClients(nodes []Node) []Node {

	for i := 0; i < len(nodes); i++ {
		for j := 0; j < 5; j++ {
			conn, err := grpc.Dial(nodes[i].addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				log.Printf("Node %d may fail, try again", nodes[i].nodeId)
				time.Sleep(10 * time.Second)
			} else {
				nodes[i].client = pb.NewCoordinateClient(conn)
				log.Printf("Node %d connect successful", nodes[i].nodeId)
				break
			}
		}

	}
	return nodes
}
func (ser *Server) updatePeerClients(nodes []Node) {
	for i := 0; i < len(nodes); i++ {
		ser.peers[nodes[i].nodeId] = &nodes[i]
	}
}

// Add user client (in case to respond results)
// TODO consider about try to connect to clients also in recovery if the original coordinator failed
func (ser *Server) createAndConnUserClient(userInfo *pb.NodeInfo) {
	conn, err := grpc.Dial(userInfo.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Could not connect to user client %s", userInfo.Addr)
		return
	}
	userClient := pb.NewCoordinateClient(conn)
	user := Node{
		addr:   userInfo.Addr,
		nodeId: 1000,
		client: userClient,
	}
	ser.clients[user.addr] = &user

}

// function which is responsible for send out gRPC request
// Run in a single go routine, but will start lots of other goroutine
// TODO check if it is thread safe to use ser.peers here
func (ser *Server) performRPC() {
	for {
		rpc, ok := <-ser.outCh
		if !ok {
			log.Fatalf("output channel closed on server %d", ser.node.nodeId)
			return
		}
		tarNodeId := int(rpc.To)
		// TODO maybe we need read write lock here?
		tarNode, exist := ser.peers[tarNodeId]
		if exist {
			// TODO maybe need to wrap the sending, in case sending fail
			// TODO anything we need to add to context?
			f := func(node *Node, msg *pb.Message) {
				log.Printf("Send %s from Node%d to Node%d", msgTypeToString[msg.Type], node.nodeId, msg.To)
				//TODO error handling here
				_, err := node.client.SendReq(context.Background(), msg)
				if err != nil {
					log.Printf(ERROR+" Could not use rpc on node %d with err %v", node.nodeId, err)
					return
				} else {
					// Perform heartbeat update here
					if rpc.Type == pb.MsgType_HeartBeat {

						ser.stateMachine.recvHeartbeatResponse(msg.To)
					}
				}
			}
			go f(tarNode, rpc)

		} else {
			log.Fatalf("Fail to perform rpc on node %d because no connection", tarNodeId)

		}

	}
}

func (ser *Server) initStateMachine() {
	ser.generateShards(32)
	//attach in/out channels to statemachine
	ser.stateMachine.inCh = ser.inCh
	ser.stateMachine.outCh = ser.outCh
}

// TODO close the connections
func main() {
	flag.Parse()
	nodeId, ip, port, err := readNodeConfig()
	if err != nil {
		return
	}
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
		inCh:  make(chan *pb.Message, 0),
		outCh: make(chan *pb.Message, 0),
	}
	log.Printf("Node %d, begin to load and address is %s"+": %d", nodeId, ip, port)
	//server begin to serve
	lis, err := net.Listen("tcp", ip+fmt.Sprintf(":%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterCoordinateServer(s, localServer)
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	// Start the timer here
	if !*mode {
		go localServer.startTicker(localServer.inCh)
	}
	nodes = createClients(nodes)
	localServer.updatePeerClients(nodes)
	// Start a go routine to send gRPC calls
	go localServer.performRPC()
	// Init statemachine
	localServer.initStateMachine()
	// Start to run state machine
	localServer.stateMachine.mainLoop(localServer.inCh, localServer.outCh)

}
