package main

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"bufio"
	"context"
	"flag"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"math"
	"math/big"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
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

func extendBytes(bytes []byte, n int) []byte {
	diff := n - len(bytes)
	if diff < 0 {
		diff = 0
	}
	for i := 0; i < diff; i++ {
		bytes = append(bytes, 0x0)
	}
	return bytes
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
	unit := int(math.Ceil(float64(nodesNum) / float64(*shardsNum)))
	//Construct shardsMap which maps nodeId to shardId
	shardMap := make(map[int32]int32)
	for i := 0; i < *shardsNum; i++ {
		var replicas []int32
		for j := i * unit; j < min((i+1)*unit, nodesNum); j++ {
			replicas = append(replicas, int32(j+1))
			shardMap[int32(j+1)] = int32(i)
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
			Start:    extendBytes(st, n),
			End:      extendBytes(ed, n),
			ShardId:  int32(i),
			Replicas: replicas,
		}
		shards = append(shards, &shard)
	}
	ser.stateMachine.shards = shards
	//Consider 1 based index
	ser.stateMachine.curShard = shards[(ser.node.nodeId-1)/unit]
	ser.stateMachine.shardMap = shardMap

}

func (ser *Server) startTicker(inCh chan *pb.Message) *time.Ticker {
	//TODO please modify back after debugging
	ticker := time.NewTicker(60 * time.Second)

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
			//log.Printf("Send Tick Msg on Node %d", ser.node.nodeId)
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
				//TODO this step seems does not mean the other node is alive
				//log.Printf("Node %d connect successful", nodes[i].nodeId)
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
		log.Printf("Could not connect to user client %s", userInfo.Addr)
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
func (ser *Server) sendToClient(req *pb.Message) {
	finalRes := &pb.FinalRes{}
	proto.Unmarshal(req.Data, finalRes)
	addr := finalRes.Tar
	//check if there is already connected conn
	_, ok := ser.clients[addr]
	var clt pb.CoordinateClient
	if ok {
		clt = ser.clients[addr].client

	} else {
		//Gen new client
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Fatalf("did not connect to %s due to : %v", addr, err)
		}
		clt = pb.NewCoordinateClient(conn)
		//register in the clients map
		ser.clients[addr] = &Node{
			nodeId: 100,
			addr:   addr,
			client: clt,
		}

	}
	//send to client
	f := func(clt pb.CoordinateClient) {
		_, err := clt.SendReq(context.Background(), req)
		if err != nil {
			log.Printf("Error happend when sending results to clients on node%d ", ser.node.nodeId)
		} else {
			log.Printf("Successful sending to %s from node%d", addr, ser.node.nodeId)
		}
	}
	go f(clt)

}

// function which is responsible for send out gRPC request
// Run in a single go routine, but will start lots of other goroutine
// TODO check if it is thread safe to use ser.peers here
func (ser *Server) performRPC() {
	for {
		rpc, ok := <-ser.outCh
		if !ok {
			log.Printf("output channel closed on server %d", ser.node.nodeId)
			return
		} else {
			//log.Printf("Received msg on outChannel on node%d", ser.node.nodeId)
		}
		tarNodeId := int(rpc.To)
		//Avoid network for the msg sending to self
		if rpc.From == ser.stateMachine.id && rpc.To == ser.stateMachine.id {
			ser.inCh <- rpc
			//log.Printf("Send %s from Node%d to Node%d", msgTypeToString[rpc.Type], rpc.To, rpc.To)
			continue
		}
		//Send specific msg to clients
		if rpc.Type == pb.MsgType_FinalResult {
			ser.sendToClient(rpc)
			continue
		}
		// TODO maybe we need read write lock here?
		tarNode, exist := ser.peers[tarNodeId]
		if exist {
			// TODO maybe need to wrap the sending, in case sending fail
			// TODO anything we need to add to context?
			f := func(node *Node, msg *pb.Message) {

				//TODO error handling here
				_, err := node.client.SendReq(context.Background(), msg)
				log.Printf("Send %s from Node%d to Node%d", msgTypeToString[msg.Type], ser.node.nodeId, msg.To)
				if err != nil {
					log.Printf(ERROR+" Could not use rpc on node %d with err %v", node.nodeId, err)
					for j := 0; j < 5; j++ {
						_, err := node.client.SendReq(context.Background(), msg)
						if status.Code(err) == codes.Unavailable {
							log.Printf("Retried %d times to use rpc on node %d", j+1, node.nodeId)
						}
						//TODO if is shutdown, should reuse the dial function
						if err == nil {
							log.Printf("Succeed to use rpc on node %d after %d tries", node.nodeId, j+1)
							break
						}
					}

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
			log.Printf("Fail to perform %s rpc on node %d because no connection", msgTypeToString[rpc.Type], tarNodeId)

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
	//create uderlying persistent layer
	dbName := strconv.Itoa(cur.nodeId) + "DB"
	//TODO could use in memopry version during initial test
	db, err := badger.Open(badger.DefaultOptions("").WithInMemory(true))
	//db, err := badger.Open(badger.DefaultOptions("tmp/" + dbName))
	if err != nil {
		log.Fatalf("failed to create db on node %d", cur.nodeId)
	}
	localServer := &Server{
		node:    cur,
		inCh:    make(chan *pb.Message, 100),
		outCh:   make(chan *pb.Message, 100),
		peers:   make(map[int]*Node),
		clients: make(map[string]*Node),
		stateMachine: &StateMachine{
			id:       int32(cur.nodeId),
			m_trans:  make(map[string]*Transaction),
			w_trans:  make(map[string]*Transaction),
			ballot:   0,
			shards:   make([]*pb.ShardInfo, 0, 6),
			curShard: &pb.ShardInfo{},
			inCh:     nil,
			outCh:    nil,
			peerStatus: &PeerStatus{
				mu:    sync.Mutex{},
				peers: make(map[int32]*Peer),
			},
			T:           make(map[string]*pb.TransTimestamp),
			conflictMap: make(map[string][]string),
			tickNum:     0,
			db:          db,
			dbName:      dbName,
			pq:          make([]*pb.Message, 0, 200),
		},
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
	f := func() {
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}
	go f()

	// Start the timer here
	if !*mode {
		go localServer.startTicker(localServer.inCh)
	}
	//TODO May wait for 60s to let server startup
	//time.Sleep(20 * time.Second)
	nodes = createClients(nodes)
	localServer.updatePeerClients(nodes)
	// Init statemachine
	localServer.initStateMachine()
	// Start a go routine to send gRPC calls
	go localServer.performRPC()
	// Start to run state machine
	//localServer.stateMachine.mainLoop(localServer.inCh, localServer.outCh)
	localServer.stateMachine.reoderMainLoop(localServer.inCh, localServer.outCh)

}
