package main

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"context"
	"flag"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gizak/termui/v3"
	"github.com/gizak/termui/v3/widgets"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	addr = flag.String("addr", "localhost:50070", "the address of client Node")

	//TODO replace this place and use round robin to select server later
	server = flag.String("ser", "localhost:50051", "the address of connected server")
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

func performTransaction(clt *DbClient, ser pb.CoordinateClient, localServer *cltServer, readKeys, writeKeys, writeValues []string) {
	trans := generateTrans(readKeys, writeKeys, writeValues, &clt.nodeinfo)
	localServer.blockRead(trans, ser)
}

// TODO optimize the client to be thread safe
func main() {
	flag.Parse()
	clt := &DbClient{nodeinfo: pb.NodeInfo{Addr: *addr}}
	ser := getServerClient(*server)
	localServer := &cltServer{
		transMap: make(map[string]chan []*pb.SingleResult),
		mu:       sync.Mutex{},
	}
	// Initialize termui
	if err := termui.Init(); err != nil {
		log.Fatalf("failed to initialize termui: %v", err)
	}
	defer termui.Close()

	// Create input widgets
	readKeysInput := widgets.NewParagraph()
	readKeysInput.Title = "Read Keys (comma separated)"
	readKeysInput.SetRect(0, 0, 60, 5)

	writeKeysInput := widgets.NewParagraph()
	writeKeysInput.Title = "Write Keys (comma separated)"
	writeKeysInput.SetRect(0, 5, 60, 10)

	writeValuesInput := widgets.NewParagraph()
	writeValuesInput.Title = "Write Values (comma separated)"
	writeValuesInput.SetRect(0, 10, 60, 15)

	resultBox := widgets.NewParagraph()
	resultBox.Title = "Result"
	resultBox.SetRect(0, 15, 60, 20)

	// Render UI
	termui.Render(readKeysInput, writeKeysInput, writeValuesInput, resultBox)

	// Event loop
	uiEvents := termui.PollEvents()
	for {
		e := <-uiEvents
		switch e.ID {
		case "q", "<C-c>":
			return
		case "<Tab>":
			if readKeysInput.BorderStyle.Fg == termui.ColorGreen {
				readKeysInput.BorderStyle.Fg = termui.ColorWhite
				writeKeysInput.BorderStyle.Fg = termui.ColorGreen
			} else if writeKeysInput.BorderStyle.Fg == termui.ColorGreen {
				writeKeysInput.BorderStyle.Fg = termui.ColorWhite
				writeValuesInput.BorderStyle.Fg = termui.ColorGreen
			} else {
				writeValuesInput.BorderStyle.Fg = termui.ColorWhite
				readKeysInput.BorderStyle.Fg = termui.ColorGreen
			}
			termui.Render(readKeysInput, writeKeysInput, writeValuesInput)
		case "<Backspace>":
			if readKeysInput.BorderStyle.Fg == termui.ColorGreen && len(readKeysInput.Text) > 0 {
				readKeysInput.Text = readKeysInput.Text[:len(readKeysInput.Text)-1]
			} else if writeKeysInput.BorderStyle.Fg == termui.ColorGreen && len(writeKeysInput.Text) > 0 {
				writeKeysInput.Text = writeKeysInput.Text[:len(writeKeysInput.Text)-1]
			} else if len(writeValuesInput.Text) > 0 {
				writeValuesInput.Text = writeValuesInput.Text[:len(writeValuesInput.Text)-1]
			}
			termui.Render(readKeysInput, writeKeysInput, writeValuesInput)
		case "<Enter>":
			readKeys := strings.Split(readKeysInput.Text, " ")
			writeKeys := strings.Split(writeKeysInput.Text, " ")
			writeValues := strings.Split(writeValuesInput.Text, " ")

			// Call the function to perform the transaction and fetch the result
			go performTransaction(clt, ser, localServer, readKeys, writeKeys, writeValues)

			// Reset the input fields
			readKeysInput.Text = ""
			writeKeysInput.Text = ""
			writeValuesInput.Text = ""

			termui.Render(readKeysInput, writeKeysInput, writeValuesInput)
		default:
			if e.Type == termui.KeyboardEvent {
				if readKeysInput.BorderStyle.Fg == termui.ColorGreen {
					readKeysInput.Text = readKeysInput.Text + e.ID
				} else if writeKeysInput.BorderStyle.Fg == termui.ColorGreen {
					writeKeysInput.Text = writeKeysInput.Text + e.ID
				} else {
					writeValuesInput.Text = writeValuesInput.Text + e.ID
				}
				termui.Render(readKeysInput, writeKeysInput, writeValuesInput)
			}
		}
	}

	//Start receiving server
	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		log.Fatalf("failed to listen on client: %v", err)
	}
	//localServer := &cltServer{
	//	transMap: make(map[string]chan []*pb.SingleResult),
	//		mu:       sync.Mutex{},
	//	}
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
	//clt := &DbClient{nodeinfo: pb.NodeInfo{Addr: *addr}}
	//ser := getServerClient(*server)
	for i := 0; i < 10; i++ {
		go localServer.MassiveConcurrent(clt, ser)
	}
	//localServer.fixedRead(clt, ser)
	time.Sleep(15 * time.Second)

}
