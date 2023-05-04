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

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
)

var (
	addr = flag.String("addr", "localhost:51082"+
		"", "the address of client Node")

	//TODO replace this place and use round robin to select server later
	server = flag.String("ser", "localhost:50036", "the address of connected server")
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
func (localServer *cltServer) concurrentOp(clt *DbClient, ser pb.CoordinateClient) {
	for i := 0; i < 50; i++ {
		go localServer.MassiveConcurrent(clt, ser)
	}
	time.Sleep(300 * time.Second)
}

func performTransaction(clt *DbClient, ser pb.CoordinateClient, localServer *cltServer, readKeys, writeKeys, writeValues []string) {
	trans := generateTrans(readKeys, writeKeys, writeValues, &clt.nodeinfo)
	localServer.blockRead(trans, ser)

}

func runGUI(localServer *cltServer) {
	a := app.New()
	w := a.NewWindow("DB Client")
	clt := &DbClient{nodeinfo: pb.NodeInfo{Addr: *addr}}
	var ser pb.CoordinateClient
	// Create input fields
	serverIPInput := widget.NewEntry()
	serverIPInput.SetPlaceHolder("Server IP")
	serverPortInput := widget.NewEntry()
	serverPortInput.SetPlaceHolder("Server Port")
	readKeysEntry := widget.NewEntry()
	readKeysEntry.SetPlaceHolder("Enter read keys")
	writeKeysEntry := widget.NewEntry()
	writeKeysEntry.SetPlaceHolder("Enter write keys")
	writeValuesEntry := widget.NewEntry()
	writeValuesEntry.SetPlaceHolder("Enter write values")

	// Create log output label and scroll container
	logOutput := widget.NewLabel("")
	logScroll := container.NewVScroll(logOutput)
	logScroll.SetMinSize(fyne.NewSize(500, 300))
	// Set custom log writer
	log.SetOutput(&logWriter{output: logOutput})

	// Create buttons and set actions
	connectBtn := widget.NewButton("Connect", func() {
		*server = serverIPInput.Text + ":" + serverPortInput.Text
		ser = getServerClient(*server)
		//log.Println("Connected to server:", *server)
	})
	performTransBtn := widget.NewButton("Perform Transaction", func() {
		readKeys := strings.Split(readKeysEntry.Text, ",")
		writeKeys := strings.Split(writeKeysEntry.Text, ",")
		writeValues := strings.Split(writeValuesEntry.Text, ",")

		go performTransaction(clt, ser, localServer, readKeys, writeKeys, writeValues)
	})
	concurrentOpBtn := widget.NewButton("Concurrent Operation", func() {
		go localServer.concurrentOp(clt, ser)
	})
	fixedReadBtn := widget.NewButton("Fixed Read", func() {
		go localServer.fixedRead(clt, ser)
	})

	// Add input fields and buttons to the container
	form := container.NewVBox(
		serverIPInput,
		serverPortInput,
		readKeysEntry,
		writeKeysEntry,
		writeValuesEntry,
		connectBtn,
		performTransBtn,
		concurrentOpBtn,
		fixedReadBtn,
		logScroll,
	)

	w.SetContent(form)
	w.ShowAndRun()
}

type logWriter struct {
	output *widget.Label
}

func (lw *logWriter) Write(p []byte) (n int, err error) {
	lw.output.SetText(lw.output.Text + string(p))
	return len(p), nil
}

// TODO optimize the client to be thread safe
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
	runGUI(localServer)
	go f()
	// calling part
	//select {}
	//localServer.concurrentOp(clt, ser)
	//localServer.fixedRead(clt, ser)
}
