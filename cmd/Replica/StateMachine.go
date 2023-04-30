package Replica

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"crypto/sha256"
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"time"
)

type Transaction struct {
	in_trans pb.Trans
}
type TimeoutTask struct {
	fc    func(Transaction)
	trans Transaction
}
type StateMachine struct {
	id      int32         // Actual node will map the Id to actual address
	m_trans []Transaction // Managed transactions, because this state machine is its coordinator
	//TODO if need optimization of its data structure
	w_trans []Transaction // witnessed transactions
}

func (st *StateMachine) generateTimestamp() *pb.TransTimestamp {
	timeNow := time.Now()
	t0 := &pb.TransTimestamp{
		TimeStamp: &timestamppb.Timestamp{
			Seconds: timeNow.Unix(),
			Nanos:   int32(timeNow.Nanosecond()),
		},
		Seq: 0,
		Id:  st.id,
	}
	return t0
}
func (st *StateMachine) generateTransId(t *pb.TransTimestamp) *string {
	data, _ := proto.Marshal(t)
	hash := sha256.Sum256(data)
	hashString := fmt.Sprintf("%x", hash)
	return &hashString
}

func (st *StateMachine) sendPreAccept(tars []int32, trans *pb.Trans) []*pb.Message {
	// The transaction received doesn't have t0 and id
	var msgs []*pb.Message
	t0 := st.generateTimestamp()
	trans.Id = *st.generateTransId(t0)
	preAccept := pb.PreAcceptReq{
		Trans: trans,
		T0:    t0,
	}
	msgData, _ := proto.Marshal(&preAccept)
	for i := 0; i < len(tars); i++ {
		msgs = append(msgs, &pb.Message{
			Type: pb.MsgType_PreAccept,
			Data: msgData,
			From: st.id,
			To:   tars[i],
		})
	}
	return msgs
}

func (st *StateMachine) executeReq(req *pb.Message) []*pb.Message {
	switch req.Type {
	case pb.MsgType_PreAccept:
		log.Printf("Receive req")
	case pb.MsgType_Accept:
		log.Printf("Receive req")
	case pb.MsgType_Commit:
		log.Printf("Receive req")
	case pb.MsgType_Read:
		log.Printf("Receive req")
	case pb.MsgType_Apply:
		log.Printf("Receive req")
	case pb.MsgType_Recover:
		log.Printf("Receive req")
	case pb.MsgType_Tick:
		log.Printf("Receive req")
	}
}

func (st *StateMachine) mainLoop(inCh chan *pb.Message, outCh chan *pb.Message) {
	for {
		val, ok := <-inCh
		if !ok {
			log.Fatal("The channel of the node has been closed, nodeId is %d", st.id)
		}
		st.executeReq(val)

	}
}
