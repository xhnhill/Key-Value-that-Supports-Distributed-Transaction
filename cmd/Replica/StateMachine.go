package Replica

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
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
func (st *StateMachine) generateTransId(transTimestamp *pb.TransTimestamp) *string {

}

func (st *StateMachine) sendPreAccept(tars []int, trans *pb.Trans) []*pb.Message {
	var msgs []*pb.Message
	t0 := st.generateTimestamp()
	preAccept := pb.PreAcceptReq{
		Trans: trans,
		T0:    t0,
	}
	for i := 0; i < len(tars); i++ {

	}
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
