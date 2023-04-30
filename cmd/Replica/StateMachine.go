package Replica

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"log"
)

type Transaction struct {
	in_trans pb.Trans
}
type TimeoutTask struct {
	fc    func(Transaction)
	trans Transaction
}
type StateMachine struct {
	id      int           // Actual node will map the Id to actual address
	m_trans []Transaction // Managed transactions, because this state machine is its coordinator
	//TODO if need optimization of its data structure
	w_trans []Transaction // witnessed transactions
}

func (st *StateMachine) sendPreAccept(tars []int) []*pb.Message {

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
