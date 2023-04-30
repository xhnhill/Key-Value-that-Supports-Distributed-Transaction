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

func (st *StateMachine) executeReq(req *pb.Request) *pb.Response {
	switch req.Type {
	case pb.ReqType_PreAccept:
		log.Printf("Receive req")
	case pb.ReqType_Accept:
		log.Printf("Receive req")
	case pb.ReqType_Commit:
		log.Printf("Receive req")
	case pb.ReqType_Read:
		log.Printf("Receive req")
	case pb.ReqType_Apply:
		log.Printf("Receive req")
	case pb.ReqType_Recover:
		log.Printf("Receive req")
	case pb.ReqType_Tick:
		log.Printf("Receive req")
	}
}

func (st *StateMachine) mainLoop(inCh chan *pb.Request, outCh chan *pb.Response) {
	for {
		val, ok := <-inCh
		if !ok {
			log.Fatal("The channel of the node has been closed, nodeId is %d", st.id)
		}
		st.executeReq(val)

	}
}
