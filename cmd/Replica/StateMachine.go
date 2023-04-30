package Replica

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
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
