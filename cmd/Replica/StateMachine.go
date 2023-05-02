package Replica

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"crypto/sha256"
	"fmt"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"sync"
	"time"
)

const PEER_TIMEOUT int = 60 // unit is second
const ERROR string = "[ERROR]: "

// TODO config struct
type Config struct {
	epoch    int // config generation
	fastSize int //fast quorum
}

type Transaction struct {
	config    Config
	in_trans  *pb.Trans
	keys      []string //Record all the keys used by the transactions
	ifTimeout bool
	// Only meaningful when ifTimeout == true, which labels the specific time to timeout
	endTime *timestamppb.Timestamp
	votes   int //total votes, different stage has different meaning
	fVotes  int // votes satisfy the fast quorum

}
type TimeoutTask struct {
	fc    func(Transaction)
	trans Transaction
}

// Used by failure detector to monitor Peer status
// Should be init when the statemachine is created
// Will be modified by
// 1. tick msg: add timeout (main statemachine go routine)
// 2. heartResponse: clear timeout (The go routine performs the synchronous rpc call)
// TODO init this struct in statemachine
type PeerStatus struct {
	mu    sync.Mutex
	peers map[int32]*Peer
}

type Peer struct {
	timeout int   // Should be init with PEER_TIMEOUT
	status  bool  // True means alive
	id      int32 // The same with nodeId
}

// Check and modify the timeout of peer Status
// when nodeId is -1, perform on all node
func (st *StateMachine) modifyAndCheck(incr int, nodeId int32) {
	peerStatus := st.peerStatus
	peerStatus.mu.Lock()
	defer peerStatus.mu.Unlock()
	if nodeId > 0 {
		// Increasing currently when receive heartbeat resp
		peerStatus.peers[nodeId].timeout = PEER_TIMEOUT
	} else {
		for k, _ := range peerStatus.peers {
			peer := peerStatus.peers[k]
			if !peer.status {
				continue
			}
			v := peer.timeout
			v = v + incr
			if v <= 0 {
				log.Printf(ERROR+"Node %d is died, timeout detected", k)
				//TODO perform timeout logic for node
				// ?? Recover the timeout?
				peerStatus.peers[k].status = false
			} else {
				peerStatus.peers[k].timeout = v
			}
		}

	}

}

// TODO examine the parts that need synchronization
type StateMachine struct {
	id      int32                   // Actual node will map the Id to actual address
	m_trans map[string]*Transaction // Managed transactions, because this state machine is its coordinator
	//TODO if need optimization of its data structure
	//TODO maybe a heap here?
	w_trans map[string]*Transaction // witnessed transactions, m_trans is a subset of this slice
	//a map here, which key is the transaction id, value should be the transaction
	//Corresponding event will come here to uodate status

	//Ballot number
	ballot int32
	//TODO shards
	//shards contain the sharding info of the cluster
	shards []*pb.ShardInfo
	// Record the shard info of the current node
	curShard *pb.ShardInfo
	//In channel, which is the source of msgs
	inCh chan *pb.Message
	//Out channel, which is the same with its node
	outCh chan *pb.Message
	//Peer status, mainly about the indication of connection
	peerStatus *PeerStatus
	// T recorded for witnessed transactions
	T map[string]*pb.TransTimestamp
	// conflicting keys, which register key - transaction relationship
	conflictMap map[string][]string
}

// Get keys of the transaction
// TODO output set
func (trans *Transaction) getKeys() {

}

// Generate conflict trans
// Judge the return
// Use the inverted index to find the conflicts
func (st *StateMachine) getConflicts(trans *Transaction) []*string {

}

//TODO a function to union the deps

//TODO Read function, read actual values from the data base

//TODO Write function, write actual values from the database

//TODO Detect failure, send heartbeat

//TODO basic function, Compare timestamp of the transaction

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

// TODO process Transaction submission from client
// TODO any persistent needs during processing   thinking after the first draft
func (st *StateMachine) recvTrans(req *pb.Message) {

	//UnMarshall trans
	var trans pb.Trans
	proto.Unmarshal(req.Data, &trans)
	// Preprocess transactions
	trans.Timestamp = st.generateTimestamp()
	trans.Id = *st.generateTransId(trans.Timestamp)
	trans.St = pb.TranStatus_New
	//Register transaction as managed transaction
	st.registerTrans(Managed, &trans)
	// TODO calculate Electorates
	e := st.getRelatedReplicas(&trans)
	preAccepts := st.sendPreAccept(e, &trans)
	st.sendMsgs(preAccepts)
}

// Logically send messages to channel
func (st *StateMachine) sendMsgs(msgs []*pb.Message) {
	for i := 0; i < len(msgs); i++ {
		st.outCh <- msgs[i]
	}
}
func genKeySet(trans *pb.Trans) map[string]bool {
	keySet := make(map[string]bool)
	for i := 0; i < len(trans.Reads); i++ {
		keySet[trans.Reads[i].Key] = true
	}
	for i := 0; i < len(trans.Writes); i++ {
		keySet[trans.Writes[i].Key] = true
	}
	return keySet
}

// 1 means b1 greater than b2, 0 means equal, -1 means less
func compareByte(b1 []byte, b2 []byte) int {
	for i := 31; i >= 0; i-- {
		if b1[i] > b2[i] {
			return 1
		} else if b1[i] < b2[i] {
			return -1
		}
	}
	return 0
}
func ifKeyInRange(key string, st []byte, end []byte) bool {
	strByte := []byte(key)
	diff := 32 - len(strByte)
	for i := 0; i < diff; i++ {
		strByte = append(strByte, 0x0)
	}
	return compareByte(strByte, st) >= 0 && compareByte(strByte, end) < 0
}
func (st *StateMachine) ifInShards(keySet map[string]bool, info *pb.ShardInfo) bool {
	for key, _ := range keySet {
		if ifKeyInRange(key, info.Start, info.End) {
			return true
		}
	}
	return false
}

// Basic version finding the electorates in preAccept
// TODO may need modify and optimize
func (st *StateMachine) getRelatedReplicas(trans *pb.Trans) []int32 {
	var tars []int32
	keySet := genKeySet(trans)
	for i := 0; i < len(st.shards); i++ {
		if st.ifInShards(keySet, st.shards[i]) {
			tars = append(tars, st.shards[i].Replicas...)
		}
	}
	return tars
}

type RegisterTransType int

const (
	Managed RegisterTransType = iota
	Witnessed
)

// TODO register transaction
func (st *StateMachine) registerTrans(t RegisterTransType, trans *pb.Trans) {
	tr := &Transaction{in_trans: trans}
	switch t {
	case Managed:
		// Because the managed statemachine will also recv PreAccept,
		//the witnessed will be update at that stage
		st.m_trans[trans.Id] = tr
	case Witnessed:
		st.w_trans[trans.Id] = tr
	}
}

// TODO if need to update the transaction info

// Send PreAccept Request
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

// TODO process tick message
func (st *StateMachine) processTick(msg *pb.Message) {

	//TODO check and deal with timeout transactions

	//TODO purge managed and witnessed transactions of statemachine

	// TODO Monitor the timeout statemachine,
	// TODO here each node should have an accumulated timeout value
	// TODO think if that value should be in this layer
	// TODO Or both layer need, and they mean different things
	st.modifyAndCheck(-1, -1)

	//TODO Send heart beats, which acts as a weak failure detector
}

// TODO process the PreAccept Request
func (st *StateMachine) processPreAccept(req *pb.Message) {
	//Register the transaction as witnesses

	//Update the transaction
	//check conflicts of transactions
}

// TODO process PreAcceptOk
func (st *StateMachine) processPreAcceptOk() {

}

// Receive heartbeat response
func (st *StateMachine) recvHeartbeatResponse(nodeId int32) {
	//TODO update the node status monitored by statemachine
	st.modifyAndCheck(PEER_TIMEOUT, nodeId)
}
func (st *StateMachine) executeReq(req *pb.Message) {
	switch req.Type {
	case pb.MsgType_PreAccept:
		log.Printf("Receive PreAccept Msg from %d", req.From)
		st.processPreAccept(req)
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
		log.Printf("Execute logics for tick")
		st.processTick(req)
	case pb.MsgType_SubmitTrans:
		log.Printf("Execute SubmitTrans request")
		st.recvTrans(req)
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
