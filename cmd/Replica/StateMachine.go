package main

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
const FQ_TIMEOUT time.Duration = 20 * time.Second

// TODO config struct
type Config struct {
	epoch     int // config generation
	fastSize  int //fast quorum
	classSize int // classical quorum
}

type Transaction struct {
	config    Config
	in_trans  *pb.Trans
	keys      []string //Record all the keys used by the transactions
	ifTimeout bool
	// Only meaningful when ifTimeout == true, which labels the specific time to timeout
	endTime   *timestamppb.Timestamp
	votes     int                //total votes, different stage has different meaning
	fVotes    int                // votes satisfy the fast quorum
	deps      map[string]bool    // A deps set used for current stage
	couldFast bool               // true when still considering the fast path, false means use slow path
	failsNum  int                //Number of failed peers
	collectT  *pb.TransTimestamp // Used in the PreAccept which collects max t from PreAcceptOK
	//TODO optimize in the final version, use former deps instead
	acceptDeps  map[string]bool //Used to collect deps in collecting AcceptOk stage
	acceptVotes int             // Count votes in AcceptOk stage
}

// Extract the keys and save it as string in the keys field of Transaction
func (tr *Transaction) updateKeys() {
	keySet := genKeySet(tr.in_trans)
	var keys []string
	for k, _ := range keySet {
		keys = append(keys, k)
	}
	tr.keys = keys
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
	//current tick number, when greater than 10, send heartbeat msg
	tickNum int
}

// Get keys of the transaction
// TODO output set
func (trans *Transaction) getKeys() {

}

// Register the transaction in the conflict keymap
// This process only happens in PreAccept phase
func (st *StateMachine) registerConflicts(trans *Transaction) {
	keys := trans.keys
	transId := trans.in_trans.Id
	for i := 0; i < len(keys); i++ {
		ls := st.conflictMap[keys[i]]
		st.conflictMap[keys[i]] = append(ls, transId)
	}
}

// Generate conflict trans
// Judge the return
// Use the inverted index to find the conflicts
func (st *StateMachine) getConflicts(trans *Transaction) []string {
	keys := trans.keys
	transId := trans.in_trans.Id
	var cMap map[string]bool
	cMap = make(map[string]bool)
	var conflicts []string
	for i := 0; i < len(keys); i++ {
		ls := st.conflictMap[keys[i]]
		for j := 0; j < len(ls); j++ {
			if ls[j] != transId {
				cMap[ls[j]] = true
			}
		}
	}
	for k, _ := range cMap {
		conflicts = append(conflicts, k)
	}
	return conflicts
}

// filter the conflicts in the processAccept stage
func (st *StateMachine) filterConflictsAccept(clfs []string, timestamp *pb.TransTimestamp) []string {
	res := make([]string, 0, 6)
	for i := 0; i < len(clfs); i++ {
		tarTrans := st.w_trans[clfs[i]]
		tarT0 := tarTrans.in_trans.T0
		if compareTimestamp(timestamp, tarT0) {
			res = append(res, tarTrans.in_trans.Id)
		}
	}
	return res

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
	trans := &pb.Trans{}
	proto.Unmarshal(req.Data, trans)
	// Preprocess transactions
	trans.St = pb.TranStatus_New
	// TODO calculate Electorates
	e := st.getRelatedReplicas(trans)
	//update the related replicas in the innerTrans
	trans.RelatedReplicas = e
	//TODO Optimize the configuration process
	//Set electorates size of the transaction
	trans.EleSize = int32(len(e))
	//Will set to,Id and register in the sendPreAccept function
	preAccepts := st.sendPreAccept(e, trans)
	st.sendMsgs(preAccepts)
	//Begin to set timeout for fast quorum
	curTrans := st.m_trans[trans.Id]
	tarTimeout := time.Now().Add(FQ_TIMEOUT)
	curTrans.endTime = &timestamppb.Timestamp{
		Seconds: tarTimeout.Unix(),
		Nanos:   int32(tarTimeout.Nanosecond()),
	}
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
	//tr := &Transaction{in_trans: trans}
	tr := &Transaction{
		config:      Config{},
		in_trans:    trans,
		keys:        make([]string, 0, 6),
		ifTimeout:   false,
		endTime:     nil,
		votes:       0,
		fVotes:      0,
		deps:        make(map[string]bool),
		couldFast:   true,
		failsNum:    0,
		collectT:    trans.T0,
		acceptDeps:  make(map[string]bool),
		acceptVotes: 0,
	}
	switch t {
	case Managed:
		// Because the managed statemachine will also recv PreAccept,
		//the witnessed will be update at that stage
		st.m_trans[trans.Id] = tr
	case Witnessed:
		_, ok := st.m_trans[trans.Id]
		if ok {
			st.w_trans[trans.Id] = st.m_trans[trans.Id]
		} else {
			st.w_trans[trans.Id] = tr
		}

	}
}

// TODO if need to update the transaction info

// Send PreAccept Request
func (st *StateMachine) sendPreAccept(tars []int32, trans *pb.Trans) []*pb.Message {
	// The transaction received doesn't have t0 and id
	var msgs []*pb.Message
	t0 := st.generateTimestamp()
	trans.Id = *st.generateTransId(t0)
	trans.T0 = t0
	st.registerTrans(Managed, trans)
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
	//Update trans status
	trans.St = pb.TranStatus_PreAccepted
	return msgs
}

// TODO process tick message
func (st *StateMachine) processTick(msg *pb.Message) {
	//UnMarshal the msg

	tickMsg := &pb.TickMsg{}
	proto.Unmarshal(msg.Data, tickMsg)
	//TODO check and deal with timeout transactions
	//TODO Timeout for fast quorum
	mTrans := st.m_trans
	for k, _ := range mTrans {
		tar := mTrans[k]

		if tar.in_trans.St == pb.TranStatus_PreAccepted ||
			tar.in_trans.St == pb.TranStatus_New {
			if tar.endTime != nil &&
				tickMsg.TimeStamp.AsTime().After(tar.endTime.AsTime()) {
				log.Printf("Fast quorum timeout on node %d", st.id)
				//Perform timeout logic
				tar.couldFast = false
				//TODO force to check the slow path logic
				tar.endTime = nil
				st.checkAndProcessSlowPath(tar)
			}

		}
	}
	//TODO  timeout for recovery

	//TODO purge managed and witnessed transactions of statemachine

	// TODO Monitor the timeout statemachine,
	// TODO here each node should have an accumulated timeout value
	// TODO think if that value should be in this layer
	// TODO Or both layer need, and they mean different things
	st.modifyAndCheck(-1, -1)

	//TODO Send heart beats, which acts as a weak failure detector
	st.tickHeartbeat()

}
func (st *StateMachine) tickHeartbeat() {
	st.peerStatus.mu.Lock()
	defer st.peerStatus.mu.Unlock()
	var msgs []*pb.Message
	st.tickNum++
	if st.tickNum < 10 {
		return
	}
	st.tickNum = 0
	for k, _ := range st.peerStatus.peers {
		heartBeat := &pb.HeartbeatMsg{To: k}
		data, _ := proto.Marshal(heartBeat)
		msgs = append(msgs, &pb.Message{
			Type: pb.MsgType_HeartBeat,
			Data: data,
			From: st.id,
			To:   k,
		})
	}
	st.sendMsgs(msgs)
}

// TODO write unit test for this function
func equals(t1 *pb.TransTimestamp, t2 *pb.TransTimestamp) bool {
	tp1 := t1.TimeStamp.AsTime()
	tp2 := t2.TimeStamp.AsTime()
	return tp1.Equal(tp2) && t1.Seq == t2.Seq && t1.Id == t2.Id
}

// true means greater
func compareTimestamp(t1 *pb.TransTimestamp, t2 *pb.TransTimestamp) bool {
	tp1 := t1.TimeStamp.AsTime()
	tp2 := t2.TimeStamp.AsTime()
	if tp1.Equal(tp2) {
		if t1.Seq == t2.Seq {
			return t1.Id > t2.Id
		} else {
			return t1.Seq > t2.Seq
		}
	} else {
		return tp1.After(tp2)
	}
}
func copyTransTimestamp(timestamp *pb.TransTimestamp) *pb.TransTimestamp {
	return &pb.TransTimestamp{
		TimeStamp: timestamp.TimeStamp,
		Seq:       timestamp.Seq,
		Id:        timestamp.Id,
	}
}

// TODO process the PreAccept Request
func (st *StateMachine) processPreAccept(req *pb.Message) {
	var innerTrans *pb.Trans
	preAccept := &pb.PreAcceptReq{}
	proto.Unmarshal(req.Data, preAccept)
	innerTrans = preAccept.Trans
	innerTrans.T0 = preAccept.T0
	//Register the transaction as witnesses
	st.registerTrans(Witnessed, innerTrans)
	trans := st.w_trans[innerTrans.Id]
	//Update the transaction
	// Update the keys of the transaction, in case of finding conlicting transa
	trans.updateKeys()
	//check conflicts of transactions
	conflicts := st.getConflicts(trans)
	// Update specific config for this Trans
	//TODO optimize this configuration process
	//TODO May move to trans submission part in case of recv late from self
	trans.couldFast = true
	eSize := innerTrans.EleSize
	trans.config.fastSize = int(3*eSize/4) + 1
	trans.config.classSize = int(eSize/2) + 1
	//compare and set t(trans) and T(trans)
	// May need to copy a timestamp, because we will modify it

	tTrans := copyTransTimestamp(preAccept.T0)
	for i := 0; i < len(conflicts); i++ {
		// Compare with T
		tarTimestamp := st.T[conflicts[i]]
		if !compareTimestamp(tTrans, tarTimestamp) {
			tTrans = copyTransTimestamp(tarTimestamp)
			tTrans.Seq = tTrans.Seq + 1
			tTrans.Id = st.id
		}
	}
	// Assign max timestamp to T
	st.T[innerTrans.Id] = tTrans
	//update the transaction status to PreAccepted
	st.w_trans[innerTrans.Id].in_trans.St = pb.TranStatus_PreAccepted
	//Send PreAcceptOk message
	PreAcceptOk := &pb.PreAcceptResp{
		T:       tTrans,
		Deps:    st.genDepsPreAccept(conflicts, innerTrans.T0),
		TransId: innerTrans.Id,
	}
	data, _ := proto.Marshal(PreAcceptOk)
	var msgs []*pb.Message
	msgs = append(msgs, &pb.Message{
		Type: pb.MsgType_PreAcceptOk,
		Data: data,
		From: st.id,
		To:   req.From,
	})
	st.sendMsgs(msgs)

}

// Generate deps of trans in processing PreAccept phase
// According to paper, should use t0 to filter
func (st *StateMachine) genDepsPreAccept(cfl []string, t0 *pb.TransTimestamp) *pb.Deps {
	var deps []string
	for i := 0; i < len(cfl); i++ {
		tarT0 := st.w_trans[cfl[i]].in_trans.T0
		if compareTimestamp(t0, tarT0) {
			deps = append(deps, cfl[i])
		}
	}
	return &pb.Deps{Ids: deps}
}

// update corresponding deps in transaction with transId
func (st *StateMachine) updateDeps(cfl []string, trans *Transaction) {
	for i := 0; i < len(cfl); i++ {
		trans.deps[cfl[i]] = true
	}
}

// Update the deps in transaction at AcceptOk stage, use acceptDeps in Transaction
func (st *StateMachine) updateDepsAcceptOk(cfl []string, trans *Transaction) {
	for i := 0; i < len(cfl); i++ {
		trans.acceptDeps[cfl[i]] = true
	}
}
func (st *StateMachine) convDepsSet(depsSet map[string]bool) []string {
	var deps []string
	for k, _ := range depsSet {
		deps = append(deps, k)
	}
	return deps
}

// Because timeout, msgs may duplicate
// Receiver should be able to filter
func (st *StateMachine) checkAndProcessSlowPath(curTrans *Transaction) {
	// Check if is the correct stage
	if curTrans.in_trans.St != pb.TranStatus_PreAccepted &&
		curTrans.in_trans.St != pb.TranStatus_New {
		return
	}
	if curTrans.votes >= curTrans.config.classSize {
		//Update the final version of Execution time
		curTrans.in_trans.ExT = copyTransTimestamp(curTrans.collectT)
		accMsg := &pb.AcceptReq{
			Trans: curTrans.in_trans,
			ExT:   curTrans.collectT,
			Deps:  &pb.Deps{Ids: st.convDepsSet(curTrans.deps)},
		}
		//send acceptMsg
		data, _ := proto.Marshal(accMsg)
		msgs := make([]*pb.Message, 0, len(curTrans.in_trans.RelatedReplicas))
		for i := 0; i < len(curTrans.in_trans.RelatedReplicas); i++ {
			msgs = append(msgs, &pb.Message{
				Type: pb.MsgType_Accept,
				Data: data,
				From: st.id,
				To:   curTrans.in_trans.RelatedReplicas[i],
			})
		}
		st.sendMsgs(msgs)
		//update current trans status avoid further votes
		curTrans.in_trans.St = pb.TranStatus_Accepted
		//TODO clear votes, because votes will be used in AcceptOk stage
	}
}

// TODO process PreAcceptOk
func (st *StateMachine) processPreAcceptOk(req *pb.Message) {
	var preAcceptOk *pb.PreAcceptResp
	preAcceptOk = &pb.PreAcceptResp{}
	proto.Unmarshal(req.Data, preAcceptOk)
	//TODO think about what if this node doesn't see this trans
	curTrans := st.m_trans[preAcceptOk.TransId]
	//Update classical votes
	curTrans.votes++
	//update collectT proposed by PreAcceptOK
	curTrans.collectT = copyTransTimestamp(curTrans.in_trans.T0)
	if compareTimestamp(preAcceptOk.T, curTrans.collectT) {
		curTrans.collectT = copyTransTimestamp(preAcceptOk.T)
	}
	//TODO check if must slow path now
	//TODO check if fast quorum is enough
	//TODO slow path status
	if curTrans.couldFast {
		//update fvotes
		if equals(preAcceptOk.T, curTrans.in_trans.T0) {
			curTrans.fVotes++
		}
		if curTrans.fVotes >= curTrans.config.fastSize {
			//perform the fast path logic

			commitMsg := &pb.CommitReq{
				Trans: curTrans.in_trans,
				ExT:   curTrans.in_trans.T0,
				Deps:  &pb.Deps{Ids: st.convDepsSet(curTrans.deps)},
			}
			data, _ := proto.Marshal(commitMsg)
			var msgs []*pb.Message
			for i := 0; i < len(curTrans.in_trans.RelatedReplicas); i++ {
				msgs = append(msgs, &pb.Message{
					Type: pb.MsgType_Commit,
					Data: data,
					From: st.id,
					To:   curTrans.in_trans.RelatedReplicas[i],
				})
			}
			st.sendMsgs(msgs)
			//TODO perform execution logic
			st.sendReads()

		}
	} else {

		st.checkAndProcessSlowPath(curTrans)

	}

}

// TODO Process the Accept Message
func (st *StateMachine) processAccept(req *pb.Message) {

	accept := &pb.AcceptReq{}
	proto.Unmarshal(req.Data, accept)
	transId := accept.Trans.Id
	//Update the T in trans
	curTrans := st.w_trans[transId]
	if compareTimestamp(accept.ExT, st.T[transId]) {
		st.T[transId] = copyTransTimestamp(accept.ExT)
	}
	// Set status of the transaction
	curTrans.in_trans.St = pb.TranStatus_Accepted
	//Get conflicts and prepare deps
	clfs := st.getConflicts(curTrans)
	//Filtered the conflicts
	fClfs := st.filterConflictsAccept(clfs, accept.ExT)
	// Send AcceptOk
	acceptOk := &pb.AcceptResp{
		Deps:    &pb.Deps{Ids: fClfs},
		TransId: transId,
	}
	data, _ := proto.Marshal(acceptOk)
	msgs := make([]*pb.Message, 0, 1)
	msgs = append(msgs, &pb.Message{
		Type: pb.MsgType_AcceptOk,
		Data: data,
		From: st.id,
		To:   req.From,
	})
	st.sendMsgs(msgs)

}
func (st *StateMachine) checkAcceptOkVotes(curTrans *Transaction) {
	if curTrans.in_trans.St != pb.TranStatus_Accepted {
		//TODO log?
		return
	}
	curTrans.acceptVotes++
	if curTrans.acceptVotes >= curTrans.config.classSize {
		//Pay attention, the Ext is the final decided version of collectT in outer Trans
		commitMsg := &pb.CommitReq{
			Trans: curTrans.in_trans,
			ExT:   curTrans.in_trans.ExT,
			Deps:  &pb.Deps{Ids: st.convDepsSet(curTrans.acceptDeps)},
		}
		data, _ := proto.Marshal(commitMsg)
		msgs := make([]*pb.Message, 0, 6)
		//TODO the sending logics may be optimized, and send only to needed peers
		for i := 0; i < len(curTrans.in_trans.RelatedReplicas); i++ {
			msgs = append(msgs, &pb.Message{
				Type: pb.MsgType_Commit,
				Data: data,
				From: st.id,
				To:   curTrans.in_trans.RelatedReplicas[i],
			})
		}
		st.sendMsgs(msgs)
		//Update local status to committed
		curTrans.in_trans.St = pb.TranStatus_Commited
		//TODO Execute the Ecution Protocol
		st.sendReads()

	}
}

// TODO Process AcceptOk message
func (st *StateMachine) processAcceptOk(req *pb.Message) {
	acceptOk := &pb.AcceptResp{}
	proto.Unmarshal(req.Data, acceptOk)
	curTrans := st.m_trans[acceptOk.TransId]
	st.updateDepsAcceptOk(acceptOk.Deps.Ids, curTrans)
	st.checkAcceptOkVotes(curTrans)
}

func (st *StateMachine) commitMessage(req *pb.Message) {
	commitMsg := &pb.CommitReq{}
	proto.Unmarshal(req.Data, commitMsg)
	transId := commitMsg.Trans.Id
	//Modify the status of the transaction
	st.w_trans[transId].in_trans.St = pb.TranStatus_Commited
	//TODO Any other thing that need to be done?
}

// TODO Send read requests
// TODO optimize the nearby configuration
func (st *StateMachine) sendReads() {

}

// TODO perform read operations, the execution logic
func (st *StateMachine) processRead(req *pb.Message) {

}

// TODO trigger the waiting trans to examine execution condition
// Trans may wait during processing Read
func (st *StateMachine) transTrigger() {

}

// TODO process readOk
func (st *StateMachine) processReadOk(req *pb.Message) {

	//TODO send results to clients
}

// TODO process apply message
func (st *StateMachine) processApply(req *pb.Message) {

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
	case pb.MsgType_PreAcceptOk:
		log.Printf("Receive PreAcceptOk Msg from %d", req.From)
		st.processPreAcceptOk(req)
	case pb.MsgType_Accept:
		log.Printf("Receive Accept Msg from %d", req.From)
		st.processAccept(req)
	case pb.MsgType_AcceptOk:
		log.Printf("Receive AcceptOk Msg from %d", req.From)
		st.processAcceptOk(req)
	case pb.MsgType_Commit:
		log.Printf("Receive Commit Msg from %d", req.From)
		st.commitMessage(req)
	case pb.MsgType_Read:
		log.Printf("Receive Read Msg from %d", req.From)
		st.processRead(req)
	case pb.MsgType_ReadOk:
		log.Printf("Receive ReadOk Msg from %d", req.From)
		st.processReadOk(req)
	case pb.MsgType_Apply:
		log.Printf("Receive Apply Msg from %d", req.From)
		st.processApply(req)
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
			log.Printf("The channel of the node has been closed, nodeId is %d", st.id)
		}
		st.executeReq(val)

	}
}
