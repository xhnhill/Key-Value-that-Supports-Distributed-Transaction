package main

import (
	pb "Distributed_Key_Value_Store/cmd/Primitive"
	"bytes"
	"container/heap"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"github.com/dgraph-io/badger/v4"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"log"
	"sync"
	"time"
)

// Author: Haining Xie

// The logic of statemachine, the core function is executeReq()
//It shows how statemmachine deals with each incoming messages
// The statemachine allows the message arrive out of order

const PEER_TIMEOUT int = 60 // unit is second
const ERROR string = "[ERROR]: "
const FQ_TIMEOUT time.Duration = 5 * time.Second
const UNFINISHED = "UNFINISHED"

// config struct, which contains the quorum size of fast path and slow path
type Config struct {
	epoch     int // config generation
	fastSize  int //fast quorum
	classSize int // classical quorum
}

// The Transaction, which holds the metadata of transactions
type Transaction struct {
	config    Config
	in_trans  *pb.Trans // The core transaction, which contains the core info of transaction
	keys      []string  //Record all the keys used by the transactions
	ifTimeout bool
	// Only meaningful when ifTimeout == true, which labels the specific time to timeout
	endTime   *timestamppb.Timestamp
	votes     int                //total votes, different stage has different meaning
	fVotes    int                // votes satisfy the fast quorum
	deps      map[string]bool    // A deps set used for current stage
	couldFast bool               // true when still considering the fast path, false means use slow path
	failsNum  int                //Number of failed peers
	collectT  *pb.TransTimestamp // Used in the PreAccept which collects max t from PreAcceptOK

	acceptDeps    map[string]bool              //Used to collect deps in collecting AcceptOk stage
	acceptVotes   int                          // Count votes in AcceptOk stage
	readRes       map[int32][]*pb.SingleResult //store read result according to shards
	collectShards map[int32]bool               // store the already collected shards reads, used in readOk phase
	ifWait        bool                         // labels if the transaction is in the waiting status, only used in await of (Read/Apply)
	replyTo       int32                        // the same with ifWait, used only in read/apply phase
}

var tranStatusToString = map[pb.TranStatus]string{
	pb.TranStatus_New:         "TranStatus_New",
	pb.TranStatus_PreAccepted: "TranStatus_PreAccepted",
	pb.TranStatus_Accepted:    "TranStatus_Accepted",
	pb.TranStatus_Commited:    "TranStatus_Commited",
	pb.TranStatus_Applied:     "TranStatus_Applied",
}

// Extract the used keys of transaction and save it as string in the keys field of Transaction
func (tr *Transaction) updateKeys() {
	keySet := genKeySet(tr.in_trans)
	var keys []string
	for k, _ := range keySet {
		keys = append(keys, k)
	}
	tr.keys = keys
}

// Used by failure detector to monitor Peer status
// Should be init when the statemachine is created
// Will be modified by
// 1. tick msg: add timeout (main statemachine go routine)
// 2. heartResponse: clear timeout (The go routine performs the synchronous rpc call)
type PeerStatus struct {
	mu    sync.Mutex
	peers map[int32]*Peer
}

// Recorde peer's status
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

// Statemachine, which manages the whole status
type StateMachine struct {
	id      int32                   // Actual node will map the Id to actual address
	m_trans map[string]*Transaction // Managed transactions, because this state machine is its coordinator

	w_trans map[string]*Transaction // witnessed transactions, m_trans is a subset of this slice

	//Ballot number
	ballot int32 //Used in recovery, seen in paper
	//TODO shards
	//shards contain the sharding info of the cluster
	shards []*pb.ShardInfo
	//shards map, which maps nodeId to shardId
	shardMap map[int32]int32
	// Record the shard info of the current node
	curShard *pb.ShardInfo
	//In channel, which is the source of msgs
	inCh chan *pb.Message
	//Out channel, which is the same with its node
	outCh chan *pb.Message
	//Peer status, mainly about the indication of connection
	peerStatus *PeerStatus
	// T :recorded for witnessed transactions, concepts from Accord paper
	T map[string]*pb.TransTimestamp
	// conflicting keys, which register key - transaction relationship
	// Used for detecting conflicted transactions
	conflictMap map[string][]string
	//current tick number, when greater than 10, send heartbeat msg
	tickNum int
	//Underlying persistent layer
	db *badger.DB
	//DB's name
	dbName string
	//bufferedInCh chan *pb.Message // store the
	pq PriorityQueue //The heap which is used as reorder buffer, the concept is from Accord paper
	ct int32         // counter used for t0, t0 is from Accord paper
}

// Generate conflict trans
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
		if CompareTimestamp(timestamp, tarT0) {
			res = append(res, tarTrans.in_trans.Id)
		}
	}
	return res

}

// Timestamp's layout: (actual_time,seqId,pId), referred from Accord Paper
func (st *StateMachine) generateTimestamp() *pb.TransTimestamp {
	timeNow := time.Now()
	st.ct++
	t0 := &pb.TransTimestamp{
		TimeStamp: &timestamppb.Timestamp{
			Seconds: timeNow.Unix(),
			Nanos:   int32(timeNow.Nanosecond()),
		},
		Seq: st.ct,
		Id:  st.id,
	}
	return t0
}

// Convert timestamp to Id, the identifier of transaction
func (st *StateMachine) generateTransId(t *pb.TransTimestamp) *string {
	data, _ := proto.Marshal(t)
	hash := sha256.Sum256(data)
	hashString := fmt.Sprintf("%x", hash)
	return &hashString
}

// process Transaction submission from client
func (st *StateMachine) recvTrans(req *pb.Message) {

	//UnMarshall trans
	trans := &pb.Trans{}
	proto.Unmarshal(req.Data, trans)
	// Preprocess transactions
	trans.St = pb.TranStatus_PreAccepted
	// Calculate Electorates
	e := st.getRelatedReplicas(trans)
	//update the related replicas in the innerTrans
	trans.RelatedReplicas = e

	//Set electorates size of the transaction
	trans.EleSize = int32(len(e))

	//Will set to,Id fields and register in the sendPreAccept function
	preAccepts := st.sendPreAccept(e, trans)
	st.sendMsgs(preAccepts)
	//Begin to set timeout for fast quorum
	curTrans := st.m_trans[trans.Id]
	curTrans.couldFast = true
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

// Generate keySet of transaction
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
	for i := 0; i < 32; i++ {
		if b1[i] > b2[i] {
			return 1
		} else if b1[i] < b2[i] {
			return -1
		}
	}
	return 0
}

// Check if key belongs the byte range
func ifKeyInRange(key string, st []byte, end []byte) bool {
	hashKey := sha256.Sum256([]byte(key))
	hashed := make([]byte, 0, 32)
	for i := 0; i < 32; i++ {
		hashed = append(hashed, hashKey[i])
	}
	return compareByte(hashed, st) >= 0 && compareByte(hashed, end) < 0
}

// Check if key belongs to this shard
func (st *StateMachine) ifInShards(keySet map[string]bool, info *pb.ShardInfo) bool {
	for key, _ := range keySet {
		if ifKeyInRange(key, info.Start, info.End) {
			return true
		}
	}
	return false
}

// Basic version finding the electorates in preAccept
func (st *StateMachine) getRelatedReplicas(trans *pb.Trans) []int32 {
	var tars []int32
	keySet := genKeySet(trans)
	relatedShards := make([]int32, 0, 3)
	for i := 0; i < len(st.shards); i++ {
		if st.ifInShards(keySet, st.shards[i]) {
			tars = append(tars, st.shards[i].Replicas...)
			relatedShards = append(relatedShards, st.shards[i].ShardId)
		}
	}
	trans.RelatedShards = relatedShards
	return tars
}

type RegisterTransType int

const (
	Managed RegisterTransType = iota
	Witnessed
)

// Register transaction in statemachine
func (st *StateMachine) registerTrans(t RegisterTransType, trans *pb.Trans) {

	//update config

	tr := &Transaction{
		config: Config{
			fastSize:  int(3*trans.EleSize/4) + 1,
			classSize: int(trans.EleSize/2) + 1,
		},
		in_trans:      trans,
		keys:          make([]string, 0, 6),
		ifTimeout:     false,
		endTime:       nil,
		votes:         0,
		fVotes:        0,
		deps:          make(map[string]bool),
		couldFast:     true,
		failsNum:      0,
		collectT:      trans.T0,
		acceptDeps:    make(map[string]bool),
		acceptVotes:   0,
		readRes:       make(map[int32][]*pb.SingleResult),
		collectShards: make(map[int32]bool),
	}
	switch t {
	case Managed:
		// Because the managed statemachine will also recv PreAccept,
		//the witnessed will be update at that stage
		st.m_trans[trans.Id] = tr
		st.w_trans[trans.Id] = tr
		// Update the keys of the transaction, in case of finding conflicting trans
		tr.updateKeys()
		//Register conflicts
		st.registerClfs(tr)
		//update T
		st.T[trans.Id] = trans.T0
	case Witnessed:
		_, ok := st.m_trans[trans.Id]
		if ok {
			st.w_trans[trans.Id] = st.m_trans[trans.Id]
		} else {
			st.w_trans[trans.Id] = tr
			// Update the keys of the transaction, in case of finding conflicting trans
			tr.updateKeys()
			//Register conflicts
			st.registerClfs(tr)
			//try to update T
			_, ok := st.T[trans.Id]
			if !ok {
				st.T[trans.Id] = trans.ExT
			}
		}

	}
}

// Send PreAccept Request
func (st *StateMachine) sendPreAccept(tars []int32, trans *pb.Trans) []*pb.Message {
	// The transaction received doesn't have t0 and id
	var msgs []*pb.Message
	t0 := st.generateTimestamp()
	trans.Id = *st.generateTransId(t0)
	trans.T0 = t0
	//Init deps part
	trans.Deps = &pb.Deps{
		Items: make([]*pb.DepsItem, 0, 0),
	}
	log.Printf("Node %d generate t0 for submitted trans, which t0 is %s", st.id, t0.TimeStamp.AsTime().String())
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
			T0:   t0,
		})
	}
	//Update trans status
	trans.St = pb.TranStatus_PreAccepted
	return msgs
}

// Process tick message
func (st *StateMachine) processTick(msg *pb.Message) {
	//UnMarshal the msg

	tickMsg := &pb.TickMsg{}
	proto.Unmarshal(msg.Data, tickMsg)
	//check and deal with timeout transactions
	//Timeout for fast quorum
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
				tar.endTime = nil
				st.checkAndProcessSlowPath(tar)
			}

		}
	}
	//Update timeout attributes of transaction
	st.modifyAndCheck(-1, -1)

	//Send heart beats, which acts as a weak failure detector
	st.tickHeartbeat()

}

// Send heartbeat
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

// check timestamp equality
func equals(t1 *pb.TransTimestamp, t2 *pb.TransTimestamp) bool {
	tp1 := t1.TimeStamp.AsTime()
	tp2 := t2.TimeStamp.AsTime()
	return tp1.Equal(tp2) && t1.Seq == t2.Seq && t1.Id == t2.Id
}

// compare timestamp, true means greater
func CompareTimestamp(t1 *pb.TransTimestamp, t2 *pb.TransTimestamp) bool {
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

// Register keys in the conflict maps
func (st *StateMachine) registerClfs(curTrans *Transaction) {
	for i := 0; i < len(curTrans.keys); i++ {
		val, ok := st.conflictMap[curTrans.keys[i]]
		if !ok {
			val = make([]string, 0, 3)
		}
		val = append(val, curTrans.in_trans.Id)
		st.conflictMap[curTrans.keys[i]] = val
	}
}

// Process the PreAccept Request
func (st *StateMachine) processPreAccept(req *pb.Message) {
	var innerTrans *pb.Trans
	preAccept := &pb.PreAcceptReq{}
	proto.Unmarshal(req.Data, preAccept)
	innerTrans = preAccept.Trans
	innerTrans.T0 = preAccept.T0
	//Check is the msg is out of date
	_, ok := st.m_trans[innerTrans.Id]
	if ok && st.m_trans[innerTrans.Id].in_trans.St > innerTrans.St {
		log.Printf("Out of date msg on Node%d with type PreAccept, curSt is%s while comming is %s", st.id,
			tranStatusToString[st.w_trans[innerTrans.Id].in_trans.St], tranStatusToString[innerTrans.St])
		return
	}

	//Register the transaction as witnesses
	st.registerTrans(Witnessed, innerTrans)
	trans := st.w_trans[innerTrans.Id]
	//Update the transaction
	// Update the keys of the transaction, in case of finding conflicting trans
	// TODO Moved to registerTrans
	//trans.updateKeys()
	//check conflicts of transactions
	conflicts := st.getConflicts(trans)
	//TODO Register conflicts Moved to registerTrans
	//st.registerClfs(trans)
	// Update specific config for this Trans

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
		if !CompareTimestamp(tTrans, tarTimestamp) {
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
	deps := make([]*pb.DepsItem, 0, 3)
	for i := 0; i < len(cfl); i++ {
		cpTrans := st.w_trans[cfl[i]].in_trans
		tarT0 := cpTrans.T0
		if CompareTimestamp(t0, tarT0) {
			deps = append(deps, &pb.DepsItem{
				Id:     cfl[i],
				Shards: cpTrans.RelatedShards,
			})
		}
	}
	return &pb.Deps{Items: deps}
}

// Merge deps
func mergeDeps(d1 []*pb.DepsItem, d2 []*pb.DepsItem) []*pb.DepsItem {
	set := make(map[string]bool)
	res := make([]*pb.DepsItem, 0, 5)
	for i := 0; i < len(d1); i++ {
		_, ok := set[d1[i].Id]
		if !ok {
			set[d1[i].Id] = true
			res = append(res, d1[i])
		}
	}
	for i := 0; i < len(d2); i++ {
		_, ok := set[d2[i].Id]
		if !ok {
			set[d2[i].Id] = true
			res = append(res, d2[i])
		}
	}
	return res

}

// update corresponding deps in transaction with transId
func (st *StateMachine) updateDeps(cfl []*pb.DepsItem, trans *Transaction) {
	for i := 0; i < len(cfl); i++ {
		trans.deps[cfl[i].Id] = true
	}
	trans.in_trans.Deps.Items = mergeDeps(trans.in_trans.Deps.Items, cfl)
}

// Update the deps in transaction at AcceptOk stage, use acceptDeps in Transaction
func (st *StateMachine) updateDepsAcceptOk(cfl []*pb.DepsItem, trans *Transaction) {
	if trans.in_trans.St != pb.TranStatus_Accepted {
		return
	}
	for i := 0; i < len(cfl); i++ {
		trans.acceptDeps[cfl[i].Id] = true
	}
	trans.in_trans.Deps.Items = mergeDeps(trans.in_trans.Deps.Items, cfl)
}

// Flatten deps set to array
func (st *StateMachine) convDepsSet(depsSet map[string]bool) []string {
	var deps []string
	for k, _ := range depsSet {
		deps = append(deps, k)
	}
	return deps
}

// Generate Deps according to transId
func (st *StateMachine) genDepsWithTransId(curTrans *Transaction, ids []string) []*pb.DepsItem {
	deps := make([]*pb.DepsItem, 0, 3)
	for i := 0; i < len(ids); i++ {
		tarTrans := st.w_trans[ids[i]]
		deps = append(deps, &pb.DepsItem{
			Id:     ids[i],
			Shards: tarTrans.in_trans.RelatedShards,
		})
	}
	return deps
}

// Check if the condition for slow path is satisfied
func (st *StateMachine) checkAndProcessSlowPath(curTrans *Transaction) {
	// Check if is the correct stage
	if curTrans.in_trans.St != pb.TranStatus_PreAccepted &&
		curTrans.in_trans.St != pb.TranStatus_New {
		return
	}
	if curTrans.votes >= curTrans.config.classSize {
		//Update the final version of Execution time
		curTrans.in_trans.ExT = copyTransTimestamp(curTrans.collectT)
		//update T in statemachine
		st.T[curTrans.in_trans.Id] = curTrans.in_trans.ExT
		//update current trans status avoid further votes
		curTrans.in_trans.St = pb.TranStatus_Accepted
		accMsg := &pb.AcceptReq{
			Trans: curTrans.in_trans,
			ExT:   curTrans.collectT,
			Deps:  &pb.Deps{Items: curTrans.in_trans.Deps.Items},
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

	}
}

// Process PreAcceptOk
func (st *StateMachine) processPreAcceptOk(req *pb.Message) {
	var preAcceptOk *pb.PreAcceptResp
	preAcceptOk = &pb.PreAcceptResp{}
	proto.Unmarshal(req.Data, preAcceptOk)

	curTrans := st.m_trans[preAcceptOk.TransId]
	if !(curTrans.in_trans.St == pb.TranStatus_New ||
		curTrans.in_trans.St == pb.TranStatus_PreAccepted) {
		return
	}
	st.updateDeps(preAcceptOk.Deps.Items, curTrans)
	//Update classical votes
	curTrans.votes++
	//update collectT proposed by PreAcceptOK
	//TODO modify bugs here
	//curTrans.collectT = copyTransTimestamp(curTrans.in_trans.T0)
	if CompareTimestamp(preAcceptOk.T, curTrans.collectT) {
		curTrans.collectT = copyTransTimestamp(preAcceptOk.T)
	}
	//check if must slow path now
	//check if fast quorum is enough
	if curTrans.couldFast {
		//update fvotes
		if equals(preAcceptOk.T, curTrans.in_trans.T0) {
			curTrans.fVotes++
		}
		if curTrans.fVotes >= curTrans.config.fastSize {
			//perform the fast path logic
			curTrans.in_trans.St = pb.TranStatus_Commited
			st.triggerCheckOnAll()
			//update curTrans's execution time
			curTrans.in_trans.ExT = curTrans.in_trans.T0
			//update T in statemachine
			st.T[curTrans.in_trans.Id] = curTrans.in_trans.T0
			deps := &pb.Deps{Items: curTrans.in_trans.Deps.Items}
			commitMsg := &pb.CommitReq{
				Trans: curTrans.in_trans,
				ExT:   curTrans.in_trans.T0,
				Deps:  deps,
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
			//Perform execution logic accordding to Accord paper
			st.sendReads(curTrans, deps)

		}
	} else {

		st.checkAndProcessSlowPath(curTrans)

	}

}

// Process the Accept Message
func (st *StateMachine) processAccept(req *pb.Message) {

	accept := &pb.AcceptReq{}
	proto.Unmarshal(req.Data, accept)
	transId := accept.Trans.Id
	innerTrans := accept.Trans
	//Update the T in trans
	curTrans, ok := st.w_trans[transId]
	if !ok {
		st.registerTrans(Witnessed, accept.Trans)
		curTrans = st.w_trans[transId]
	}

	//check is msg is out of date
	if curTrans.in_trans.St > accept.Trans.St {
		log.Printf("Receive out of date msg on Node%d with type Accept, curSt is%s while comming is %s", st.id,
			tranStatusToString[st.w_trans[innerTrans.Id].in_trans.St], tranStatusToString[innerTrans.St])
		return
	}
	if CompareTimestamp(accept.ExT, st.T[transId]) {
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
		Deps:    &pb.Deps{Items: st.genDepsWithTransId(curTrans, fClfs)},
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

// Check if collect quorum of acceptOk
func (st *StateMachine) checkAcceptOkVotes(curTrans *Transaction) {
	if curTrans.in_trans.St != pb.TranStatus_Accepted {
		//TODO log?
		return
	}
	curTrans.acceptVotes++
	if curTrans.acceptVotes >= curTrans.config.classSize {
		//Pay attention, the Ext is the final decided version of collectT in outer Trans
		deps := &pb.Deps{Items: curTrans.in_trans.Deps.Items}
		commitMsg := &pb.CommitReq{
			Trans: curTrans.in_trans,
			ExT:   curTrans.in_trans.ExT,
			Deps:  deps,
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
		st.triggerCheckOnAll()
		//TODO Execute the Ecution Protocol
		st.sendReads(curTrans, deps)

	}
}

// Process AcceptOk message
func (st *StateMachine) processAcceptOk(req *pb.Message) {
	acceptOk := &pb.AcceptResp{}
	proto.Unmarshal(req.Data, acceptOk)
	curTrans := st.m_trans[acceptOk.TransId]
	st.updateDepsAcceptOk(acceptOk.Deps.Items, curTrans)
	st.checkAcceptOkVotes(curTrans)
}

// Deal with commit message
func (st *StateMachine) commitMessage(req *pb.Message) {
	commitMsg := &pb.CommitReq{}
	proto.Unmarshal(req.Data, commitMsg)
	transId := commitMsg.Trans.Id
	innerTrans := commitMsg.Trans
	//Modify the status of the transaction
	curTrans, ok := st.w_trans[transId]
	if !ok {
		st.registerTrans(Witnessed, commitMsg.Trans)
		curTrans = st.w_trans[transId]
	}
	if curTrans.in_trans.St > commitMsg.Trans.St {
		log.Printf("out of date msg on Node%d with type Commit, cur st is%s while comming st %s", st.id,
			tranStatusToString[st.w_trans[innerTrans.Id].in_trans.St], tranStatusToString[innerTrans.St])
		return
	}
	curTrans.in_trans.St = pb.TranStatus_Commited
	//Update Execution time for innerTrans
	curTrans.in_trans.ExT = commitMsg.ExT
	curTrans.in_trans.Deps = commitMsg.Deps
	//TODO Any other thing that need to be done?
	//Trigger other trans and make progress
	st.triggerCheckOnAll()
}

// Send read requests
func (st *StateMachine) sendReads(curTrans *Transaction, deps *pb.Deps) {
	// Save deps to innerTrans, because it is decided yet
	curTrans.in_trans.Deps = deps
	readMsg := &pb.ReadReq{
		Trans: curTrans.in_trans,
		ExT:   curTrans.in_trans.ExT,
		Deps:  deps,
	}
	data, _ := proto.Marshal(readMsg)
	tars := curTrans.in_trans.RelatedReplicas
	var msgs []*pb.Message
	for i := 0; i < len(tars); i++ {
		msgs = append(msgs, &pb.Message{
			Type: pb.MsgType_Read,
			Data: data,
			From: st.id,
			To:   tars[i],
		})
	}
	st.sendMsgs(msgs)

}

// Only get read keys of transactions
func (st *StateMachine) getReadKeys(trans *Transaction) []string {
	reads := make([]string, 0, 3)
	readOps := trans.in_trans.Reads
	for i := 0; i < len(readOps); i++ {
		reads = append(reads, readOps[i].Key)
	}
	return reads
}

// return read keys related to this shard
func (st *StateMachine) filterReadKey(trans *Transaction) []string {
	keys := st.getReadKeys(trans)
	fKeys := make([]string, 0, 3)
	shardInfo := st.curShard

	for i := 0; i < len(keys); i++ {
		if ifKeyInRange(keys[i], shardInfo.Start, shardInfo.End) {
			fKeys = append(fKeys, keys[i])
		}
	}
	return fKeys
}

// Check if two transaction share the same shard
func shareSameShard(tars []int32, cShardId int32) bool {
	for i := 0; i < len(tars); i++ {
		if tars[i] == cShardId {
			return true
		}
	}
	return false
}

// Check if read condition is satisfied
func (st *StateMachine) checkReadCondition(curTrans *Transaction) {
	//Only check trans which sets the ifWait attribute
	if !curTrans.ifWait {
		return

	}
	//Check if await condition satisfied
	//TODO check commit await
	curDeps := curTrans.in_trans.Deps.Items
	//Current shard, only deps related to this shard is considered
	cShard := st.curShard.ShardId
	for i := 0; i < len(curDeps); i++ {
		tarId := curDeps[i].Id
		if shareSameShard(curDeps[i].Shards, cShard) {
			tarTrans, ok := st.w_trans[tarId]
			if !ok {
				return
			}
			if tarTrans.in_trans.St == pb.TranStatus_Applied ||
				tarTrans.in_trans.St == pb.TranStatus_Commited {
				if CompareTimestamp(curTrans.in_trans.ExT, tarTrans.in_trans.ExT) {
					if tarTrans.in_trans.St == pb.TranStatus_Commited {
						return
					}
				}

			} else {
				return
			}

		}
	}
	//Check Applied await, seen in Accord paper

	fKeys := st.filterReadKey(curTrans)

	reads := make([]*pb.SingleResult, 0, 3)
	st.db.View(func(txn *badger.Txn) error {
		var tmp []*pb.SingleResult
		for i := 0; i < len(fKeys); i++ {
			item, err := txn.Get([]byte(fKeys[i]))
			if err != nil {
				return err
			} else {

				valCopy, _ := item.ValueCopy([]byte{})
				tmp = append(tmp, &pb.SingleResult{
					Key: fKeys[i],
					Val: string(valCopy),
				})
			}
		}
		reads = append(reads, tmp...)
		return nil

	})
	//read condition satisfied, reset ifWait flag
	curTrans.ifWait = false
	//Prepare readOk msgs
	readOk := &pb.ReadResp{
		Res:     reads,
		TransId: curTrans.in_trans.Id,
	}
	data, _ := proto.Marshal(readOk)
	msgs := make([]*pb.Message, 0, 1)
	msgs = append(msgs, &pb.Message{
		Type: pb.MsgType_ReadOk,
		Data: data,
		From: st.id,
		To:   curTrans.replyTo,
	})
	st.sendMsgs(msgs)
}

// Perform read operations, the execution logic
// Current version needs to filter out the deps which is not on this shard
// Because the node and shard is one-to-one relationship
func (st *StateMachine) processRead(req *pb.Message) {
	readMsg := &pb.ReadReq{}
	proto.Unmarshal(req.Data, readMsg)
	curTrans, ok := st.w_trans[readMsg.Trans.Id]
	if !ok {
		st.registerTrans(Witnessed, readMsg.Trans)
		curTrans = st.w_trans[readMsg.Trans.Id]
	}
	if curTrans.in_trans.St > readMsg.Trans.St {
		return
	}
	//update innerTrans, Because there will be out order msg here
	curTrans.in_trans = readMsg.Trans
	//Update deps locally according to Read request
	curTrans.in_trans.Deps = readMsg.Deps
	//update the outer transaction into waiting status, labeled by ifWait attribute
	curTrans.ifWait = true
	//update replyTo attribute of outer Trans
	curTrans.replyTo = req.From
	st.checkReadCondition(curTrans)

}

// Get replicas (nodes) of the specific shard
func (st *StateMachine) getReplicasOfShard(shardId int32) []int32 {
	shards := st.shards
	tars := make([]int32, 0, 3)
	for i := 0; i < len(shards); i++ {
		if shardId == shards[i].ShardId {
			tars = append(tars, shards[i].Replicas...)
			break
		}
	}
	return tars
}

// Merge read results
func (tr *Transaction) mergeReadRes() []*pb.SingleResult {
	res := make([]*pb.SingleResult, 0, 6)
	for k, _ := range tr.readRes {
		res = append(res, tr.readRes[k]...)
	}
	return res

}

// Process readOk
func (st *StateMachine) processReadOk(req *pb.Message) {

	readOk := &pb.ReadResp{}
	proto.Unmarshal(req.Data, readOk)
	curTrans := st.m_trans[readOk.TransId]
	//TODO
	fromShard := st.shardMap[req.From]
	_, ok := curTrans.collectShards[fromShard]
	if ok {
		//filtered already counted shard
		return
	} else {
		curTrans.collectShards[fromShard] = true
		//Update ReadRes, collect all read responses
		partialReads := make([]*pb.SingleResult, 0, 6)
		partialReads = append(partialReads, readOk.Res...)
		curTrans.readRes[fromShard] = partialReads
		// Send Apply request to replicas of these shards
		tars := st.getReplicasOfShard(fromShard)
		applyMsg := &pb.ApplyReq{
			Trans: curTrans.in_trans,
			ExT:   curTrans.in_trans.ExT,
			DepsP: curTrans.in_trans.Deps,
			Res:   readOk.Res,
		}
		data, _ := proto.Marshal(applyMsg)
		msgs := make([]*pb.Message, 0, 3)
		for i := 0; i < len(tars); i++ {
			msgs = append(msgs, &pb.Message{
				Type: pb.MsgType_Apply,
				Data: data,
				From: st.id,
				To:   tars[i],
			})
		}
		st.sendMsgs(msgs)

	}
	//Check if all data is collected, from different shards
	//and return results to clients
	if len(curTrans.collectShards) == len(curTrans.in_trans.RelatedShards) {
		//Label on the managed node, the status as applied
		//TODO Only being labeled when current node is not in the trans related node
		ifIn := false
		for i := 0; i < len(curTrans.in_trans.RelatedReplicas); i++ {
			if curTrans.in_trans.RelatedReplicas[i] == st.id {
				ifIn = true
				break
			}
		}
		if !ifIn {
			curTrans.in_trans.St = pb.TranStatus_Applied
			st.triggerCheckOnAll()
		}
		//Return results to clients
		finalRes := &pb.FinalRes{
			CId: curTrans.in_trans.CId,
			Res: curTrans.mergeReadRes(),
			Tar: curTrans.in_trans.ClientInfo.Addr,
		}
		data, _ := proto.Marshal(finalRes)
		msgs := make([]*pb.Message, 0, 1)
		msgs = append(msgs, &pb.Message{
			Type: pb.MsgType_FinalResult,
			Data: data,
			From: st.id,
			To:   100,
		})
		st.sendMsgs(msgs)

	}
}

// Filter out the unrelated writes
func (st *StateMachine) filterWrite(curTrans *Transaction) []*pb.WriteOp {
	writes := curTrans.in_trans.Writes
	curShard := st.curShard
	fWrites := make([]*pb.WriteOp, 0, 3)
	for i := 0; i < len(writes); i++ {
		if ifKeyInRange(writes[i].Key, curShard.Start, curShard.End) {
			fWrites = append(fWrites, writes[i])
		}
	}
	return fWrites
}

// Write writes to db
func (st *StateMachine) writeRes(writes []*pb.WriteOp) error {
	db := st.db
	err := db.Update(func(txn *badger.Txn) error {
		for i := 0; i < len(writes); i++ {
			err := txn.Set([]byte(writes[i].Key), []byte(writes[i].Val))
			if err != nil {
				return err
			}
		}
		return nil
	})
	return err
}

// Convert deps to string, for debug and log
func depsToString(items []*pb.DepsItem) string {
	s := "Deps is "
	for i := 0; i < len(items); i++ {
		s = s + "   " + items[i].Id
	}
	return s
}

// Check is Apply condition is satisfied
func (st *StateMachine) checkApplyCondition(curTrans *Transaction) {
	//Check if await condition satisfied
	//Check commit await
	if !curTrans.ifWait {
		return
	}
	curDeps := curTrans.in_trans.Deps.Items
	//Current shard, only deps related to this shard is considered
	cShard := st.curShard.ShardId
	for i := 0; i < len(curDeps); i++ {
		tarId := curDeps[i].Id

		if tarId == curTrans.in_trans.Id {
			continue
		}
		if shareSameShard(curDeps[i].Shards, cShard) {
			tarTrans, ok := st.w_trans[tarId]
			if !ok {
				return
			}
			if tarTrans.in_trans.St == pb.TranStatus_Applied ||
				tarTrans.in_trans.St == pb.TranStatus_Commited {
				if CompareTimestamp(curTrans.in_trans.ExT, tarTrans.in_trans.ExT) {
					if tarTrans.in_trans.St == pb.TranStatus_Commited {
						return
					}
				}

			} else {
				return
			}

		}
	}
	// filtered the writes
	fWrites := st.filterWrite(curTrans)
	//write to persistent layer
	for {
		err := st.writeRes(fWrites)
		if err == nil {
			curTrans.in_trans.St = pb.TranStatus_Applied
			curTrans.ifWait = false
			if len(fWrites) > 0 {
				//log.Printf("Write %s on node%d, whose deps is %s", fWrites[0].Val, st.id, depsToString(curDeps))
			}
			log.Printf("[Applied :] Node %d Execution time is %s converting %s", st.id, curTrans.in_trans.ExT.String(), curTrans.in_trans.ExT.TimeStamp.String())
			st.triggerCheckOnAll()
			return
		} else {
			log.Printf(ERROR+"Could not write to db on node%d", st.id)
		}
	}

}

// Process apply message
// Applied transaction will trigger other transactions
func (st *StateMachine) processApply(req *pb.Message) {
	applyMsg := &pb.ApplyReq{}
	proto.Unmarshal(req.Data, applyMsg)
	curTrans, ok := st.w_trans[applyMsg.Trans.Id]
	if !ok {
		st.registerTrans(Witnessed, applyMsg.Trans)
		curTrans = st.w_trans[applyMsg.Trans.Id]
	} else {
		if curTrans.in_trans.St >= pb.TranStatus_Applied {
			log.Printf("Repeated Apply message from node %d", req.From)
			return
		}
	}
	if curTrans.in_trans.St > applyMsg.Trans.St {
		return
	}
	//Update inner Trans, in case of out of order msg
	curTrans.in_trans = applyMsg.Trans
	//Update deps of locally trans
	curTrans.in_trans.Deps = applyMsg.DepsP
	//update ifWait attribute of outer transaction
	curTrans.ifWait = true
	//update replyTo attribute of outer Trans
	curTrans.replyTo = req.From

	st.checkApplyCondition(curTrans)

}

// Trigger to check if other transactions could be processes
func (st *StateMachine) triggerCheckOnAll() {
	transAll := st.w_trans
	for k, _ := range transAll {
		tarTrans := transAll[k]
		if tarTrans.in_trans.St == pb.TranStatus_Commited {
			st.checkReadCondition(tarTrans)
			st.checkApplyCondition(tarTrans)
		}

	}
}

// Receive heartbeat response
func (st *StateMachine) recvHeartbeatResponse(nodeId int32) {
	//TODO update the node status monitored by statemachine
	st.modifyAndCheck(PEER_TIMEOUT, nodeId)
}

// Execute incoming messages, which changes the status of statemachine
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
		//log.Printf("Execute logics for tick")
		st.processTick(req)
	case pb.MsgType_SubmitTrans:
		log.Printf("Execute SubmitTrans request on node%d", st.id)
		st.recvTrans(req)
	}
}

// Store the statemachine
func (st *StateMachine) storeStatemachine() {
	// create a buffer to encode the struct
	buf := new(bytes.Buffer)
	// create an encoder and encode the struct
	enc := gob.NewEncoder(buf)
	err := enc.Encode(st)
	if err != nil {
		panic(err)
	}
	// create a decoder and decode the struct from the buffer
	dec := gob.NewDecoder(buf)
	newSt := StateMachine{}
	err = dec.Decode(&newSt)
	if err != nil {
		panic(err)
	}
}

// Main loop of stateMachine
func (st *StateMachine) mainLoop(inCh chan *pb.Message, outCh chan *pb.Message) {
	for {
		val, ok := <-inCh
		if !ok {
			log.Printf("The channel of the node has been closed, nodeId is %d", st.id)
		}
		st.executeReq(val)

	}
}

// Define a slice of Items that implements the heap.Interface
type PriorityQueue []*pb.Message

func (pq PriorityQueue) Len() int {
	return len(pq)
}
func CpTimestamp(t1 *pb.TransTimestamp, t2 *pb.TransTimestamp) bool {
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
func (pq PriorityQueue) Less(i, j int) bool {
	// Use the custom comparison function to determine which item has higher priority
	return CpTimestamp(pq[j].T0, pq[i].T0)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	item := x.(*pb.Message)

	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1] // for safety
	*pq = old[0 : n-1]
	return item
}

// TODO only for test
func (st *StateMachine) findMinTrans() string {
	tp := st.generateTimestamp()
	var s string
	for i, _ := range st.m_trans {
		ct := st.m_trans[i]
		if ct.in_trans.St == pb.TranStatus_Commited {
			ntp := ct.in_trans.ExT
			if CompareTimestamp(tp, ntp) {
				tp = copyTransTimestamp(ntp)
				s = ct.in_trans.Id
			}
		}
	}
	return s
}
func (st *StateMachine) checkunAppliedTrans() {
	for i, _ := range st.m_trans {
		if st.m_trans[i].in_trans.St != pb.TranStatus_Applied {
			log.Printf(UNFINISHED+" On node%d the trans %s, current st is %s", st.id, st.m_trans[i].in_trans.Id,
				tranStatusToString[st.m_trans[i].in_trans.St])
			if i == "asdw" {
				st.findMinTrans()
			}

			//TODO tmp for debug
			st.triggerCheckOnAll()
			//TODO Because here we just make a check
			// Do not block too much IO
			break
		}
	}
}

// The reorder version that buffered the message
func (st *StateMachine) reoderMainLoop(inCh chan *pb.Message, outCh chan *pb.Message) {
	ticker := time.NewTicker(100 * time.Millisecond)
	//TODO to be removed
	logCt := 0
	for {
		select {
		// This ticker is only used in this scope
		case <-ticker.C:
			//check heap first
			now := time.Now()
			for st.pq.Len() > 0 {
				fir := st.pq[0]
				minTime := now.Add(-2 * time.Second)
				maxTime := now.Add(-1 * time.Second)
				recTime := fir.T0.TimeStamp.AsTime()
				if recTime.After(maxTime) {
					break
				} else if recTime.Before(minTime) {
					log.Printf(ERROR + "Out of scope time in buffered array")
				} else {
					st.executeReq(heap.Pop(&st.pq).(*pb.Message))
				}
				/*
					minTime := now.Add(-1 * time.Second)
					maxTime := now.Add(time.Second)
					recTime := fir.T0.TimeStamp.AsTime()
					if recTime.After(maxTime) {
						break
					} else if recTime.Before(minTime) {
						log.Printf(ERROR + "Out of scope time in buffered array")
					} else {
						st.executeReq(heap.Pop(&st.pq).(*pb.Message))
					}

				*/

			}
			logCt++
			if logCt%100 == 0 {
				st.checkunAppliedTrans()
			}
			//TODO This function is only used in debug
			//st.checkunAppliedTrans()
		case val, ok := <-inCh:
			if !ok {
				log.Printf("The channel of the node has been closed, nodeId is %d", st.id)
			}
			if val.T0 != nil {
				//put into the heap
				heap.Push(&st.pq, val)
				continue
			} else {
				st.executeReq(val)
			}

		}

	}
}
