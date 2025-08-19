// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"strings"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// CampaignType represents the type of campaigning
// the reason we use the type of string instead of uint64
// is because it's simpler to compare and fill in raft entries
type CampaignType string

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

// Possible values for CampaignType
const (
	// campaignPreElection represents the first phase of a normal election when
	// Config.PreVote is true.
	campaignPreElection CampaignType = "CampaignPreElection"
	// campaignElection represents a normal (time-based) election (the second phase
	// of the election when Config.PreVote is true).
	campaignElection CampaignType = "CampaignElection"
	// campaignTransfer represents the type of leader transfer
	campaignTransfer CampaignType = "CampaignTransfer"
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	// Logger is the logger used for raft log. For multinode which can host
	// multiple raft group, each raft group can have its own logger
	logger Logger
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).

	r := &Raft{
		id:               c.ID,
		Lead:             None,
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
	}

	//raft.Term = 0
	//raft.Vote = raft.id
	r.Prs = make(map[uint64]*Progress)
	r.RaftLog = new(RaftLog)
	r.votes = make(map[uint64]bool)

	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.Prs {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.RaftLog.committed, r.RaftLog.applied, r.RaftLog.LastIndex(), r.RaftLog.LastIndex())
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	return false
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.Step(pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).

	if r.State == StateLeader {
		r.heartbeatElapsed++
	}

	r.electionElapsed++
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	//r.step = stepFollower
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch r.State {
	case StateFollower:
		switch m.MsgType {
		case pb.MessageType_MsgPropose:
			if r.Lead == None {
				r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
				return ErrProposalDropped
			}
			m.To = r.Lead
			r.send(m)
		case pb.MessageType_MsgAppend:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleAppendEntries(m)
		case pb.MessageType_MsgHeartbeat:
			r.electionElapsed = 0
			r.Lead = m.From
			r.handleHeartbeat(m)
		//`case pb.MsgSnap:
		//`	r.electionElapsed = 0
		//`	r.lead = m.From
		//`	r.handleSnapshot(m)
		//case pb.MsgTransferLeader:
		//	if r.lead == None {
		//		r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
		//		return nil
		//	}
		//	m.To = r.lead
		//	r.send(m)
		case pb.MessageType_MsgTimeoutNow:
			r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			// Leadership transfers never use pre-vote even if r.preVote is true; we
			// know we are not recovering from a partition so there is no need for the
			// extra round trip.
			r.hup(campaignTransfer)
			//case pb.MsgReadIndex:
			//	if r.lead == None {
			//		r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			//		return nil
			//	}
			//	m.To = r.lead
			//	r.send(m)
			//case pb.MsgReadIndexResp:
			//	if len(m.Entries) != 1 {
			//		r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			//		return nil
			//	}
			//	r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})
		}
	case StateCandidate:
	case StateLeader:
	}
	return nil
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	// TODO random election timeout

	r.votes = make(map[uint64]bool)
	for id, p := range r.Prs {
		p = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
		if id == r.id {
			p.Match = r.RaftLog.LastIndex()
		}
	}
}

// send schedules persisting state to a stable storage and AFTER that
// sending the message (as part of next Ready message processing).
func (r *Raft) send(m pb.Message) {
	if m.From == None {
		m.From = r.id
	}
	if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgRequestVoteResponse {
		if m.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			// - MsgPreVote: m.Term is the term the node will campaign,
			//   non-zero as we use m.Term to indicate the next term we'll be
			//   campaigning for
			// - MsgPreVoteResp: m.Term is the term received in the original
			//   MsgPreVote if the pre-vote was granted, non-zero for the
			//   same reasons MsgPreVote is
			panic(fmt.Sprintf("term should be set when sending %s", m.MsgType))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.MsgType, m.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) hup(t CampaignType) {
	if r.State == StateLeader {
		r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		return
	}

	if !r.promotable() {
		r.logger.Warningf("%x is unpromotable and can not campaign", r.id)
		return
	}
	//ents, err := r.RaftLog.slice(r.RaftLog.applied+1, r.RaftLog.committed+1, noLimit)
	//if err != nil {
	//	r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
	//}
	//if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
	//	r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
	//	return
	//}

	r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
	r.campaign(t)
}

// promotable indicates whether state machine can be promoted to leader,
// which is true when its own id is in progress list.
func (r *Raft) promotable() bool {
	_, ok := r.Prs[r.id]
	return ok
}

// campaign transitions the raft instance to candidate state. This must only be
// called after verifying that this is a legitimate transition.
func (r *Raft) campaign(t CampaignType) {
	// if !r.promotable() {
	// 	// This path should not be hit (callers are supposed to check), but
	// 	// better safe than sorry.
	// 	r.logger.Warningf("%x is unpromotable; campaign() should have been called", r.id)
	// }
	// var term uint64
	// var voteMsg pb.MessageType
	// if t == campaignPreElection {
	// 	r.becomePreCandidate()
	// 	voteMsg = pb.MsgPreVote
	// 	// PreVote RPCs are sent for the next term before we've incremented r.Term.
	// 	term = r.Term + 1
	// } else {
	// 	r.becomeCandidate()
	// 	voteMsg = pb.MsgVote
	// 	term = r.Term
	// }
	// if _, _, res := r.poll(r.id, voteRespMsgType(voteMsg), true); res == quorum.VoteWon {
	// 	// We won the election after voting for ourselves (which must mean that
	// 	// this is a single-node cluster). Advance to the next state.
	// 	if t == campaignPreElection {
	// 		r.campaign(campaignElection)
	// 	} else {
	// 		r.becomeLeader()
	// 	}
	// 	return
	// }
	// var ids []uint64
	// {
	// 	idMap := r.prs.Voters.IDs()
	// 	ids = make([]uint64, 0, len(idMap))
	// 	for id := range idMap {
	// 		ids = append(ids, id)
	// 	}
	// 	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	// }
	// for _, id := range ids {
	// 	if id == r.id {
	// 		continue
	// 	}
	// 	r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
	// 		r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

	// 	var ctx []byte
	// 	if t == campaignTransfer {
	// 		ctx = []byte(t)
	// 	}
	// 	r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
	// }
}
