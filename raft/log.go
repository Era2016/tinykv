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

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
	logger Logger

	offset uint64
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	return nil
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	return nil
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	return nil
}

func (l *RaftLog) FirstIndex() uint64 {
	return 0
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if ll := len(l.entries); ll > 0 {
		return l.entries[ll-1].Index
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if i > l.LastIndex() {
		return 0, errors.New("invalid index")
	}
	return l.entries[i].Term, nil
}

// slice returns a slice of log entries from lo through hi-1, inclusive.
func (l *RaftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	if lo < l.offset {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.offset))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.offset))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}

		// check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, l.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}
	if hi > l.offset {
		//	unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		unstable := l.entriesSlice(max(lo, l.offset), hi)
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstable))
			n := copy(combined, ents)
			copy(combined[n:], unstable)
			ents = combined
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *RaftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.FirstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.LastIndex() + 1 - fi
	if hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.LastIndex())
	}
	return nil
}

func (l *RaftLog) entriesSlice(lo, hi uint64) []pb.Entry {
	l.entriesMustCheckOutOfBounds(lo, hi)
	return l.entries[lo-l.offset : hi-l.offset]
}

// u.offset <= lo <= hi <= u.offset+len(u.entries)
func (l *RaftLog) entriesMustCheckOutOfBounds(lo, hi uint64) {
	if lo > hi {
		l.logger.Panicf("invalid unstable.slice %d > %d", lo, hi)
	}
	upper := l.offset + uint64(len(l.entries))
	if lo < l.offset || hi > upper {
		l.logger.Panicf("unstable.slice[%d,%d) out of bound [%d,%d]", lo, hi, l.offset, upper)
	}
}
