package raft

import (
	"log"
	"sync"
	"testing"
	"time"
)

type Harness struct {
	mu sync.Mutex

	// list of all raft servers
	cluster []*Server

	// list of all raft servers'channel channels
	commitChans []chan CommitEntry

	// list of all raft servers'commit entries
	commits [][]CommitEntry

	// bool per server in cluster, specifying whether his server is currently connected to peer
	connected []bool

	n int
	t *testing.T
}

func NewHarness(t *testing.T, n int) *Harness {
	ns := make([]*Server, n)
	connected := make([]bool, n)
	commitChans := make([]chan CommitEntry, n)
	commits := make([][]CommitEntry, n)
	ready := make(chan interface{})

	// create all servers in this cluster, assign ids and peer ids
	for i := 0; i < n; i++ {
		peerIds := make([]int, 0)
		for p := 0; p < n; p++ {
			if p != i {
				peerIds = append(peerIds, p)
			}
		}

		commitChans[i] = make(chan CommitEntry)
		ns[i] = NewServer(i, peerIds, ready, commitChans[i])
		ns[i].Serve()
	}

	// connect all peers to each other
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				ns[i].ConnectToPeer(j, ns[j].GetListenAddr())
			}
		}
		connected[i] = true
	}

	close(ready)

	h := &Harness{
		cluster:     ns,
		commitChans: commitChans,
		commits:     commits,
		connected:   connected,
		n:           n,
		t:           t,
	}
	for i := 0; i < n; i++ {
		go h.collectCommit(i)
	}

	return h
}

// disconnect a server from all other servers in the cluster.
func (h *Harness) DisconnectPeer(id int) {
	tlog("Disconnect %d", id)
	h.cluster[id].DisconnectAll()
	for j := 0; j < h.n; j++ {
		if j != id {
			h.cluster[j].DisconnectPeer(id)
		}
	}
	h.connected[id] = false
}

// connects a server to all other servers in the cluster.
func (h *Harness) ReconnectPeer(id int) {
	tlog("Reconnect %d", id)
	for j := 0; j < h.n; j++ {
		if j != id {
			if err := h.cluster[id].ConnectToPeer(j, h.cluster[j].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
			if err := h.cluster[j].ConnectToPeer(id, h.cluster[id].GetListenAddr()); err != nil {
				h.t.Fatal(err)
			}
		}
	}
	h.connected[id] = true
}

// checks that only a single server thinks it is the leader.
func (h *Harness) CheckSingleLeader() (int, int) {
	// retries serveral times if no leader is identified yet.
	for r := 0; r < 5; r++ {
		leaderId := -1
		leaderTerm := -1
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				_, term, isLeader := h.cluster[i].cm.Report()
				if isLeader {
					if leaderId < 0 {
						leaderId = i
						leaderTerm = term
					} else {
						h.t.Fatalf("both %d and %d think they're leaders", leaderId, i)
					}
				}
			}
		}
		if leaderId >= 0 {
			return leaderId, leaderTerm
		}
		// sleep for some time before retrying.
		sleepMs(150)
	}

	h.t.Fatalf("leader not found")
	return -1, -1
}

// checks that no connected server considers itself the leader.
func (h *Harness) CheckNoLeader() {
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			_, _, isLeader := h.cluster[i].cm.Report()
			if isLeader {
				h.t.Fatalf("server %d leader; want none", i)
			}
		}
	}
}

func (h *Harness) Shutdown() {
	for i := 0; i < h.n; i++ {
		h.cluster[i].DisconnectAll()
		h.connected[i] = false
	}
	for i := 0; i < h.n; i++ {
		h.cluster[i].Shutdown()
	}
	for i := 0; i < h.n; i++ {
		close(h.commitChans[i])
	}
}

func (h *Harness) SubmitToServer(serverId int, cmd interface{}) bool {
	return h.cluster[serverId].cm.Submit(cmd)
}

// read commitChans of all servers and add them to corresponding commits.
func (h *Harness) collectCommit(i int) {
	for c := range h.commitChans[i] {
		h.mu.Lock()
		tlog("collectCommit(%d) got %+v", i, c)
		h.commits[i] = append(h.commits[i], c)
		h.mu.Unlock()
	}
}

func (h *Harness) CheckCommittedN(cmd int, n int) {
	nc, _ := h.CheckCommitted(cmd)
	if nc != n {
		h.t.Errorf("CheckCommittedN got nc=%d, want %d", nc, n)
	}
}

// verifies that connected servers have cmd committed with the same index.
// it also verifies that all commands before cmd in the commit sequence match.
func (h *Harness) CheckCommitted(cmd int) (nc int, index int) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// find length of commits
	commitsLen := -1
	for i := 0; i < h.n; i++ {
		if h.connected[i] {
			if commitsLen >= 0 {
				if len(h.commits[i]) != commitsLen {
					h.t.Fatalf("commits[%d] = %d, commitsLen = %d", i, h.commits[i], commitsLen)
				}
			} else {
				commitsLen = len(h.commits[i])
			}
		}
	}

	for c := 0; c < commitsLen; c++ {
		cmdAtC := -1
		// check consistency of command
		for i := 0; i < h.n; i++ {
			if h.connected[i] {
				cmdOfN := h.commits[i][c].Command.(int)
				if cmdAtC >= 0 {
					if cmdOfN != cmdAtC {
						h.t.Errorf("got %d, want %d at h.commits[%d][%d]", cmdOfN, cmdAtC, i, c)
					}
				} else {
					cmdAtC = cmdOfN
				}
			}
		}
		// found cmd in commit sequence.
		if cmdAtC == cmd {
			// check consistency of index
			index := -1
			nc := 0
			for i := 0; i < h.n; i++ {
				if h.connected[i] {
					if index >= 0 && h.commits[i][c].Index != index {
						h.t.Errorf("got Index=%d, want %d at h.commits[%d][%d]", h.commits[i][c].Index, index, i, c)
					} else {
						index = h.commits[i][c].Index
					}
					nc++
				}
			}
			return nc, index
		}
	}

	h.t.Errorf("cmd=%d not found in commits", cmd)
	return -1, -1
}

func tlog(format string, a ...interface{}) {
	format = "[TEST] " + format
	log.Printf(format, a...)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
