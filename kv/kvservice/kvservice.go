package kvservice

import (
	"context"
	"encoding/gob"
	"fmt"
	"kv/api"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"raft"
)

const DebugKV = 1

type KVService struct {
	sync.Mutex

	id int

	rs *raft.Server

	commitChan chan raft.CommitEntry

	commitSubs map[int]chan Command

	ds *DataStore

	srv *http.Server
}

func New(id int, peerIds []int, storage raft.Storage, readyChan <-chan any) *KVService {
	gob.Register(Command{})
	commitChan := make(chan raft.CommitEntry)

	// raft server
	rs := raft.NewServer(id, peerIds, storage, readyChan, commitChan)
	rs.Serve()
	kvs := &KVService{
		id:         id,
		rs:         rs,
		commitChan: commitChan,
		ds:         NewDataStore(),
		commitSubs: make(map[int]chan Command),
	}

	kvs.runUpdater()
	return kvs
}

func (kvs *KVService) runUpdater() {
	go func() {
		// watches commit channel for committed commands
		for entry := range kvs.commitChan {
			cmd := entry.Command.(Command)

			// apply command to data store
			switch cmd.Kind {
			case CommandGet:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
			case CommandPut:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
			case CommandCAS:
				cmd.ResultValue, cmd.ResultFound = kvs.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
			default:
				panic(fmt.Errorf("unexpected command %v", cmd))
			}

			// forward to subscriber of the command based on index
			if sub := kvs.popCommitSubcription(entry.Index); sub != nil {
				sub <- cmd
				close(sub)
			}
		}
	}()
}

func (kvs *KVService) ServeHTTP(port int) {
	if kvs.srv != nil {
		panic("ServeHTTP called")
	}

	// http server
	mux := http.NewServeMux()
	mux.HandleFunc("POST /get/", kvs.handleGet)
	mux.HandleFunc("POST /put/", kvs.handlePut)
	mux.HandleFunc("POST /cas/", kvs.handleCAS)

	kvs.srv = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}

	// start http server
	go func() {
		kvs.kvlog("serving HTTP on %s", kvs.srv.Addr)
		if err := kvs.srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatal(err)
		}
		kvs.srv = nil
	}()
}

func (kvs *KVService) handlePut(w http.ResponseWriter, req *http.Request) {
	// deserialize put request
	pr := &api.PutRequest{}
	if err := readRequestJSON(req, pr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP PUT %v", pr)

	// submit command to raft server
	cmd := Command{
		Kind:  CommandPut,
		Key:   pr.Key,
		Value: pr.Value,
		Id:    kvs.id,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		renderJSON(w, api.PutResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// subcribe for commit update for the log index.
	sub := kvs.createCommitSubscription(logIndex)

	// wait on subscription channel
	select {
	case commitCmd := <-sub:
		// check if command belong to kvs
		if commitCmd.Id == kvs.id {
			renderJSON(w, api.PutResponse{
				RespStatus: api.StatusOK,
				KeyFound:   commitCmd.ResultFound,
				PrevValue:  commitCmd.ResultValue,
			})
		} else {
			renderJSON(w, api.PutResponse{RespStatus: api.StatusFailedCommit})
		}

	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleGet(w http.ResponseWriter, req *http.Request) {
	gr := &api.GetRequest{}
	if err := readRequestJSON(req, gr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP GET %v", gr)

	cmd := Command{
		Kind: CommandGet,
		Key:  gr.Key,
		Id:   kvs.id,
	}

	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		renderJSON(w, api.GetResponse{RespStatus: api.StatusNotLeader})
		return
	}

	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.Id == kvs.id {
			renderJSON(w, api.GetResponse{
				RespStatus: api.StatusOK,
				KeyFound:   commitCmd.ResultFound,
				Value:      commitCmd.ResultValue,
			})
		} else {
			renderJSON(w, api.GetResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleCAS(w http.ResponseWriter, req *http.Request) {

}

func (kvs *KVService) createCommitSubscription(logIndex int) chan Command {
	kvs.Lock()
	defer kvs.Unlock()

	if _, exists := kvs.commitSubs[logIndex]; exists {
		panic(fmt.Sprintf("duplicate commit subscription for logIndex=%d", logIndex))
	}

	ch := make(chan Command, 1)
	kvs.commitSubs[logIndex] = ch
	return ch
}

func (kvs *KVService) popCommitSubcription(logIndex int) chan Command {
	kvs.Lock()
	defer kvs.Unlock()

	ch := kvs.commitSubs[logIndex]
	delete(kvs.commitSubs, logIndex)
	return ch
}

func (kvs *KVService) kvlog(format string, args ...any) {
	if DebugKV > 0 {
		format = fmt.Sprintf("[kv %d] ", kvs.id) + format
		log.Printf(format, args...)
	}
}

func (kvs *KVService) ConnectToRaftPeer(peerId int, addr net.Addr) error {
	return kvs.rs.ConnectToPeer(peerId, addr)
}

func (kvs *KVService) GetRaftListenAddr() net.Addr {
	return kvs.rs.GetListenAddr()
}

func (kvs *KVService) IsLeader() bool {
	return kvs.rs.IsLeader()
}

func (kvs *KVService) DisconnectFromAllRaftPeers() {
	kvs.rs.DisconnectAll()
}

func (kvs *KVService) Shutdown() error {
	kvs.kvlog("shutting down Raft server")
	kvs.rs.Shutdown()
	kvs.kvlog("closing commitChan")
	close(kvs.commitChan)

	if kvs.srv != nil {
		kvs.kvlog("shutting down HTTP server")
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		kvs.srv.Shutdown(ctx)
		kvs.kvlog("HTP shutdown complete")
		return nil
	}

	return nil
}
