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

	// It stores last request ID that was applied per client.
	lastRequestIDPerClient map[int64]int64
}

func New(id int, peerIds []int, storage raft.Storage, readyChan <-chan any) *KVService {
	gob.Register(Command{})
	commitChan := make(chan raft.CommitEntry)

	// raft server
	rs := raft.NewServer(id, peerIds, storage, readyChan, commitChan)
	rs.Serve()
	kvs := &KVService{
		id:                     id,
		rs:                     rs,
		commitChan:             commitChan,
		ds:                     NewDataStore(),
		commitSubs:             make(map[int]chan Command),
		lastRequestIDPerClient: make(map[int64]int64),
	}

	kvs.runUpdater()
	return kvs
}

func (kvs *KVService) runUpdater() {
	go func() {
		// watches commit channel for committed commands
		for entry := range kvs.commitChan {
			cmd := entry.Command.(Command)

			// handle duplicate
			lastReqID, ok := kvs.lastRequestIDPerClient[cmd.ClientID]
			if ok && lastReqID >= cmd.RequestID {
				kvs.kvlog("duplicate request id%v, from client id=%v", cmd.RequestID, cmd.ClientID)

				cmd = Command{
					Kind:        cmd.Kind,
					IsDuplicate: true,
				}
			} else {
				kvs.lastRequestIDPerClient[cmd.ClientID] = cmd.RequestID

				// apply command to data store
				switch cmd.Kind {
				case CommandGet:
					cmd.ResultValue, cmd.ResultFound = kvs.ds.Get(cmd.Key)
				case CommandPut:
					cmd.ResultValue, cmd.ResultFound = kvs.ds.Put(cmd.Key, cmd.Value)
				case CommandCAS:
					cmd.ResultValue, cmd.ResultFound = kvs.ds.CAS(cmd.Key, cmd.CompareValue, cmd.Value)
				case CommandAppend:
					cmd.ResultValue, cmd.ResultFound = kvs.ds.Append(cmd.Key, cmd.Value)
				default:
					panic(fmt.Errorf("unexpected command %v", cmd))
				}
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
	mux.HandleFunc("POST /append/", kvs.handleAppend)

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
		Kind:      CommandPut,
		Key:       pr.Key,
		Value:     pr.Value,
		Id:        kvs.id,
		ClientID:  pr.ClientID,
		RequestID: pr.RequestID,
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
			// handle duplicate
			if commitCmd.IsDuplicate {
				renderJSON(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				renderJSON(w, api.PutResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
				})
			}
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
		Kind:      CommandGet,
		Key:       gr.Key,
		Id:        kvs.id,
		ClientID:  gr.ClientID,
		RequestID: gr.RequestID,
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
			// handle duplicate
			if commitCmd.IsDuplicate {
				renderJSON(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				renderJSON(w, api.GetResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					Value:      commitCmd.ResultValue,
				})
			}
		} else {
			renderJSON(w, api.GetResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
}

func (kvs *KVService) handleCAS(w http.ResponseWriter, req *http.Request) {
	cr := &api.CASRequest{}
	if err := readRequestJSON(req, cr); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP CAS %v", cr)

	// submit command to raft
	cmd := Command{
		Kind:         CommandCAS,
		Key:          cr.Key,
		Value:        cr.Value,
		CompareValue: cr.CompareValue,
		Id:           kvs.id,

		ClientID:  cr.ClientID,
		RequestID: cr.RequestID,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		renderJSON(w, api.CASResponse{RespStatus: api.StatusNotLeader})
		return
	}

	// create subscription
	sub := kvs.createCommitSubscription(logIndex)

	// wait for command to be committed
	select {
	case commitCmd := <-sub:
		if commitCmd.Id == kvs.id {
			// handle duplicate
			if commitCmd.IsDuplicate {
				renderJSON(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				renderJSON(w, api.CASResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
				})
			}
		} else {
			renderJSON(w, api.CASResponse{
				RespStatus: api.StatusFailedCommit,
			})
		}
	case <-req.Context().Done():
		return
	}

}

func (kvs *KVService) handleAppend(w http.ResponseWriter, req *http.Request) {
	ar := &api.AppendRequest{}
	if err := readRequestJSON(req, ar); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	kvs.kvlog("HTTP APPEND %v", ar)

	// submit command
	cmd := Command{
		Kind:      CommandAppend,
		Key:       ar.Key,
		Value:     ar.Value,
		ClientID:  ar.ClientID,
		RequestID: ar.RequestID,
	}
	logIndex := kvs.rs.Submit(cmd)
	if logIndex < 0 {
		renderJSON(w, api.AppendResponse{
			RespStatus: api.StatusNotLeader,
		})
	}

	// create subscription
	sub := kvs.createCommitSubscription(logIndex)

	select {
	case commitCmd := <-sub:
		if commitCmd.Id == kvs.id {
			if commitCmd.IsDuplicate {
				renderJSON(w, api.AppendResponse{
					RespStatus: api.StatusDuplicateRequest,
				})
			} else {
				renderJSON(w, api.AppendResponse{
					RespStatus: api.StatusOK,
					KeyFound:   commitCmd.ResultFound,
					PrevValue:  commitCmd.ResultValue,
				})
			}
		} else {
			renderJSON(w, api.AppendResponse{RespStatus: api.StatusFailedCommit})
		}
	case <-req.Context().Done():
		return
	}
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
