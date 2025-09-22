package api

type PutRequest struct {
	Key   string
	Value string
}

type Response interface {
	Status() ResponseStatus
}

type ResponseStatus int

type PutResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	PrevValue  string
}

func (pr *PutResponse) Status() ResponseStatus {
	return pr.RespStatus
}

type GetRequest struct {
	Key string
}

type GetResponse struct {
	RespStatus ResponseStatus
	KeyFound   bool
	Value      string
}

func (gr *GetResponse) Status() ResponseStatus {
	return gr.RespStatus
}

const (
	StatusInvalid ResponseStatus = iota
	StatusOK
	StatusNotLeader
	StatusFailedCommit
)
