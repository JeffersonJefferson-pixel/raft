package kvservice

type CommandKind int

const (
	CommandInvalid CommandKind = iota
	CommandGet
	CommandPut
	CommandCAS
	CommandAppend
)

type Command struct {
	Kind         CommandKind
	Key, Value   string
	CompareValue string
	ResultValue  string
	ResultFound  bool

	Id int

	ClientID, RequestID int64

	IsDuplicate bool
}
