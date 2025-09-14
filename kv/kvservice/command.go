package kvservice

type CommandKind int

const (
	CommandInvalid CommandKind = iota
	CommandGet
	CommandPut
	CommandCAS
)

type Command struct {
	Kind         CommandKind
	Key, Value   string
	CompareValue string
	ResultValue  string
	ResultFound  bool

	Id int
}
