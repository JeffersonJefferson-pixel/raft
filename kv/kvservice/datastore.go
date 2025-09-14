package kvservice

import "sync"

// a simple key-value store
type DataStore struct {
	sync.Mutex
	data map[string]string
}

func NewDataStore() *DataStore {
	return &DataStore{
		data: make(map[string]string),
	}
}

func (ds *DataStore) Get(key string) (string, bool) {
	return "", false
}

func (ds *DataStore) Put(key, value string) (string, bool) {
	return "", false
}

func (ds *DataStore) CAS(key, compare, value string) (string, bool) {
	return "", false
}
