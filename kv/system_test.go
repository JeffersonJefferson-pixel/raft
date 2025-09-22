package main

import (
	"testing"
	"time"
)

func TestBasicPutGetSingleClient(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "llave", "cosa")

	h.CheckGet(c1, "llave", "cosa")
	sleepMs(80)
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
