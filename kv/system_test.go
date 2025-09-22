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

func TestCASBasic(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "k", "v")

	if pv, found := h.CheckCAS(c1, "k", "v", "newv"); pv != "v" || !found {
		t.Errorf("got %s,%v, want replacement", pv, found)
	}

	h.CheckGet(c1, "k", "newv")
}

func TestBasicAppend(t *testing.T) {
	h := NewHarness(t, 3)
	defer h.Shutdown()
	h.CheckSingleLeader()

	c1 := h.NewClient()
	h.CheckPut(c1, "foo", "bar")

	prev, found := h.CheckAppend(c1, "foo", "baz")
	if !found || prev != "bar" {
		t.Errorf("got found=%v, prev=%v, want true/foo", found, prev)
	}
	h.CheckGet(c1, "foo", "barbaz")
}

func sleepMs(n int) {
	time.Sleep(time.Duration(n) * time.Millisecond)
}
