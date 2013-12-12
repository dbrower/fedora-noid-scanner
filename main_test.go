package main

import (
	"sync"
	"testing"
	"time"
)

func TestIdDecoder(t *testing.T) {
	var out chan idInfo = make(chan idInfo, 10)
	var names chan string = make(chan string, 10)
	var wg sync.WaitGroup

	wg.Add(1)
	go idDecoder(&wg, "changeme:.sdd", out, names)

	table := []struct {
		name   string
		result *idInfo
	}{
		{"info%3Afedora%2Fchangeme%3A56", &idInfo{id: "changeme:56", pos: 56}},
		{"changeme%3A12", &idInfo{id: "changeme:12", pos: 12}},
		{"info%3Afedora%2Fchangeme%3Ax6", nil},
		{"info%3Afedora%2Fchangeme%3A22.md5", nil},
	}

	for i := range table {
		names <- table[i].name
	}
	close(names)
	for i := range table {
		if table[i].result != nil {
			select {
			case r := <-out:
				t.Logf("%+v\n", r)
				if table[i].result.id != r.id || table[i].result.pos != r.pos {
					t.Errorf("Bad decode of %v. got %v\n", table[i], r)
				}
			case <-time.After(10 * time.Millisecond):
				t.Errorf("Timeout in decode of %+v\n", table[i])
			}
		}
	}

	wg.Wait()
}
