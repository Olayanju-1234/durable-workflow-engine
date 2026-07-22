package timer

import (
	"sync"
	"testing"
	"time"
)

// TestFiresInDeadlineOrder adds timers out of order and asserts the min-heap fires
// them earliest-deadline-first.
func TestFiresInDeadlineOrder(t *testing.T) {
	var (
		mu    sync.Mutex
		fired []string
	)
	done := make(chan struct{})
	svc := New(func(tm Timer) {
		mu.Lock()
		fired = append(fired, tm.StepName)
		n := len(fired)
		mu.Unlock()
		if n == 3 {
			close(done)
		}
	})
	svc.Start()
	defer svc.Stop()

	now := time.Now()
	svc.Add(Timer{StepName: "third", FireAt: now.Add(90 * time.Millisecond)})
	svc.Add(Timer{StepName: "first", FireAt: now.Add(30 * time.Millisecond)})
	svc.Add(Timer{StepName: "second", FireAt: now.Add(60 * time.Millisecond)})

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timers did not all fire")
	}
	mu.Lock()
	defer mu.Unlock()
	want := []string{"first", "second", "third"}
	for i := range want {
		if fired[i] != want[i] {
			t.Fatalf("fire order: got %v, want %v", fired, want)
		}
	}
}

// TestPastDeadlineFiresPromptly covers the crash-recovery case: a timer whose
// deadline elapsed while the engine was down must fire immediately on Add.
func TestPastDeadlineFiresPromptly(t *testing.T) {
	fired := make(chan Timer, 1)
	svc := New(func(tm Timer) { fired <- tm })
	svc.Start()
	defer svc.Stop()

	svc.Add(Timer{StepName: "expired", FireAt: time.Now().Add(-time.Hour)})
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("expired timer did not fire promptly")
	}
}

// TestAddEarlierWakesLoop: adding a nearer deadline than the current earliest must
// pre-empt the loop's long sleep, or the near timer would be stuck behind the far one.
func TestAddEarlierWakesLoop(t *testing.T) {
	fired := make(chan string, 2)
	svc := New(func(tm Timer) { fired <- tm.StepName })
	svc.Start()
	defer svc.Stop()

	now := time.Now()
	svc.Add(Timer{StepName: "far", FireAt: now.Add(5 * time.Second)})
	svc.Add(Timer{StepName: "near", FireAt: now.Add(40 * time.Millisecond)})

	select {
	case got := <-fired:
		if got != "near" {
			t.Fatalf("first fire: got %q, want near", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("near timer did not fire (loop didn't wake for the earlier deadline)")
	}
}
