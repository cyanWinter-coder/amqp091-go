// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// logCapture captures package Logger output for assertions in tests.
type logCapture struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logCapture) Printf(format string, v ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	fmt.Fprintf(&l.buf, format+"\n", v...)
}

func (l *logCapture) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func TestGeneratedUniqueConsumerTagDoesNotExceedMaxLength(t *testing.T) {
	assertCorrectLength := func(commandName string) {
		tag := commandNameBasedUniqueConsumerTag(commandName)
		if len(tag) > consumerTagLengthMax {
			t.Error("Generated unique consumer tag exceeds maximum length:", tag)
		}
	}

	assertCorrectLength("test")
	assertCorrectLength(strings.Repeat("z", 249))
	assertCorrectLength(strings.Repeat("z", 256))
	assertCorrectLength(strings.Repeat("z", 1024))
}

func TestCancelByQueue(t *testing.T) {
	subs := makeConsumers()
	defer subs.close()

	ch1 := make(chan Delivery, 1)
	ch2 := make(chan Delivery, 1)
	ch3 := make(chan Delivery, 1)

	subs.add("ctag1", ch1, consumerConfig{Queue: "queue1"})
	subs.add("ctag2", ch2, consumerConfig{Queue: "queue2"})
	subs.add("ctag3", ch3, consumerConfig{Queue: "queue1"})

	// Cancel all consumers on "queue1"
	subs.cancelByQueue("queue1")

	subs.Lock()
	_, found1 := subs.configs["ctag1"]
	_, found2 := subs.configs["ctag2"]
	_, found3 := subs.configs["ctag3"]
	subs.Unlock()

	if found1 {
		t.Error("Expected ctag1 to be removed from configs")
	}
	if !found2 {
		t.Error("Expected ctag2 to remain in configs")
	}
	if found3 {
		t.Error("Expected ctag3 to be removed from configs")
	}

	// Verify that the channels for canceled consumers were closed
	// (reading from closed channels returns immediately with ok=false)
	// We use a small timeout to allow concurrent goroutines to execute the deferred closes.
	select {
	case _, ok := <-ch1:
		if ok {
			t.Error("Expected ch1 to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected ch1 to be closed, but it blocked")
	}

	select {
	case _, ok := <-ch3:
		if ok {
			t.Error("Expected ch3 to be closed")
		}
	case <-time.After(100 * time.Millisecond):
		t.Error("Expected ch3 to be closed, but it blocked")
	}

	// Verify ch2 remains open and we can write/read from it
	select {
	case _, ok := <-ch2:
		t.Errorf("Expected ch2 to remain open and block, but read succeeded (ok=%t)", ok)
	default:
		// Passed, ch2 is still open and blocked as expected
	}
}

func TestConsumerCancelRejectsNewDeliveries(t *testing.T) {
	subs := makeConsumers()
	defer subs.close()

	out := make(chan Delivery)
	subs.add("ctag", out, consumerConfig{Queue: "q"})

	if !subs.send("ctag", &Delivery{Body: []byte("accepted")}) {
		t.Fatal("expected send to be accepted while the consumer is active")
	}
	if !subs.cancel("ctag") {
		t.Fatal("expected cancel to perform the active -> draining transition")
	}
	if subs.send("ctag", &Delivery{Body: []byte("late")}) {
		t.Fatal("expected send to be rejected after cancel")
	}

	// The delivery accepted before the cancel is still flushed, then the
	// channel closes.
	d, ok := <-out
	if !ok || string(d.Body) != "accepted" {
		t.Fatalf("expected the pre-cancel delivery to be flushed, got %v (ok=%t)", d, ok)
	}
	if _, ok := <-out; ok {
		t.Fatal("expected the delivery channel to close after draining")
	}
}

func TestConsumerCancelFlushesInflightDeliveries(t *testing.T) {
	subs := makeConsumers()
	defer subs.close()

	out := make(chan Delivery)
	subs.add("ctag", out, consumerConfig{Queue: "q"})

	const total = 3
	for i := 0; i < total; i++ {
		if !subs.send("ctag", &Delivery{Body: []byte{byte('0' + i)}}) {
			t.Fatalf("send %d was not accepted", i)
		}
	}
	subs.cancel("ctag")

	for i := 0; i < total; i++ {
		d, ok := <-out
		if !ok {
			t.Fatalf("delivery channel closed after %d of %d deliveries", i, total)
		}
		if d.Body[0] != byte('0'+i) {
			t.Fatalf("deliveries out of order: got %q at position %d", d.Body, i)
		}
	}
	if _, ok := <-out; ok {
		t.Fatal("expected the delivery channel to close after draining")
	}
}

func TestConsumerDoubleCancelProducesSingleTerminalState(t *testing.T) {
	subs := makeConsumers()
	defer subs.close()

	out := make(chan Delivery)
	subs.add("ctag", out, consumerConfig{Queue: "q"})

	if !subs.cancel("ctag") {
		t.Fatal("first cancel should perform the transition")
	}
	if subs.cancel("ctag") {
		t.Fatal("second cancel should not perform the transition again")
	}

	subs.Lock()
	if c, ok := subs.chans["ctag"]; ok && c.state != consumerDraining {
		t.Error("a cancelled consumer must never return to the active state")
	}
	subs.Unlock()

	// The delivery channel closes exactly once: ranging over it terminates.
	for range out {
		t.Error("unexpected delivery after cancel")
	}
}

// TestConsumerCancelCloseAndSendRace exercises the terminal-state boundary
// under concurrent cancel, teardown, and delivery. Run with -race.
func TestConsumerCancelCloseAndSendRace(t *testing.T) {
	for i := 0; i < 50; i++ {
		subs := makeConsumers()
		out := make(chan Delivery)
		subs.add("ctag", out, consumerConfig{Queue: "q"})

		drained := make(chan struct{})
		go func() {
			for range out {
			}
			close(drained)
		}()

		var wg sync.WaitGroup
		wg.Add(3)
		go func() { defer wg.Done(); subs.cancel("ctag") }()
		go func() { defer wg.Done(); subs.cancel("ctag") }()
		go func() { defer wg.Done(); subs.send("ctag", &Delivery{}) }()
		wg.Wait()

		subs.close()
		<-drained
	}
}

func TestConsumerCloseDrainsWithinDeadlineAndReportsRemaining(t *testing.T) {
	defer func(d time.Duration) { consumerDrainTimeout = d }(consumerDrainTimeout)
	consumerDrainTimeout = 50 * time.Millisecond

	logger := &logCapture{}
	SetLogger(logger)
	defer SetLogger(NullLogger{})

	subs := makeConsumers()
	// The application never receives from out: the slow consumer.
	out := make(chan Delivery)
	subs.add("ctag-slow", out, consumerConfig{Queue: "q"})

	const total = 3
	for i := 0; i < total; i++ {
		if !subs.send("ctag-slow", &Delivery{Body: []byte{byte('0' + i)}}) {
			t.Fatalf("send %d was not accepted", i)
		}
	}

	start := time.Now()
	subs.close()
	elapsed := time.Since(start)

	if elapsed > 10*consumerDrainTimeout {
		t.Fatalf("close blocked for %v, expected it to be bounded by the drain timeout", elapsed)
	}
	if got := logger.String(); !strings.Contains(got, "3 queued deliveries") {
		t.Fatalf("expected the remaining delivery count to be reported, got: %q", got)
	}
	if _, ok := <-out; ok {
		t.Fatal("expected the delivery channel to be closed after teardown")
	}
}

func TestConsumerCloseFlushesToReadingApplication(t *testing.T) {
	subs := makeConsumers()

	out := make(chan Delivery)
	subs.add("ctag", out, consumerConfig{Queue: "q"})

	const total = 2
	for i := 0; i < total; i++ {
		if !subs.send("ctag", &Delivery{Body: []byte{byte('0' + i)}}) {
			t.Fatalf("send %d was not accepted", i)
		}
	}

	closed := make(chan struct{})
	go func() {
		subs.close()
		close(closed)
	}()

	// The queued deliveries are flushed to the application within the drain
	// window instead of being dropped immediately.
	for i := 0; i < total; i++ {
		d, ok := <-out
		if !ok {
			t.Fatalf("delivery channel closed after %d of %d deliveries", i, total)
		}
		if d.Body[0] != byte('0'+i) {
			t.Fatalf("deliveries out of order: got %q at position %d", d.Body, i)
		}
	}
	if _, ok := <-out; ok {
		t.Fatal("expected the delivery channel to close after draining")
	}

	select {
	case <-closed:
	case <-time.After(consumerDrainTimeout):
		t.Fatal("close did not return after the queue was drained")
	}
}

func TestConsumerSlowConsumerDoesNotBlockOthers(t *testing.T) {
	defer func(d time.Duration) { consumerDrainTimeout = d }(consumerDrainTimeout)
	consumerDrainTimeout = 50 * time.Millisecond

	subs := makeConsumers()
	defer subs.close()

	slow := make(chan Delivery)
	fast := make(chan Delivery)
	subs.add("slow", slow, consumerConfig{Queue: "q"})
	subs.add("fast", fast, consumerConfig{Queue: "q"})

	// The slow consumer's delivery is accepted but never received from its
	// channel.
	if !subs.send("slow", &Delivery{Body: []byte("slow")}) {
		t.Fatal("send to slow consumer was not accepted")
	}
	if !subs.send("fast", &Delivery{Body: []byte("fast")}) {
		t.Fatal("send to fast consumer was not accepted")
	}

	select {
	case d := <-fast:
		if string(d.Body) != "fast" {
			t.Fatalf("unexpected delivery: %q", d.Body)
		}
	case <-time.After(time.Second):
		t.Fatal("slow consumer blocked deliveries to other consumers")
	}
}

func TestDispatchDropsDeliveryForUnknownConsumer(t *testing.T) {
	logger := &logCapture{}
	SetLogger(logger)
	defer SetLogger(NullLogger{})

	ch := &Channel{id: 1, connection: &Connection{}, consumers: makeConsumers()}
	defer ch.consumers.close()

	ch.dispatch(&basicDeliver{ConsumerTag: "gone"})

	if got := logger.String(); !strings.Contains(got, `"gone"`) {
		t.Fatalf("expected the dropped delivery to be logged, got: %q", got)
	}
}
