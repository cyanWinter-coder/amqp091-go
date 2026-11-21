// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConfirmOneResequences(t *testing.T) {
	var (
		fixtures = []Confirmation{
			{1, true},
			{2, false},
			{3, true},
		}
		c = newConfirms()
		l = make(chan Confirmation, len(fixtures))
	)

	c.Listen(l)

	for i := range fixtures {
		if want, got := uint64(i+1), c.publish(); want != got.DeliveryTag {
			t.Fatalf("expected publish to return the 1 based delivery tag published, want: %d, got: %d", want, got.DeliveryTag)
		}
	}

	c.One(fixtures[1])
	c.One(fixtures[2])

	select {
	case confirm := <-l:
		t.Fatalf("expected to wait in order to properly resequence results, got: %+v", confirm)
	default:
	}

	c.One(fixtures[0])

	for i, fix := range fixtures {
		if want, got := fix, <-l; want != got {
			t.Fatalf("expected to return confirmations in sequence for %d, want: %+v, got: %+v", i, want, got)
		}
	}
}

func TestConfirmAndPublishDoNotDeadlock(t *testing.T) {
	var (
		c          = newConfirms()
		l          = make(chan Confirmation)
		iterations = 10
	)
	c.Listen(l)

	go func() {
		for i := 0; i < iterations; i++ {
			c.One(Confirmation{uint64(i + 1), true})
		}
	}()

	for i := 0; i < iterations; i++ {
		c.publish()
		<-l
	}
}

func TestConfirmMixedResequences(t *testing.T) {
	var (
		fixtures = []Confirmation{
			{1, true},
			{2, true},
			{3, true},
		}
		c = newConfirms()
		l = make(chan Confirmation, len(fixtures))
	)
	c.Listen(l)

	for range fixtures {
		c.publish()
	}

	c.One(fixtures[0])
	c.One(fixtures[2])
	c.Multiple(fixtures[1])

	for i, fix := range fixtures {
		want := fix
		var got Confirmation
		select {
		case got = <-l:
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout on reading confirmations")
		}
		if want != got {
			t.Fatalf("expected to confirm in sequence for %d, want: %+v, got: %+v", i, want, got)
		}
	}
}

func TestConfirmMultipleResequences(t *testing.T) {
	var (
		fixtures = []Confirmation{
			{1, true},
			{2, true},
			{3, true},
			{4, true},
		}
		c = newConfirms()
		l = make(chan Confirmation, len(fixtures))
	)
	c.Listen(l)

	for range fixtures {
		c.publish()
	}

	c.Multiple(fixtures[len(fixtures)-1])

	for i, fix := range fixtures {
		if want, got := fix, <-l; want != got {
			t.Fatalf("expected to confirm multiple in sequence for %d, want: %+v, got: %+v", i, want, got)
		}
	}
}

func BenchmarkSequentialBufferedConfirms(t *testing.B) {
	var (
		c = newConfirms()
		l = make(chan Confirmation, 10)
	)

	c.Listen(l)

	for i := 0; i < t.N; i++ {
		if i > cap(l)-1 {
			<-l
		}
		c.One(Confirmation{c.publish().DeliveryTag, true})
	}
}

func TestConfirmsIsThreadSafe(t *testing.T) {
	const count = 1000
	const timeout = 5 * time.Second
	var (
		c    = newConfirms()
		l    = make(chan Confirmation)
		pub  = make(chan Confirmation)
		done = make(chan Confirmation)
		late = time.After(timeout)
	)

	c.Listen(l)

	for i := 0; i < count; i++ {
		go func() { pub <- Confirmation{c.publish().DeliveryTag, true} }()
	}

	for i := 0; i < count; i++ {
		go func() { c.One(<-pub) }()
	}

	for i := 0; i < count; i++ {
		go func() { done <- <-l }()
	}

	for i := 0; i < count; i++ {
		select {
		case <-done:
		case <-late:
			t.Fatalf("expected all publish/confirms to finish after %s", timeout)
		}
	}
}

func TestDeferredConfirmationsConfirm(t *testing.T) {
	dcs := newDeferredConfirmations()
	var wg sync.WaitGroup
	for i, ack := range []bool{true, false} {
		var result bool
		deliveryTag := uint64(i + 1)
		dc := dcs.Add(deliveryTag)
		wg.Add(1)
		go func() {
			result = dc.Wait()
			wg.Done()
		}()
		dcs.Confirm(Confirmation{deliveryTag, ack})
		wg.Wait()
		if result != ack {
			t.Fatalf("expected to receive matching ack got %v", result)
		}
	}
}

func TestDeferredConfirmationsConfirmMultiple(t *testing.T) {
	dcs := newDeferredConfirmations()
	var wg sync.WaitGroup
	var result bool
	dc1 := dcs.Add(1)
	dc2 := dcs.Add(2)
	dc3 := dcs.Add(3)
	wg.Add(1)
	go func() {
		result = dc1.Wait() && dc2.Wait() && dc3.Wait()
		wg.Done()
	}()
	dcs.ConfirmMultiple(Confirmation{4, true})
	wg.Wait()
	if !result {
		t.Fatal("expected to receive true for result, received false")
	}
}

func TestDeferredConfirmationsClose(t *testing.T) {
	dcs := newDeferredConfirmations()
	var wg sync.WaitGroup
	var result bool
	dc1 := dcs.Add(1)
	dc2 := dcs.Add(2)
	dc3 := dcs.Add(3)
	wg.Add(1)
	go func() {
		result = !dc1.Wait() && !dc2.Wait() && !dc3.Wait()
		wg.Done()
	}()
	dcs.Close(ErrClosed)
	wg.Wait()
	if !result {
		t.Fatal("expected to receive false for nacked confirmations, received true")
	}
}

func TestDeferredConfirmationsDoneAcked(t *testing.T) {
	dcs := newDeferredConfirmations()
	dc := dcs.Add(1)

	if dc.Acked() {
		t.Fatal("expected to receive false for pending confirmations, received true")
	}

	// Confirm twice to ensure that setAck is called once.
	for i := 0; i < 2; i++ {
		dcs.Confirm(Confirmation{dc.DeliveryTag, true})
	}

	<-dc.Done()

	if !dc.Acked() {
		t.Fatal("expected to receive true for acked confirmations, received false")
	}
}

func TestDeferredConfirmationsWaitContextNack(t *testing.T) {
	dcs := newDeferredConfirmations()
	dc := dcs.Add(1)

	dcs.Confirm(Confirmation{dc.DeliveryTag, false})

	ack, err := dc.WaitContext(context.Background())
	if err != nil {
		t.Fatalf("expected to receive nil, got %v", err)
	}
	if ack {
		t.Fatal("expected to receive false for nacked confirmations, received true")
	}
}

func TestDeferredConfirmationsWaitContextCancel(t *testing.T) {
	dc := newDeferredConfirmations().Add(1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	ack, err := dc.WaitContext(ctx)
	if err == nil {
		t.Fatal("expected to receive context error, got nil")
	}
	if ack {
		t.Fatal("expected to receive false for pending confirmations, received true")
	}
}

func TestDeferredConfirmationsConcurrency(t *testing.T) {
	dcs := newDeferredConfirmations()
	var wg sync.WaitGroup
	var result bool
	dc1 := dcs.Add(1)
	dc2 := dcs.Add(2)
	dc3 := dcs.Add(3)
	wg.Add(1)

	go func() {
		defer wg.Done()
		result = dc1.Wait() && dc2.Wait() && dc3.Wait()
	}()
	dcs.ConfirmMultiple(Confirmation{4, true})
	wg.Wait()
	if !result {
		t.Fatal("expected to receive true for concurrent confirmations, received false")
	}
}

func TestConfirmDispatchDropsNotificationOnFullListener(t *testing.T) {
	t.Parallel()

	c := newConfirms()
	listener := make(chan Confirmation, 1)
	listener <- Confirmation{0, false} // pre-fill to capacity
	c.Listen(listener)

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.confirm(Confirmation{1, true})
	}()

	select {
	case <-done:
		if len(listener) != 1 {
			t.Fatalf("expected listener to retain 1 item (notification dropped), got %d", len(listener))
		}
	case <-time.After(6 * time.Second):
		t.Fatal("confirm blocked on full listener for more than 6 seconds")
	}
}

func TestConfirmOneDropsStaleConfirmation(t *testing.T) {
	var (
		c = newConfirms()
		l = make(chan Confirmation, 4)
	)
	c.Listen(l)

	for i := 0; i < 3; i++ {
		c.publish()
	}

	c.Multiple(Confirmation{2, true})

	// Duplicates of already settled tags must be dropped, not parked.
	c.One(Confirmation{1, true})
	c.One(Confirmation{2, false})

	if got := len(c.sequencer); got != 0 {
		t.Fatalf("expected no stale confirmations in the sequencer, got %d", got)
	}

	c.One(Confirmation{3, true})

	for i := uint64(1); i <= 3; i++ {
		if want, got := (Confirmation{i, true}), <-l; want != got {
			t.Fatalf("expected exactly one confirmation per tag, want: %+v, got: %+v", want, got)
		}
	}

	select {
	case confirm := <-l:
		t.Fatalf("expected no duplicate notification for a settled tag, got: %+v", confirm)
	default:
	}
}

func TestConfirmOutOfOrderDoesNotSettleOthers(t *testing.T) {
	c := newConfirms()

	dcs := make([]*DeferredConfirmation, 4)
	for i := range dcs {
		dcs[i] = c.publish()
	}

	// A single out-of-order confirm must leave smaller and larger tags pending.
	c.One(Confirmation{3, true})

	for _, i := range []int{0, 1, 3} {
		select {
		case <-dcs[i].Done():
			t.Fatalf("expected tag %d to remain pending after an out-of-order confirm of tag 3", i+1)
		default:
		}
	}
	if !dcs[2].Acked() {
		t.Fatal("expected tag 3 to be acknowledged")
	}

	// Confirming tag 1 must not flush the buffered tag 3 past the pending tag 2.
	c.One(Confirmation{1, true})
	if !dcs[0].Acked() {
		t.Fatal("expected tag 1 to be acknowledged")
	}
	select {
	case <-dcs[1].Done():
		t.Fatal("expected tag 2 to remain pending")
	default:
	}

	c.One(Confirmation{2, true})
	for _, i := range []int{0, 1, 2} {
		if !dcs[i].Acked() {
			t.Fatalf("expected tag %d to be acknowledged after the gap was filled", i+1)
		}
	}
	select {
	case <-dcs[3].Done():
		t.Fatal("expected tag 4 to remain pending")
	default:
	}

	c.One(Confirmation{4, true})
	if !dcs[3].Acked() {
		t.Fatal("expected tag 4 to be acknowledged")
	}
}

func TestConfirmMultipleSettlesOnlyCoveredRange(t *testing.T) {
	c := newConfirms()

	dcs := make([]*DeferredConfirmation, 4)
	for i := range dcs {
		dcs[i] = c.publish()
	}

	c.Multiple(Confirmation{2, false})

	for _, i := range []int{0, 1} {
		select {
		case <-dcs[i].Done():
		default:
			t.Fatalf("expected tag %d to be settled by the multiple confirm", i+1)
		}
		if dcs[i].Acked() {
			t.Fatalf("expected tag %d to be nacked", i+1)
		}
		if err := dcs[i].Err(); err != nil {
			t.Fatalf("expected no error for a broker nack of tag %d, got: %v", i+1, err)
		}
	}
	for _, i := range []int{2, 3} {
		select {
		case <-dcs[i].Done():
			t.Fatalf("expected tag %d to remain pending outside the multiple confirm range", i+1)
		default:
		}
	}
}

func TestConfirmMultipleBeyondPublishedIsClamped(t *testing.T) {
	var (
		c = newConfirms()
		l = make(chan Confirmation, 3)
	)
	c.Listen(l)

	c.publish()
	c.publish()

	// A batch confirm covering tags never published must not advance the
	// expecting tag past them.
	c.Multiple(Confirmation{5, true})

	dc := c.publish()
	c.One(Confirmation{3, true})

	for i := uint64(1); i <= 3; i++ {
		if want, got := (Confirmation{i, true}), <-l; want != got {
			t.Fatalf("expected confirmations in sequence, want: %+v, got: %+v", want, got)
		}
	}
	if !dc.Acked() {
		t.Fatal("expected the tag published after the clamped multiple confirm to be acknowledged")
	}
}

func TestConfirmsCloseWakesAllWaitersWithReason(t *testing.T) {
	c := newConfirms()

	dcs := make([]*DeferredConfirmation, 3)
	for i := range dcs {
		dcs[i] = c.publish()
	}
	c.One(Confirmation{2, true}) // park an out-of-order confirm in the sequencer

	closeErr := &Error{Code: ChannelError, Reason: "test close"}
	if err := c.Close(closeErr); err != nil {
		t.Fatalf("expected no error from Close, got: %v", err)
	}

	for i, dc := range dcs {
		select {
		case <-dc.Done():
		case <-time.After(time.Second):
			t.Fatalf("expected waiter for tag %d to be woken on close", i+1)
		}
	}

	// Tag 2 was settled by its own confirm before the close.
	if !dcs[1].Acked() {
		t.Fatal("expected tag 2 to remain acknowledged from its pre-close confirm")
	}
	if err := dcs[1].Err(); err != nil {
		t.Fatalf("expected no error for the pre-close confirmed tag 2, got: %v", err)
	}

	for _, i := range []int{0, 2} {
		if dcs[i].Acked() {
			t.Fatalf("expected tag %d to be settled as not acknowledged on close", i+1)
		}
		if got := dcs[i].Err(); got != closeErr {
			t.Fatalf("expected the close reason to be preserved for tag %d, want: %v, got: %v", i+1, closeErr, got)
		}
	}

	if got := len(c.sequencer); got != 0 {
		t.Fatalf("expected the sequencer to be discarded on close, got %d entries", got)
	}
	if got := len(c.deferredConfirmations.confirmations); got != 0 {
		t.Fatalf("expected no pending deferred confirmations after close, got %d", got)
	}
}

func TestConfirmsCloseWithoutErrorUsesErrClosed(t *testing.T) {
	c := newConfirms()
	dc := c.publish()

	if err := c.Close(nil); err != nil {
		t.Fatalf("expected no error from Close, got: %v", err)
	}

	if got := dc.Err(); got != ErrClosed {
		t.Fatalf("expected ErrClosed for a graceful close, got: %v", got)
	}
}

func TestDeferredConfirmationTerminalStates(t *testing.T) {
	t.Run("nack", func(t *testing.T) {
		dcs := newDeferredConfirmations()
		dc := dcs.Add(1)
		dcs.Confirm(Confirmation{1, false})

		if dc.Wait() {
			t.Fatal("expected Wait to return false for a nacked confirmation")
		}
		if err := dc.Err(); err != nil {
			t.Fatalf("expected no error for a broker nack, got: %v", err)
		}
	})

	t.Run("publish failure", func(t *testing.T) {
		c := newConfirms()
		dc := c.publish()

		sendErr := &Error{Code: ChannelError, Reason: "send failed"}
		c.unpublish(sendErr)

		if dc.Wait() {
			t.Fatal("expected Wait to return false for a failed publish")
		}
		if got := dc.Err(); got != sendErr {
			t.Fatalf("expected the publish error as the terminal state, want: %v, got: %v", sendErr, got)
		}
	})

	t.Run("connection close", func(t *testing.T) {
		c := newConfirms()
		dc := c.publish()
		c.reset()

		if dc.Wait() {
			t.Fatal("expected Wait to return false for a confirmation dropped by recovery")
		}
		if got := dc.Err(); got != ErrClosed {
			t.Fatalf("expected ErrClosed for a confirmation dropped by recovery, got: %v", got)
		}
	})
}

func TestConfirmsSettledTagsAreForgotten(t *testing.T) {
	c := newConfirms()

	const count = 1000
	for i := 0; i < count; i++ {
		c.publish()
	}
	// Confirm out of order so every tag passes through the sequencer.
	for i := count; i > 1; i-- {
		c.One(Confirmation{uint64(i), true})
	}
	c.One(Confirmation{1, true})

	if got := len(c.sequencer); got != 0 {
		t.Fatalf("expected the sequencer to be empty after settling, got %d entries", got)
	}
	if got := len(c.deferredConfirmations.confirmations); got != 0 {
		t.Fatalf("expected no pending deferred confirmations after settling, got %d", got)
	}
}
