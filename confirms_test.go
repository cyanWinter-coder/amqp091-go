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

	for i := 0; i < iterations; i++ {
		c.publish()
	}

	go func() {
		for i := 0; i < iterations; i++ {
			c.One(Confirmation{uint64(i + 1), true})
		}
	}()

	for i := 0; i < iterations; i++ {
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

// A batched ack must not override the terminal state of a tag that was
// already confirmed out of order.
func TestConfirmMultipleKeepsOutOfOrderNack(t *testing.T) {
	var (
		c = newConfirms()
		l = make(chan Confirmation, 3)
	)

	c.Listen(l)

	dc1 := c.publish()
	dc2 := c.publish()
	dc3 := c.publish()

	c.One(Confirmation{2, false})
	c.Multiple(Confirmation{3, true})

	want := []Confirmation{{1, true}, {2, false}, {3, true}}
	for i, w := range want {
		if got := <-l; got != w {
			t.Fatalf("expected confirmation %d to be %+v, got: %+v", i, w, got)
		}
	}

	if !dc1.Wait() || dc2.Wait() || !dc3.Wait() {
		t.Fatalf("expected deferred confirmations to resolve as ack, nack, ack")
	}
	if got := dc2.Status(); got != ConfirmationNacked {
		t.Fatalf("expected tag 2 to keep its nack terminal state, got: %v", got)
	}
}

// A batched confirmation only completes tags that were actually published and
// are still pending; it must not advance past tags that do not exist yet.
func TestConfirmMultipleOnlyCompletesPendingTags(t *testing.T) {
	var (
		c = newConfirms()
		l = make(chan Confirmation, 3)
	)

	c.Listen(l)

	c.publish()
	c.publish()

	c.Multiple(Confirmation{5, true})

	for _, tag := range []uint64{1, 2} {
		if got := <-l; got != (Confirmation{tag, true}) {
			t.Fatalf("expected confirmation for tag %d, got: %+v", tag, got)
		}
	}
	select {
	case got := <-l:
		t.Fatalf("expected no confirmation for unpublished tags, got: %+v", got)
	default:
	}

	// The next publish must be delivered normally, proving the batch confirm
	// did not skip over the unpublished tags.
	c.publish()
	c.One(Confirmation{3, true})
	if got := <-l; got != (Confirmation{3, true}) {
		t.Fatalf("expected confirmation for tag 3, got: %+v", got)
	}
}

// A tag that already reached a terminal state cannot be completed again.
func TestConfirmOneCompletesExactlyOnce(t *testing.T) {
	var (
		c = newConfirms()
		l = make(chan Confirmation, 2)
	)

	c.Listen(l)

	dc := c.publish()
	c.One(Confirmation{1, true})
	c.One(Confirmation{1, false})

	if got := <-l; got != (Confirmation{1, true}) {
		t.Fatalf("expected the first confirmation to win, got: %+v", got)
	}
	select {
	case got := <-l:
		t.Fatalf("expected duplicate confirmation to be ignored, got: %+v", got)
	default:
	}
	if !dc.Wait() || dc.Status() != ConfirmationAcked {
		t.Fatalf("expected the deferred confirmation to stay acked, got status: %v", dc.Status())
	}
}

// Publish failure, channel close, and broker nack are distinct terminal
// states, all reporting false from Wait.
func TestDeferredConfirmationTerminalStates(t *testing.T) {
	c := newConfirms()

	acked := c.publish()
	nacked := c.publish()
	closed := c.publish()
	failed := c.publish()

	c.One(Confirmation{acked.DeliveryTag, true})
	c.One(Confirmation{nacked.DeliveryTag, false})
	c.unpublish()
	c.Close()

	fixtures := []struct {
		dc     *DeferredConfirmation
		status ConfirmationStatus
	}{
		{acked, ConfirmationAcked},
		{nacked, ConfirmationNacked},
		{failed, ConfirmationPublishFailed},
		{closed, ConfirmationChannelClosed},
	}
	for _, fix := range fixtures {
		if got := fix.dc.Status(); got != fix.status {
			t.Fatalf("expected terminal state %v, got: %v", fix.status, got)
		}
	}
	if !acked.Wait() {
		t.Fatal("expected the acked confirmation to return true from Wait")
	}
	for _, dc := range []*DeferredConfirmation{nacked, failed, closed} {
		if dc.Wait() {
			t.Fatalf("expected Wait to return false for terminal state %v", dc.Status())
		}
	}
}

// Confirmed ranges are compacted as soon as they complete, so memory does not
// grow with the publish history.
func TestConfirmCompactsCompletedTags(t *testing.T) {
	const count = 100

	var (
		c = newConfirms()
		l = make(chan Confirmation, count)
	)

	c.Listen(l)

	for i := 0; i < count; i++ {
		c.publish()
	}

	// Confirm out of order, leaving holes that keep every other tag pending.
	for tag := count; tag >= 2; tag -= 2 {
		c.One(Confirmation{uint64(tag), true})
	}
	if got := len(c.pending.ranges); got != count/2 {
		t.Fatalf("expected %d pending ranges, got: %d", count/2, got)
	}

	for tag := count - 1; tag >= 1; tag -= 2 {
		c.One(Confirmation{uint64(tag), true})
	}

	for i := 0; i < count; i++ {
		<-l
	}
	if got := len(c.pending.ranges); got != 0 {
		t.Fatalf("expected no pending ranges after full confirmation, got: %d", got)
	}
	if got := len(c.completed); got != 0 {
		t.Fatalf("expected no buffered confirmations after in-order delivery, got: %d", got)
	}
	if got := len(c.deferredConfirmations.confirmations); got != 0 {
		t.Fatalf("expected no pending deferred confirmations, got: %d", got)
	}
}

func TestConfirmResetResolvesPendingAsChannelClosed(t *testing.T) {
	c := newConfirms()

	dc1 := c.publish()
	dc2 := c.publish()

	c.reset()

	for _, dc := range []*DeferredConfirmation{dc1, dc2} {
		if dc.Wait() {
			t.Fatal("expected reset to resolve pending confirmations as not acked")
		}
		if got := dc.Status(); got != ConfirmationChannelClosed {
			t.Fatalf("expected terminal state %v after reset, got: %v", ConfirmationChannelClosed, got)
		}
	}

	// The tag sequence restarts after a reset.
	if got := c.publish(); got.DeliveryTag != 1 {
		t.Fatalf("expected the tag sequence to restart at 1, got: %d", got.DeliveryTag)
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
		dcs.resolve(deliveryTag, statusOf(ack))
		wg.Wait()
		if result != ack {
			t.Fatalf("expected to receive matching ack got %v", result)
		}
	}
}

func TestDeferredConfirmationsResolveAllPending(t *testing.T) {
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
	for _, tag := range []uint64{1, 2, 3} {
		dcs.resolve(tag, ConfirmationAcked)
	}
	wg.Wait()
	if !result {
		t.Fatal("expected to receive true for result, received false")
	}
}

func TestDeferredConfirmationsCloseAll(t *testing.T) {
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
	dcs.closeAll(ConfirmationChannelClosed)
	wg.Wait()
	if !result {
		t.Fatal("expected to receive false for nacked confirmations, received true")
	}
	for _, dc := range []*DeferredConfirmation{dc1, dc2, dc3} {
		if got := dc.Status(); got != ConfirmationChannelClosed {
			t.Fatalf("expected terminal state %v, got: %v", ConfirmationChannelClosed, got)
		}
	}
}

func TestDeferredConfirmationsDoneAcked(t *testing.T) {
	dcs := newDeferredConfirmations()
	dc := dcs.Add(1)

	if dc.Acked() {
		t.Fatal("expected to receive false for pending confirmations, received true")
	}
	if dc.Status() != ConfirmationPending {
		t.Fatalf("expected terminal state %v for pending confirmations, got: %v", ConfirmationPending, dc.Status())
	}

	// Resolve twice to ensure that setTerminal is called once.
	for i := 0; i < 2; i++ {
		dcs.resolve(dc.DeliveryTag, ConfirmationAcked)
	}

	<-dc.Done()

	if !dc.Acked() {
		t.Fatal("expected to receive true for acked confirmations, received false")
	}
}

func TestDeferredConfirmationsWaitContextNack(t *testing.T) {
	dcs := newDeferredConfirmations()
	dc := dcs.Add(1)

	dcs.resolve(dc.DeliveryTag, ConfirmationNacked)

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
	for _, tag := range []uint64{1, 2, 3} {
		dcs.resolve(tag, ConfirmationAcked)
	}
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

	c.publish()

	done := make(chan struct{})
	go func() {
		defer close(done)
		c.One(Confirmation{1, true})
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
