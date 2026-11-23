// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"errors"
	"testing"
	"time"
)

func TestRPCManagerCapturesSequenceAndGeneration(t *testing.T) {
	r := newRPCManager()

	first, err := r.begin(func() error { return nil })
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	second, err := r.begin(func() error { return nil })
	if err != nil {
		t.Fatalf("begin: %v", err)
	}

	if first.sequence == second.sequence {
		t.Fatal("each request must get its own sequence")
	}
	if first.sequence+1 != second.sequence {
		t.Fatalf("sequences must increase by one: %d then %d", first.sequence, second.sequence)
	}
	if first.generation != second.generation {
		t.Fatal("requests of the same session must share a generation")
	}

	r.reset()
	third, err := r.begin(func() error { return nil })
	if err != nil {
		t.Fatalf("begin after reset: %v", err)
	}
	if third.generation == first.generation {
		t.Fatal("reset must open a new generation")
	}
	if third.sequence <= second.sequence {
		t.Fatal("sequence must keep increasing across generations")
	}
}

func TestRPCManagerBeginPropagatesSendError(t *testing.T) {
	r := newRPCManager()
	sentinel := errors.New("send failed")

	req, err := r.begin(func() error { return sentinel })
	if req != nil {
		t.Fatal("failed send must not register a request")
	}
	if !errors.Is(err, sentinel) {
		t.Fatalf("error = %v, want %v", err, sentinel)
	}
	// A reply with nothing pending is dropped.
	if r.deliver(&queueDeclareOk{}) {
		t.Fatal("reply delivered with no waiter")
	}
}

func TestRPCManagerDeliversOnlyToCurrentWaiter(t *testing.T) {
	r := newRPCManager()
	first, _ := r.begin(func() error { return nil })
	second, _ := r.begin(func() error { return nil })

	if !r.deliver(&queueDeclareOk{Queue: "first"}) {
		t.Fatal("first reply was not delivered")
	}
	if !r.deliver(&queueDeclareOk{Queue: "second"}) {
		t.Fatal("second reply was not delivered")
	}

	msg, err := r.wait(first, 0)
	if err != nil || msg.(*queueDeclareOk).Queue != "first" {
		t.Fatalf("first waiter got %v, %v", msg, err)
	}
	msg, err = r.wait(second, 0)
	if err != nil || msg.(*queueDeclareOk).Queue != "second" {
		t.Fatalf("second waiter got %v, %v", msg, err)
	}

	if r.deliver(&queueDeclareOk{}) {
		t.Fatal("a third reply must be dropped when no waiter remains")
	}
}

func TestRPCManagerAbandonedRequestDropsLateReply(t *testing.T) {
	r := newRPCManager()
	req, _ := r.begin(func() error { return nil })

	// The caller times out, leaving a tombstone.
	if _, err := r.wait(req, time.Millisecond); !errors.Is(err, ErrRPCTimeout) {
		t.Fatalf("wait error = %v, want ErrRPCTimeout", err)
	}

	// A later call is registered behind the tombstone.
	next, _ := r.begin(func() error { return nil })

	// The late reply belongs to the abandoned request and must be dropped.
	if r.deliver(&queueDeclareOk{Queue: "late"}) {
		t.Fatal("late reply delivered instead of dropped")
	}

	// The reply for the next call must reach it, not the dropped one.
	if !r.deliver(&queueDeclareOk{Queue: "next"}) {
		t.Fatal("reply for the next call was not delivered")
	}
	msg, err := r.wait(next, 0)
	if err != nil || msg.(*queueDeclareOk).Queue != "next" {
		t.Fatalf("next waiter got %v, %v", msg, err)
	}
}

func TestRPCManagerTimeoutPrefersDeliveredReply(t *testing.T) {
	r := newRPCManager()
	req, _ := r.begin(func() error { return nil })
	r.deliver(&queueDeclareOk{Queue: "raced"})

	// The reply is already buffered; a tiny timeout must still return it.
	msg, err := r.wait(req, time.Nanosecond)
	if err != nil {
		t.Fatalf("wait error = %v, want the buffered reply", err)
	}
	if msg.(*queueDeclareOk).Queue != "raced" {
		t.Fatalf("msg = %v", msg)
	}
}

func TestRPCManagerFailWakesWaiterWithReason(t *testing.T) {
	r := newRPCManager()
	req, _ := r.begin(func() error { return nil })

	want := &Error{Code: ChannelError, Reason: "closed for test"}
	go func() {
		time.Sleep(5 * time.Millisecond)
		r.fail(want)
	}()

	_, err := r.wait(req, time.Second)
	if !errors.Is(err, want) {
		t.Fatalf("wait error = %v, want %v", err, want)
	}
}

func TestRPCManagerFailDefaultsToErrClosed(t *testing.T) {
	r := newRPCManager()
	req, _ := r.begin(func() error { return nil })

	r.fail(nil)
	if _, err := r.wait(req, 0); err != ErrClosed {
		t.Fatalf("wait error = %v, want ErrClosed", err)
	}
}

func TestRPCManagerFailIsIdempotentAndKeepsFirstReason(t *testing.T) {
	r := newRPCManager()
	req, _ := r.begin(func() error { return nil })

	first := &Error{Code: ChannelError, Reason: "first"}
	second := &Error{Code: ChannelError, Reason: "second"}
	r.fail(first)
	r.fail(second)

	if _, err := r.wait(req, 0); !errors.Is(err, first) {
		t.Fatalf("wait error = %v, want the first reason %v", err, first)
	}

	// A second generation is unaffected by the first generation's reason.
	r.reset()
	next, _ := r.begin(func() error { return nil })
	r.fail(nil)
	if _, err := r.wait(next, 0); err != ErrClosed {
		t.Fatalf("new generation error = %v, want ErrClosed", err)
	}
}

func TestRPCManagerPoisonRefusesNewRequests(t *testing.T) {
	r := newRPCManager()
	req, _ := r.begin(func() error { return nil })

	r.poison(ErrCommandInvalid)

	if _, err := r.wait(req, 0); !errors.Is(err, ErrCommandInvalid) {
		t.Fatalf("blocked waiter error = %v, want ErrCommandInvalid", err)
	}
	if err := r.errIfPoisoned(); !errors.Is(err, ErrCommandInvalid) {
		t.Fatalf("errIfPoisoned = %v, want ErrCommandInvalid", err)
	}

	// Resetting for a new session clears the poison state.
	r.reset()
	if err := r.errIfPoisoned(); err != nil {
		t.Fatalf("errIfPoisoned after reset = %v, want nil", err)
	}
}

func TestRPCManagerResetInvalidatesOldGeneration(t *testing.T) {
	r := newRPCManager()
	old, _ := r.begin(func() error { return nil })

	r.reset()

	// A reply left over from the old generation has no waiter in the new one.
	if r.deliver(&queueDeclareOk{Queue: "stale"}) {
		t.Fatal("reply from the old generation was delivered after reset")
	}

	// A waiter of the old generation is woken with ErrClosed by the reset, and
	// never with a reply from the new session.
	if _, err := r.wait(old, 0); err != ErrClosed {
		t.Fatalf("old waiter error = %v, want ErrClosed", err)
	}
}

func TestRPCManagerEndRemovesQueuedRequest(t *testing.T) {
	r := newRPCManager()
	req, _ := r.begin(func() error { return nil })
	r.end(req)

	if r.deliver(&queueDeclareOk{}) {
		t.Fatal("reply delivered for a request that was already retired")
	}
}
