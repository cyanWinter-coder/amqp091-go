// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"sync"
	"time"
)

// rpcGeneration is one channel session. Every request captures the generation
// it was sent on, and a generation is retired (never reused) when the session
// ends, so a reply left over from an abandoned session cannot be mistaken for
// a reply to a later call.
type rpcGeneration struct {
	done   chan struct{}
	reason error
	closed bool
}

// rpcRequest is one synchronous request waiting for its reply. The sequence
// identifies the request within its generation; the reply channel is buffered
// so that delivering a reply never blocks the connection reader.
type rpcRequest struct {
	sequence   uint64
	generation uint64
	gen        *rpcGeneration
	reply      chan message

	// abandoned is set when the caller stops waiting before the reply
	// arrives. The request keeps its queue position so the broker's eventual
	// reply is consumed by it and discarded instead of reaching the next call.
	abandoned bool

	// delivered is set when the reply has been placed in reply.
	delivered bool
}

// rpcManager correlates synchronous channel requests with the broker's
// replies.
//
// AMQP 0-9-1 carries no client-side request id, and a channel is answered in
// the order in which the broker received its methods. Requests are therefore
// kept in a FIFO queue and a reply is offered only to the oldest pending
// request of the current generation. The generation changes when the channel
// session is reopened, which invalidates every leftover request at once.
//
// No method blocks while holding the manager lock, and a caller never waits
// for a reply while holding a Channel or Connection lock: heartbeat frames and
// close control frames stay writable no matter how long an RPC waits.
type rpcManager struct {
	mu sync.Mutex

	// sendMu orders "enqueue the waiter" with "write the request frame", so
	// the queue order always matches the order of the requests on the wire.
	sendMu sync.Mutex

	sequence   uint64
	generation uint64
	gen        *rpcGeneration
	pending    []*rpcRequest

	// poisonErr, once set, is the reason this channel can no longer host a
	// synchronous RPC. It is cleared only by reset, i.e. by a new session.
	poisonErr error
}

func newRPCManager() *rpcManager {
	return &rpcManager{
		gen: &rpcGeneration{done: make(chan struct{})},
	}
}

// begin registers the request's waiter and then writes the request frame.
// Both happen under sendMu, so concurrent callers enqueue in exactly the order
// their frames reach the wire. The waiter is registered before the frame is
// written because the reply can arrive as soon as the frame leaves: a reply
// must always find its waiter in the queue, never be dropped as unmatched.
func (r *rpcManager) begin(send func() error) (*rpcRequest, error) {
	r.sendMu.Lock()
	defer r.sendMu.Unlock()

	r.mu.Lock()
	r.sequence++
	req := &rpcRequest{
		sequence:   r.sequence,
		generation: r.generation,
		gen:        r.gen,
		reply:      make(chan message, 1),
	}
	r.pending = append(r.pending, req)
	r.mu.Unlock()

	if err := send(); err != nil {
		r.end(req)
		return nil, err
	}
	return req, nil
}

// end retires a request that has been answered or has given up. A request
// still queued (no reply arrived) is dropped from the queue; a request whose
// reply was already delivered needs no further bookkeeping.
func (r *rpcManager) end(req *rpcRequest) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if req.delivered || req.abandoned {
		return
	}
	for i, p := range r.pending {
		if p == req {
			r.pending = append(r.pending[:i], r.pending[i+1:]...)
			return
		}
	}
}

// deliver offers a fully-read reply to the oldest pending request. It reports
// false when there is no such request, or when the oldest request was
// abandoned after a timeout; either way the reply is dropped.
//
// The reply is handed to the waiter's buffered slot even when the waiter has
// already been woken (a timeout raced with the reply): the frame is consumed
// by whichever request is next in line, so it can never answer a later call.
func (r *rpcManager) deliver(msg message) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(r.pending) == 0 {
		return false
	}

	req := r.pending[0]
	r.pending[0] = nil
	r.pending = r.pending[1:]

	// A request can only be in the queue of its own generation, but check
	// explicitly so a reply is never handed to a waiter from another session.
	if req.gen != r.gen {
		return false
	}

	req.delivered = true
	req.reply <- msg
	return !req.abandoned
}

// wait blocks until the request's reply arrives, its generation ends, or the
// timeout elapses. A non-positive timeout preserves the historical behavior of
// waiting indefinitely.
func (r *rpcManager) wait(req *rpcRequest, timeout time.Duration) (message, error) {
	if timeout <= 0 {
		select {
		case msg := <-req.reply:
			return msg, nil
		case <-req.gen.done:
			return nil, r.generationReason(req.gen)
		}
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case msg := <-req.reply:
		return msg, nil
	case <-req.gen.done:
		return nil, r.generationReason(req.gen)
	case <-timer.C:
		// If the reply landed in the slot as the timer fired, use it instead
		// of reporting a spurious timeout. Otherwise leave the request queued
		// as a tombstone so its late reply is consumed and dropped.
		r.mu.Lock()
		if !req.delivered {
			req.abandoned = true
			r.mu.Unlock()
			return nil, ErrRPCTimeout
		}
		r.mu.Unlock()
		return <-req.reply, nil
	}
}

func (r *rpcManager) generationReason(gen *rpcGeneration) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if gen.reason != nil {
		return gen.reason
	}
	return ErrClosed
}

// fail ends the current generation, waking every waiter with err (or
// ErrClosed when err is nil). It is idempotent and preserves the first reason
// given for the generation.
func (r *rpcManager) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retireLocked(err)
}

// poison ends the current generation and records why the channel must not host
// another synchronous RPC (for example an unexpected reply type, which means
// the request and reply streams are out of sync). The poison state is cleared
// by reset only, i.e. by a new session.
func (r *rpcManager) poison(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.poisonErr == nil {
		r.poisonErr = err
	}
	r.retireLocked(err)
}

// errIfPoisoned returns the reason synchronous calls are refused, or nil.
func (r *rpcManager) errIfPoisoned() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.poisonErr
}

// reset starts a new generation for a channel session that is being reopened.
// Any waiter still registered on the old generation is woken with ErrClosed
// (or the reason it was already failed with) rather than being orphaned, and
// leftovers in queue as well as the poison state are discarded.
func (r *rpcManager) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.retireLocked(ErrClosed)
	r.generation++
	r.gen = &rpcGeneration{done: make(chan struct{})}
	r.poisonErr = nil
}

// retireLocked ends the current generation: waiters are woken with reason (or
// the reason the generation already carries) and the queue is dropped. The
// rpcManager lock must be held.
func (r *rpcManager) retireLocked(reason error) {
	gen := r.gen
	if gen.reason == nil && reason != nil {
		gen.reason = reason
	}
	if !gen.closed {
		gen.closed = true
		close(gen.done)
	}
	r.pending = nil
}
