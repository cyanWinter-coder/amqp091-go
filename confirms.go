// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"context"
	"sort"
	"sync"
)

// ConfirmationStatus is the terminal state of a DeferredConfirmation. Every
// published delivery tag reaches exactly one terminal state.
type ConfirmationStatus uint8

const (
	// ConfirmationPending means the publishing has not reached a terminal
	// state yet.
	ConfirmationPending ConfirmationStatus = iota
	// ConfirmationAcked means the broker acknowledged the publishing.
	ConfirmationAcked
	// ConfirmationNacked means the broker negatively acknowledged the
	// publishing.
	ConfirmationNacked
	// ConfirmationPublishFailed means the publishing could not be written to
	// the connection and was never seen by the broker.
	ConfirmationPublishFailed
	// ConfirmationChannelClosed means the channel or connection closed before
	// the broker confirmed the publishing.
	ConfirmationChannelClosed
)

// confirms tracks published delivery tags as a set of ranges and drives each
// tag to exactly one terminal state: acked, nacked, publish failed, or channel
// closed. Confirmations are delivered to listeners strictly in tag order, so a
// batched or out-of-order confirmation can neither skip a tag that is still
// pending nor override the terminal state of a completed one.
type confirms struct {
	m                     sync.Mutex
	listeners             []chan Confirmation
	pending               tagRanges
	completed             map[uint64]Confirmation
	deferredConfirmations *deferredConfirmations
	published             uint64
	publishedMut          sync.Mutex
	expecting             uint64
}

// newConfirms allocates a confirms
func newConfirms() *confirms {
	return &confirms{
		completed:             map[uint64]Confirmation{},
		deferredConfirmations: newDeferredConfirmations(),
		published:             0,
		expecting:             1,
	}
}

func (c *confirms) Listen(l chan Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	c.listeners = append(c.listeners, l)
}

// publish increments the publishing counter and records the new delivery tag
// as pending. The counter and the pending set are updated atomically so that
// concurrent publishes cannot insert tags out of order.
func (c *confirms) publish() *DeferredConfirmation {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()
	c.m.Lock()
	defer c.m.Unlock()

	c.published++
	c.pending.add(c.published)
	return c.deferredConfirmations.Add(c.published)
}

// unpublish removes the most recently published tag and resolves its
// DeferredConfirmation as failed. It must be called immediately after a
// publish fails.
func (c *confirms) unpublish() {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()
	c.m.Lock()
	defer c.m.Unlock()

	tag := c.published
	c.published--
	c.pending.removeLast(tag)
	c.deferredConfirmations.resolve(tag, ConfirmationPublishFailed)
}

// deliver notifies listeners of completed confirmations in strict tag order,
// stopping at the first tag that is still pending. It must be called with c.m
// held.
func (c *confirms) deliver() {
	for {
		confirmation, found := c.completed[c.expecting]
		if !found {
			return
		}
		delete(c.completed, c.expecting)
		c.expecting++
		notifyAll(c.listeners, confirmation)
	}
}

func statusOf(ack bool) ConfirmationStatus {
	if ack {
		return ConfirmationAcked
	}
	return ConfirmationNacked
}

// One terminally confirms a single publishing, provided its tag is still
// pending.
func (c *confirms) One(confirmation Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	if !c.pending.remove(confirmation.DeliveryTag) {
		// Duplicate, late, or never published tags are ignored so that every
		// publishing completes exactly once.
		return
	}
	c.deferredConfirmations.resolve(confirmation.DeliveryTag, statusOf(confirmation.Ack))
	c.completed[confirmation.DeliveryTag] = confirmation
	c.deliver()
}

// Multiple terminally confirms all still pending publishings up to and
// including the delivery tag. Tags that already reached a terminal state keep
// it, so a batched confirmation never overrides an earlier outcome, and tags
// beyond the published sequence are never completed.
func (c *confirms) Multiple(confirmation Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	status := statusOf(confirmation.Ack)
	for _, tag := range c.pending.removeUpTo(confirmation.DeliveryTag) {
		c.deferredConfirmations.resolve(tag, status)
		c.completed[tag] = Confirmation{DeliveryTag: tag, Ack: confirmation.Ack}
	}
	c.deliver()
}

// Close resolves all pending DeferredConfirmations as channel closed, closes
// all listeners, and discards any buffered confirmations.
func (c *confirms) Close() error {
	c.m.Lock()
	defer c.m.Unlock()

	c.deferredConfirmations.closeAll(ConfirmationChannelClosed)
	c.pending = tagRanges{}
	c.completed = map[uint64]Confirmation{}

	for _, l := range c.listeners {
		close(l)
	}
	c.listeners = nil
	return nil
}

// reset resolves all pending DeferredConfirmations as channel closed and
// resets the tracking state for recovery, while keeping the listeners intact.
func (c *confirms) reset() {
	c.publishedMut.Lock()
	c.published = 0
	c.publishedMut.Unlock()

	c.m.Lock()
	defer c.m.Unlock()

	c.expecting = 1
	c.pending = tagRanges{}
	c.completed = map[uint64]Confirmation{}
	c.deferredConfirmations.closeAll(ConfirmationChannelClosed)
}

// tagRange is an inclusive range of delivery tags.
type tagRange struct {
	lo, hi uint64
}

// tagRanges is a sorted set of disjoint, non-adjacent tag ranges. Its memory
// footprint is bounded by the number of pending tag runs, not by the total
// publish history, because confirmed tags are removed as soon as they reach a
// terminal state.
type tagRanges struct {
	ranges []tagRange
}

// add appends a tag that is one past the highest tag added so far.
func (s *tagRanges) add(tag uint64) {
	if n := len(s.ranges); n > 0 && s.ranges[n-1].hi == tag-1 {
		s.ranges[n-1].hi = tag
		return
	}
	s.ranges = append(s.ranges, tagRange{lo: tag, hi: tag})
}

// removeLast drops the most recently added tag.
func (s *tagRanges) removeLast(tag uint64) {
	n := len(s.ranges)
	if n == 0 || s.ranges[n-1].hi != tag {
		return
	}
	if s.ranges[n-1].lo == tag {
		s.ranges = s.ranges[:n-1]
	} else {
		s.ranges[n-1].hi--
	}
}

// remove deletes a single tag, returning false when it is not in the set.
func (s *tagRanges) remove(tag uint64) bool {
	i := sort.Search(len(s.ranges), func(i int) bool { return s.ranges[i].hi >= tag })
	if i == len(s.ranges) || s.ranges[i].lo > tag {
		return false
	}
	r := s.ranges[i]
	switch {
	case r.lo == r.hi:
		s.ranges = append(s.ranges[:i], s.ranges[i+1:]...)
	case r.lo == tag:
		s.ranges[i].lo++
	case r.hi == tag:
		s.ranges[i].hi--
	default:
		s.ranges[i].hi = tag - 1
		s.ranges = append(s.ranges, tagRange{})
		copy(s.ranges[i+2:], s.ranges[i+1:])
		s.ranges[i+1] = tagRange{lo: tag + 1, hi: r.hi}
	}
	return true
}

// removeUpTo deletes and returns, in ascending order, every tag less than or
// equal to tag.
func (s *tagRanges) removeUpTo(tag uint64) []uint64 {
	var tags []uint64
	consumed := 0
	for i, r := range s.ranges {
		if r.lo > tag {
			break
		}
		hi := r.hi
		if hi > tag {
			hi = tag
		}
		for t := r.lo; t <= hi; t++ {
			tags = append(tags, t)
		}
		if r.hi > tag {
			s.ranges[i].lo = tag + 1
			break
		}
		consumed = i + 1
	}
	if consumed > 0 {
		copy(s.ranges, s.ranges[consumed:])
		s.ranges = s.ranges[:len(s.ranges)-consumed]
	}
	return tags
}

type deferredConfirmations struct {
	m             sync.Mutex
	confirmations map[uint64]*DeferredConfirmation
}

func newDeferredConfirmations() *deferredConfirmations {
	return &deferredConfirmations{
		confirmations: map[uint64]*DeferredConfirmation{},
	}
}

func (d *deferredConfirmations) Add(tag uint64) *DeferredConfirmation {
	d.m.Lock()
	defer d.m.Unlock()

	dc := &DeferredConfirmation{DeliveryTag: tag}
	dc.done = make(chan struct{})
	d.confirmations[tag] = dc
	return dc
}

// resolve drives the DeferredConfirmation of a tag to its terminal state. It
// is a no-op for tags that are not pending, which keeps every confirmation
// completing exactly once.
func (d *deferredConfirmations) resolve(tag uint64, status ConfirmationStatus) {
	d.m.Lock()
	defer d.m.Unlock()

	dc, found := d.confirmations[tag]
	if !found {
		return
	}
	dc.setTerminal(status)
	delete(d.confirmations, tag)
}

// closeAll resolves every pending DeferredConfirmation with the given status,
// unblocking any goroutine blocked in Wait.
func (d *deferredConfirmations) closeAll(status ConfirmationStatus) {
	d.m.Lock()
	defer d.m.Unlock()

	for tag, dc := range d.confirmations {
		dc.setTerminal(status)
		delete(d.confirmations, tag)
	}
}

// setTerminal records the terminal state and unblocks waiters. Note that it
// must not be called more than once.
func (d *DeferredConfirmation) setTerminal(status ConfirmationStatus) {
	d.status = status
	d.ack = status == ConfirmationAcked
	close(d.done)
}

// Done returns the channel that can be used to wait for the publisher
// confirmation.
func (d *DeferredConfirmation) Done() <-chan struct{} {
	return d.done
}

// Acked returns the publisher confirmation in a non-blocking manner. It returns
// false if the confirmation was not acknowledged yet or received negative
// acknowledgement.
func (d *DeferredConfirmation) Acked() bool {
	select {
	case <-d.done:
	default:
		return false
	}
	return d.ack
}

// Status returns the terminal state of the confirmation, or
// ConfirmationPending if it has not completed yet. It allows distinguishing a
// broker negative acknowledgement from a failed publish or a closed channel,
// all of which report false from Acked and Wait.
func (d *DeferredConfirmation) Status() ConfirmationStatus {
	select {
	case <-d.done:
	default:
		return ConfirmationPending
	}
	return d.status
}

// Wait blocks until the publisher confirmation. It returns true if the server
// successfully received the publishing.
func (d *DeferredConfirmation) Wait() bool {
	<-d.done
	return d.ack
}

// WaitContext waits until the publisher confirmation. It returns true if the
// server successfully received the publishing. If the context expires before
// that, ctx.Err() is returned.
func (d *DeferredConfirmation) WaitContext(ctx context.Context) (bool, error) {
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case <-d.done:
	}
	return d.ack, nil
}
