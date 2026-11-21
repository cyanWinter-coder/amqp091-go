// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"context"
	"sync"
)

// confirms resequences and notifies one or multiple publisher confirmation listeners
type confirms struct {
	m                     sync.Mutex
	listeners             []chan Confirmation
	sequencer             map[uint64]Confirmation
	deferredConfirmations *deferredConfirmations
	published             uint64
	publishedMut          sync.Mutex
	expecting             uint64
}

// newConfirms allocates a confirms
func newConfirms() *confirms {
	return &confirms{
		sequencer:             map[uint64]Confirmation{},
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

// Publish increments the publishing counter
func (c *confirms) publish() *DeferredConfirmation {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()

	c.published++
	return c.deferredConfirmations.Add(c.published)
}

// unpublish decrements the publishing counter and settles the
// DeferredConfirmation with the publish error. It must be called immediately
// after a publish fails.
func (c *confirms) unpublish(err error) {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()
	c.deferredConfirmations.remove(c.published, err)
	c.published--
}

// confirm confirms one publishing, increments the expecting delivery tag, and
// removes bookkeeping for that delivery tag.
func (c *confirms) confirm(confirmation Confirmation) {
	delete(c.sequencer, c.expecting)
	c.expecting++
	notifyAll(c.listeners, confirmation)
}

// resequence confirms any out of order delivered confirmations
func (c *confirms) resequence() {
	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()

	for c.expecting <= c.published {
		sequenced, found := c.sequencer[c.expecting]
		if !found {
			return
		}
		c.confirm(sequenced)
	}
}

// One confirms one publishing and all following in the publishing sequence
func (c *confirms) One(confirmed Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	if confirmed.DeliveryTag < c.expecting {
		// The tag was already settled by an earlier (multiple) confirmation.
		// Drop the duplicate instead of parking it in the sequencer, where it
		// could never be resequenced and would leak.
		return
	}

	c.deferredConfirmations.Confirm(confirmed)

	if c.expecting == confirmed.DeliveryTag {
		c.confirm(confirmed)
	} else {
		c.sequencer[confirmed.DeliveryTag] = confirmed
	}
	c.resequence()
}

// Multiple confirms all publishings up until the delivery tag
func (c *confirms) Multiple(confirmed Confirmation) {
	c.m.Lock()
	defer c.m.Unlock()

	c.deferredConfirmations.ConfirmMultiple(confirmed)

	// Settle only the published tags covered by this batch. A tag beyond
	// published must not advance expecting, or later confirmations would be
	// dropped as stale.
	c.publishedMut.Lock()
	for c.expecting <= confirmed.DeliveryTag && c.expecting <= c.published {
		c.confirm(Confirmation{c.expecting, confirmed.Ack})
	}
	c.publishedMut.Unlock()
	c.resequence()
}

// Close settles every pending DeferredConfirmation with err as the reason,
// closes all listeners, and discards any out of sequence confirmations.
func (c *confirms) Close(err error) error {
	c.m.Lock()
	defer c.m.Unlock()

	if err == nil {
		err = ErrClosed
	}
	c.deferredConfirmations.Close(err)

	for _, l := range c.listeners {
		close(l)
	}
	c.listeners = nil
	c.sequencer = map[uint64]Confirmation{}
	return nil
}

// reset clears any pending deferred confirmations and resets the sequencer
// state for recovery, while keeping the listeners intact.
func (c *confirms) reset() {
	c.m.Lock()
	defer c.m.Unlock()

	c.publishedMut.Lock()
	defer c.publishedMut.Unlock()

	c.published = 0
	c.expecting = 1
	c.deferredConfirmations.Close(ErrClosed)
	c.sequencer = map[uint64]Confirmation{}
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

// remove settles the tag whose publish failed with the publish error
func (d *deferredConfirmations) remove(tag uint64, err error) {
	d.m.Lock()
	defer d.m.Unlock()
	dc, found := d.confirmations[tag]
	if !found {
		return
	}
	dc.settle(false, err)
	delete(d.confirmations, tag)
}

func (d *deferredConfirmations) Confirm(confirmation Confirmation) {
	d.m.Lock()
	defer d.m.Unlock()

	dc, found := d.confirmations[confirmation.DeliveryTag]
	if !found {
		// We should never receive a confirmation for a tag that hasn't
		// been published, but a test causes this to happen.
		return
	}
	dc.settle(confirmation.Ack, nil)
	delete(d.confirmations, confirmation.DeliveryTag)
}

func (d *deferredConfirmations) ConfirmMultiple(confirmation Confirmation) {
	d.m.Lock()
	defer d.m.Unlock()

	for k, v := range d.confirmations {
		if k <= confirmation.DeliveryTag {
			v.settle(confirmation.Ack, nil)
			delete(d.confirmations, k)
		}
	}
}

// Close settles all pending DeferredConfirmations as not acknowledged with
// err as the reason, unblocking any dc.Wait().
func (d *deferredConfirmations) Close(err error) {
	d.m.Lock()
	defer d.m.Unlock()

	for k, v := range d.confirmations {
		v.settle(false, err)
		delete(d.confirmations, k)
	}
}

// settle resolves the confirmation. Each tag is removed from the pending map
// as it is settled, so a tag can never be settled twice.
func (d *DeferredConfirmation) settle(ack bool, err error) {
	d.ack = ack
	d.err = err
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

// Err returns why the confirmation was settled without a broker response: the
// publish error when the publishing could not be sent, or the close error when
// the channel or connection closed before the broker confirmed. It returns nil
// while the confirmation is pending and once the broker has acknowledged or
// nacked the publishing; use Acked to tell those two apart.
func (d *DeferredConfirmation) Err() error {
	select {
	case <-d.done:
	default:
		return nil
	}
	return d.err
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
