// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var consumerSeq atomic.Uint64

const consumerTagLengthMax = 0xFF // see writeShortstr

// consumerDrainTimeout bounds how long a consumer's buffer goroutine keeps
// flushing queued deliveries to the application channel once the channel has
// been closed. Deliveries the application does not receive within this
// window are dropped and reported. It is a variable so tests can shorten it.
var consumerDrainTimeout = 2 * time.Second

func uniqueConsumerTag() string {
	return commandNameBasedUniqueConsumerTag(os.Args[0])
}

func commandNameBasedUniqueConsumerTag(commandName string) string {
	tagPrefix := "ctag-"
	tagInfix := commandName
	tagSuffix := "-" + strconv.FormatUint(consumerSeq.Add(1), 10)

	if len(tagPrefix)+len(tagInfix)+len(tagSuffix) > consumerTagLengthMax {
		tagInfix = "streadway/amqp"
	}

	return tagPrefix + tagInfix + tagSuffix
}

// consumerConfig stores the configuration parameters used when a consumer is registered.
// This is required during automatic connection and channel recovery. When a network connection
// drops and is restored, the library uses these stored parameters to automatically re-register
// and resume all consumers on the recovered channels seamlessly, preserving the subscriber topology.
type consumerConfig struct {
	Queue     string
	Consumer  string
	AutoAck   bool
	Exclusive bool
	NoLocal   bool
	NoWait    bool
	Args      Table
}

// consumerState identifies the lifecycle state of a single consumer
// registration. The state moves monotonically from active to draining, and
// the transition happens exactly once, however it is reached:
// Channel.Cancel, a server-sent basic.cancel, a tag conflict on
// re-registration, or channel teardown. Removal from the consumers registry
// is the terminal state.
type consumerState int

const (
	// consumerActive accepts new deliveries from the reader goroutine.
	consumerActive consumerState = iota
	// consumerDraining rejects new deliveries. Deliveries already accepted
	// are flushed to the application channel before it is closed.
	consumerDraining
)

// consumer tracks one consumer tag registration.
type consumer struct {
	tag   string
	in    chan *Delivery
	state consumerState // guarded by consumers.Mutex
}

// stopAccepting performs the active -> draining transition, exactly once.
// The caller must hold consumers.Mutex. It reports whether this call
// performed the transition.
func (c *consumer) stopAccepting() bool {
	if c.state != consumerActive {
		return false
	}
	c.state = consumerDraining
	close(c.in)
	return true
}

// consumerConfigs maps unique consumer tags to their respective consumerConfig parameters,
// facilitating the automatic re-registration of all consumers during channel recovery.
type consumerConfigs map[string]consumerConfig

// Concurrent type that manages the consumerTag ->
// ingress consumerBuffer mapping
type consumers struct {
	sync.WaitGroup               // one for buffer
	closed         chan struct{} // signal buffer

	sync.Mutex // protects below
	chans      map[string]*consumer
	configs    consumerConfigs
}

func makeConsumers() *consumers {
	return &consumers{
		closed:  make(chan struct{}),
		chans:   make(map[string]*consumer),
		configs: make(consumerConfigs),
	}
}

func (subs *consumers) buffer(c *consumer, out chan Delivery) {
	defer func() {
		close(out)
		// Remove the registration unless a newer consumer has already
		// replaced it under the same tag.
		subs.Lock()
		if subs.chans[c.tag] == c {
			delete(subs.chans, c.tag)
		}
		subs.Unlock()
		subs.Done()
	}()

	inflight := c.in
	var queue []*Delivery

	for delivery := range c.in {
		queue = append(queue, delivery)

		for len(queue) > 0 {
			select {
			case <-subs.closed:
				// Closed before drained: flush what remains within a
				// bounded window, then drop and report the rest.
				drainQueue(c.tag, queue, out)
				return

			case delivery, consuming := <-inflight:
				if consuming {
					queue = append(queue, delivery)
				} else {
					inflight = nil
				}

			case out <- *queue[0]:
				/*
				* https://github.com/rabbitmq/amqp091-go/issues/179
				* https://github.com/rabbitmq/amqp091-go/pull/180
				*
				* Comment from @lars-t-hansen:
				*
				* Given Go's slice semantics, and barring any information
				* available to the compiler that proves that queue is the only
				* pointer to the memory it references, the only meaning that
				* queue = queue[1:] can have is basically queue += sizeof(queue
				* element), ie, it bumps a pointer. Looking at the generated
				* code for a simple example (on ARM64 in this case) bears this
				* out. So what we're left with is an array that we have a
				* pointer into the middle of. When the GC traces this pointer,
				* it too does not know whether the array has multiple
				* referents, and so its only sensible choice is to find the
				* beginning of the array, and if the array is not already
				* visited, mark every element in it, including the "dead"
				* pointer.
				*
				* (Depending on the program dynamics, an element may eventually
				* be appended to the queue when the queue is at capacity, and
				* in this case the live elements are copied into a new array
				* and the old array is left to be GC'd eventually, along with
				* the dead object. But that can take time.)
				 */
				queue[0] = nil
				queue = queue[1:]
			}
		}
	}
}

// drainQueue flushes queued deliveries to out after the channel has been
// closed. Deliveries the application does not receive within
// consumerDrainTimeout are dropped and reported; they remain unacknowledged,
// so the broker requeues them when the channel closes.
func drainQueue(tag string, queue []*Delivery, out chan Delivery) {
	if len(queue) == 0 {
		return
	}
	timer := time.NewTimer(consumerDrainTimeout)
	defer timer.Stop()
	for len(queue) > 0 {
		select {
		case out <- *queue[0]:
			queue[0] = nil
			queue = queue[1:]
		case <-timer.C:
			Logger.Printf("consumer %s: dropping %d queued deliveries on channel close", tag, len(queue))
			return
		}
	}
}

// On key conflict, the previous registration is cancelled: it stops
// accepting new deliveries and its buffer goroutine drains and terminates on
// its own.
func (subs *consumers) add(tag string, out chan Delivery, config consumerConfig) {
	subs.Lock()
	defer subs.Unlock()

	if prev, found := subs.chans[tag]; found {
		prev.stopAccepting()
		delete(subs.chans, tag)
	}

	c := &consumer{
		tag:   tag,
		in:    make(chan *Delivery),
		state: consumerActive,
	}
	subs.chans[tag] = c
	subs.configs[tag] = config

	subs.Add(1)
	go subs.buffer(c, out)
}

// cancel transitions the consumer identified by tag from active to draining:
// no new deliveries are accepted, and deliveries already accepted are
// flushed to the application channel before it is closed. It reports whether
// this call performed the transition, so a repeated cancel, or one racing
// with channel teardown, returns false and leaves the terminal state
// untouched.
func (subs *consumers) cancel(tag string) (found bool) {
	subs.Lock()
	defer subs.Unlock()

	c, found := subs.chans[tag]
	if !found {
		return false
	}

	delete(subs.configs, tag)
	return c.stopAccepting()
}

// queueForTag returns the queue name for the given consumer tag without removing the consumer.
func (subs *consumers) queueForTag(tag string) (string, bool) {
	subs.Lock()
	defer subs.Unlock()
	if cfg, ok := subs.configs[tag]; ok {
		return cfg.Queue, true
	}
	return "", false
}

// hasConsumerForQueue reports whether any consumer is registered on the given queue.
func (subs *consumers) hasConsumerForQueue(queue string) bool {
	subs.Lock()
	defer subs.Unlock()
	for _, cfg := range subs.configs {
		if cfg.Queue == queue {
			return true
		}
	}
	return false
}

// cancelByQueue untracks and terminates all consumers registered on the given queue.
func (subs *consumers) cancelByQueue(queue string) {
	subs.Lock()
	var tags []string
	for tag, config := range subs.configs {
		if config.Queue == queue {
			tags = append(tags, tag)
		}
	}
	subs.Unlock()

	for _, tag := range tags {
		subs.cancel(tag)
	}
}

func (subs *consumers) close() {
	subs.Lock()

	close(subs.closed)

	for tag, c := range subs.chans {
		delete(subs.chans, tag)
		delete(subs.configs, tag)
		c.stopAccepting()
	}

	subs.Unlock()

	// Wait for the buffer goroutines to drain; each is bounded by
	// consumerDrainTimeout, so a slow consumer cannot block teardown
	// indefinitely.
	subs.Wait()
}

// Sends a delivery to a the consumer identified by `tag`.
// If unbuffered channels are used for Consume this method
// could block all deliveries until the consumer
// receives on the other end of the channel.
//
// It returns false — dropping the delivery — when the consumer is unknown or
// no longer active: a consumer that has begun cancellation does not accept
// new deliveries.
func (subs *consumers) send(tag string, msg *Delivery) bool {
	subs.Lock()
	defer subs.Unlock()

	c, found := subs.chans[tag]
	if !found || c.state != consumerActive {
		return false
	}

	c.in <- msg
	return true
}
