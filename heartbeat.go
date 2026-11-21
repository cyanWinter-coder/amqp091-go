// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"sync/atomic"
	"time"
)

// Shutdown reasons used by the heartbeater, delivered to NotifyClose
// listeners. They let applications tell an idle peer and a blocked write
// path apart from an outright connection failure (a read or write error
// surfaced as a FrameError) when deciding whether to reconnect.
var (
	// ErrHeartbeatIdleTimeout is the shutdown reason when no complete frame,
	// heartbeat or otherwise, has arrived from the peer within two negotiated
	// heartbeat intervals.
	ErrHeartbeatIdleTimeout = &Error{Code: FrameError, Reason: "heartbeat: no frame received within the negotiated timeout", Recover: true}

	// ErrHeartbeatSendTimeout is the shutdown reason when the write path
	// stays blocked, for example by a congested peer, for two negotiated
	// heartbeat intervals so that no heartbeat could be sent in time.
	ErrHeartbeatSendTimeout = &Error{Code: FrameError, Reason: "heartbeat: send blocked beyond the negotiated timeout", Recover: true}
)

// heartbeatState tracks the last complete frame read from and written to the
// wire against the negotiated heartbeat interval. The reader and writer
// goroutines record activity; the heartbeater goroutine is the only consumer.
type heartbeatState struct {
	interval  atomic.Int64 // Negotiated heartbeat interval in nanoseconds; 0 disables heartbeats.
	lastRead  atomic.Int64 // UnixNano of the last complete frame read.
	lastWrite atomic.Int64 // UnixNano of the last complete frame written.
}

func newHeartbeatState() *heartbeatState {
	now := time.Now().UnixNano()
	h := &heartbeatState{}
	h.lastRead.Store(now)
	h.lastWrite.Store(now)
	return h
}

func (h *heartbeatState) setInterval(d time.Duration) { h.interval.Store(int64(d)) }
func (h *heartbeatState) negotiated() time.Duration   { return time.Duration(h.interval.Load()) }

// recordRead and recordWrite tolerate a nil receiver so that zero-value
// Connection literals (which have no heartbeat tracking) cannot panic the
// I/O paths.
func (h *heartbeatState) recordRead() {
	if h != nil {
		h.lastRead.Store(time.Now().UnixNano())
	}
}

func (h *heartbeatState) recordWrite() {
	if h != nil {
		h.lastWrite.Store(time.Now().UnixNano())
	}
}

func (h *heartbeatState) lastReadAt() time.Time  { return time.Unix(0, h.lastRead.Load()) }
func (h *heartbeatState) lastWriteAt() time.Time { return time.Unix(0, h.lastWrite.Load()) }

// heartbeater enforces the negotiated heartbeat interval in both directions.
// It writes a heartbeat frame whenever no complete frame has been written for
// half the interval, and shuts the connection down when no complete frame has
// been read for two full intervals, or when a blocked write path has kept any
// frame from being written for two full intervals. A zero negotiated interval
// disables heartbeats: the goroutine then only waits for shutdown.
func (c *Connection) heartbeater(done chan struct{}) {
	interval := c.heartbeat.negotiated()
	if interval <= 0 {
		<-done
		return
	}

	ticker := time.NewTicker(interval / 2)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			if c.isClosing() {
				// The close handshake owns the connection from here on. Stop
				// sending heartbeats and leave the shutdown reason to it.
				return
			}
			if !c.checkHeartbeat(now, interval) {
				return
			}
		}
	}
}

// checkHeartbeat applies one heartbeat tick and reports whether the
// heartbeater should keep running.
func (c *Connection) checkHeartbeat(now time.Time, interval time.Duration) bool {
	// The peer is considered dead when no complete frame has arrived within
	// two negotiated intervals, matching the broker's own tolerance.
	if now.Sub(c.heartbeat.lastReadAt()) > 2*interval {
		c.shutdown(ErrHeartbeatIdleTimeout)
		return false
	}

	if now.Sub(c.heartbeat.lastWriteAt()) < interval/2 {
		// A complete frame was written recently enough to count as traffic.
		return true
	}

	err := c.sendHeartbeat(now, interval)
	if err == nil {
		// Sent, or skipped because another write is in flight.
		return true
	}
	if c.isClosing() {
		// A close raced with the send. Leave the shutdown reason to it.
		return false
	}
	c.shutdown(err)
	return false
}

// sendHeartbeat writes a single heartbeat frame without queueing behind a
// blocked writer. When another goroutine holds the write lock, for example a
// publish stalled on a congested socket, the heartbeat is skipped instead of
// piling up behind the lock: the in-flight write counts as activity once it
// completes, and a write path that stays blocked past two negotiated
// intervals is reported as ErrHeartbeatSendTimeout.
func (c *Connection) sendHeartbeat(now time.Time, interval time.Duration) *Error {
	if !c.sendM.TryLock() {
		if now.Sub(c.heartbeat.lastWriteAt()) > 2*interval {
			return ErrHeartbeatSendTimeout
		}
		return nil
	}
	err := c.writer.WriteFrame(&heartbeatFrame{})
	c.sendM.Unlock()

	if err != nil {
		return &Error{Code: FrameError, Reason: err.Error()}
	}
	c.heartbeat.recordWrite()
	return nil
}
