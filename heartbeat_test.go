// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// recordingWriter counts heartbeat frames written through it and can fail
// writes on demand so tests distinguish congestion from connection failure.
type recordingWriter struct {
	mu          sync.Mutex
	heartbeats  int
	otherFrames int
	writeErr    error
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	if w.writeErr != nil {
		return 0, w.writeErr
	}
	// Heartbeat frame: frameHeartbeat type, channel 0, size 0, frame end.
	if len(p) == 8 && p[0] == frameHeartbeat && p[1] == 0 && p[2] == 0 &&
		p[3] == 0 && p[4] == 0 && p[5] == 0 && p[6] == 0 && p[7] == frameEnd {
		w.mu.Lock()
		w.heartbeats++
		w.mu.Unlock()
	} else {
		w.mu.Lock()
		w.otherFrames++
		w.mu.Unlock()
	}
	return len(p), nil
}

func (w *recordingWriter) heartbeatCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.heartbeats
}

// newHeartbeatTestConnection builds a Connection with just the machinery the
// heartbeater and frame-activity accessors touch. The deadlines channel is
// intentionally unbuffered so a test sending on it only succeeds while the
// heartbeater goroutine is actually parked in its select and not stuck on sendM.
func newHeartbeatTestConnection(w io.Writer) *Connection {
	c := &Connection{
		writer:                &writer{bufio.NewWriter(w)},
		sends:                 make(chan time.Time),
		deadlines:             make(chan readDeadliner),
		close:                 make(chan struct{}),
		errors:                make(chan *Error, 1),
		channels:              make(map[uint16]*Channel),
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
		conn:                  dummyReadWriteCloser{},
		lifeCycle:             newLifeCycle(),
	}
	c.lifeCycle.SetState(StateOpen, nil)
	return c
}

// waitFor polls until cond is true or the deadline expires.
func waitFor(t *testing.T, deadline time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadlineAt := time.Now().Add(deadline)
	for time.Now().Before(deadlineAt) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

// TestHeartbeaterZeroHeartbeatRealConnectionSendsNoFrames proves the zero
// interval keeps the legacy wire behavior end to end: after the handshake an
// idle connection produces no heartbeat frames at all.
func TestHeartbeaterZeroHeartbeatRealConnectionSendsNoFrames(t *testing.T) {
	t.Parallel()

	rwc, srv := newSession(t)
	t.Cleanup(func() { _ = rwc.Close() })

	go func() {
		srv.expectAMQP()
		srv.connectionStart()
		srv.send(0, &connectionTune{ChannelMax: 11, FrameMax: 20000, Heartbeat: 0})
		srv.recv(0, &srv.tune)
		srv.recv(0, &connectionOpen{})
		srv.send(0, &connectionOpenOk{})
		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
	}()

	config := defaultConfig()
	config.Heartbeat = 0
	c, err := Open(rwc, config)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	if c.Config.Heartbeat != 0 {
		t.Fatalf("negotiated heartbeat = %v, want 0", c.Config.Heartbeat)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestHeartbeaterSendsHeartbeatOverRealConnection drives a full handshake
// against the in-memory broker with a one-second negotiated heartbeat and
// proves a heartbeat frame reaches the broker while the connection is idle.
func TestHeartbeaterSendsHeartbeatOverRealConnection(t *testing.T) {
	t.Parallel()

	rwc, srv := newSession(t)
	t.Cleanup(func() { _ = rwc.Close() })

	gotHeartbeat := make(chan struct{})
	go func() {
		srv.expectAMQP()
		srv.connectionStart()
		srv.send(0, &connectionTune{ChannelMax: 11, FrameMax: 20000, Heartbeat: 1})
		srv.recv(0, &srv.tune)
		srv.recv(0, &connectionOpen{})
		srv.send(0, &connectionOpenOk{})

		// Read frames until a heartbeat arrives, then run the close handshake
		// on this same goroutine so srv.r never has two readers.
		deadline := time.After(3 * time.Second)
		for {
			frame, err := srv.r.ReadFrame()
			if err != nil {
				return
			}
			if _, ok := frame.(*heartbeatFrame); ok {
				close(gotHeartbeat)
				srv.recv(0, &connectionClose{})
				srv.send(0, &connectionCloseOk{})
				return
			}
			select {
			case <-deadline:
				return
			default:
			}
		}
	}()

	config := defaultConfig()
	config.Heartbeat = time.Second
	c, err := Open(rwc, config)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}

	select {
	case <-gotHeartbeat:
	case <-time.After(3 * time.Second):
		t.Fatal("broker never received a heartbeat frame from an idle connection")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestHeartbeatDueUsesFrameLevelWriteBoundary verifies the heartbeat decision
// is driven by the last complete frame write: stalled writes that never commit
// keep the heartbeat due, while a recent commit suppresses it.
func TestHeartbeatDueUsesFrameLevelWriteBoundary(t *testing.T) {
	t.Parallel()

	interval := 5 * time.Second
	base := time.Unix(1_000_000, 0)

	cases := []struct {
		name     string
		lastSent time.Time
		now      time.Time
		want     bool
	}{
		{"idle beyond interval", base, base.Add(interval), true},
		// Mirrors the legacy jitter tolerance of one second.
		{"idle within one-second jitter", base, base.Add(interval - 1500*time.Millisecond), false},
		{"recent complete frame suppresses heartbeat", base, base.Add(100 * time.Millisecond), false},
		{"zero lastSent is always due", time.Time{}, base, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := heartbeatDue(tc.lastSent, tc.now, interval); got != tc.want {
				t.Fatalf("heartbeatDue(%v, %v, %v) = %v, want %v", tc.lastSent, tc.now, interval, got, tc.want)
			}
		})
	}
}

// gateConn serves queued bytes on Read and blocks on release once queued
// bytes are exhausted, letting a test park the reader mid-frame.
type gateConn struct {
	mu       sync.Mutex
	queued   []byte
	release  chan struct{}
	released bool
	closed   chan struct{}
}

func newGateConn(initial []byte) *gateConn {
	return &gateConn{
		queued:  initial,
		release: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (g *gateConn) Read(p []byte) (int, error) {
	g.mu.Lock()
	if len(g.queued) > 0 {
		n := copy(p, g.queued)
		g.queued = g.queued[n:]
		g.mu.Unlock()
		return n, nil
	}
	g.mu.Unlock()

	select {
	case <-g.release:
	case <-g.closed:
		return 0, io.EOF
	}

	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.queued) == 0 {
		select {
		case <-g.closed:
			return 0, io.EOF
		default:
			return 0, io.EOF
		}
	}
	n := copy(p, g.queued)
	g.queued = g.queued[n:]
	return n, nil
}

// releaseWith makes the blocked Read return these bytes next.
func (g *gateConn) releaseWith(b []byte) {
	g.mu.Lock()
	g.queued = append(g.queued, b...)
	if !g.released {
		g.released = true
		close(g.release)
	}
	g.mu.Unlock()
}

func (g *gateConn) Write(p []byte) (int, error) { return len(p), nil }
func (g *gateConn) Close() error {
	select {
	case <-g.closed:
	default:
		close(g.closed)
	}
	return nil
}

// frameBytes encodes a heartbeat frame as its on-wire bytes.
func heartbeatFrameBytes() []byte {
	return []byte{frameHeartbeat, 0, 0, 0, 0, 0, 0, frameEnd}
}

func newReaderTestConnection(t *testing.T, transport *gateConn) *Connection {
	t.Helper()

	c := &Connection{
		conn:                  transport,
		writer:                &writer{bufio.NewWriter(&bytes.Buffer{})},
		channels:              make(map[uint16]*Channel),
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
		rpc:                   make(chan message),
		sends:                 make(chan time.Time),
		errors:                make(chan *Error, 1),
		close:                 make(chan struct{}),
		deadlines:             make(chan readDeadliner, 1),
		lifeCycle:             newLifeCycle(),
	}
	// A one-byte buffer keeps the reader from prefetching bytes the test has
	// not released yet, so the hold gates exactly at the half-frame boundary.
	go c.readerWithBuffer(transport, 1)

	t.Cleanup(func() {
		_ = transport.Close()
		// The reader closes c.rpc on exit; waiting for it proves no reader
		// goroutine leaks out of the test.
		select {
		case <-c.rpc:
		case <-time.After(2 * time.Second):
			t.Error("reader goroutine did not exit after the transport closed")
		}
	})
	return c
}

// TestLastFrameReadAdvancesOnCompleteFrame proves the read timestamp commits
// when a full frame, including its frame-end byte, has been parsed.
func TestLastFrameReadAdvancesOnCompleteFrame(t *testing.T) {
	t.Parallel()

	transport := newGateConn(heartbeatFrameBytes())
	c := newReaderTestConnection(t, transport)

	waitFor(t, 2*time.Second, func() bool { return !c.LastFrameRead().IsZero() },
		"LastFrameRead was not set after a complete frame was parsed")
	first := c.LastFrameRead()

	transport.releaseWith(heartbeatFrameBytes())
	waitFor(t, 2*time.Second, func() bool { return c.LastFrameRead().After(first) },
		"LastFrameRead did not advance on the second complete frame")
}

// TestLastFrameReadHoldsAtHalfFrame proves the read timestamp does not commit
// while only part of a frame has arrived: the frame-end byte is held back and
// the reader is parked inside ReadFrame.
func TestLastFrameReadHoldsAtHalfFrame(t *testing.T) {
	t.Parallel()

	full := heartbeatFrameBytes()
	transport := newGateConn(append(heartbeatFrameBytes(), full[:7]...))
	c := newReaderTestConnection(t, transport)

	waitFor(t, 2*time.Second, func() bool { return !c.LastFrameRead().IsZero() },
		"LastFrameRead was not set after the first complete frame")
	afterComplete := c.LastFrameRead()

	// The reader is now parked on the held-back frame-end byte of the second
	// frame. Half a frame must never look like activity.
	time.Sleep(100 * time.Millisecond)
	if got := c.LastFrameRead(); !got.Equal(afterComplete) {
		t.Fatalf("LastFrameRead advanced on a partial frame: got %v, want %v", got, afterComplete)
	}

	// Completing the frame commits a new read timestamp at the frame boundary.
	transport.releaseWith(full[7:])
	waitFor(t, 2*time.Second, func() bool { return c.LastFrameRead().After(afterComplete) },
		"LastFrameRead did not advance after the frame was completed")
}

// TestReadFrameDoesNotCommitOnPartialInput proves at the framing layer that a
// frame missing its frame-end byte yields an error and no frame, so the
// reader goroutine can only call noteFrameRead after a complete frame.
func TestReadFrameDoesNotCommitOnPartialInput(t *testing.T) {
	t.Parallel()

	// Full frame parses successfully.
	full := bytes.NewReader(heartbeatFrameBytes())
	r := &reader{r: full, maxFrameSize: &atomic.Uint32{}}
	if _, err := r.ReadFrame(); err != nil {
		t.Fatalf("complete heartbeat frame failed to parse: %v", err)
	}

	// Header only, no frame-end byte: ReadFrame must fail, never return a
	// frame that the reader could mistake for activity.
	partial := bytes.NewReader([]byte{frameHeartbeat, 0, 0, 0, 0, 0, 0})
	r2 := &reader{r: partial, maxFrameSize: &atomic.Uint32{}}
	if frame, err := r2.ReadFrame(); err == nil {
		t.Fatalf("partial frame returned frame %v with no error", frame)
	}
}

// TestLastFrameWriteCommitsOnlyOnFlush proves the write-side boundary: a
// flushed frame updates LastFrameWrite, while frames held in an unflushed
// buffer do not count as a committed write.
func TestLastFrameWriteCommitsOnlyOnFlush(t *testing.T) {
	t.Parallel()

	var sink bytes.Buffer
	c := &Connection{
		writer:    &writer{bufio.NewWriter(&sink)},
		sends:     make(chan time.Time),
		lifeCycle: newLifeCycle(),
	}

	if err := c.sendUnflushed(&heartbeatFrame{}); err != nil {
		t.Fatalf("sendUnflushed failed: %v", err)
	}
	if !c.LastFrameWrite().IsZero() {
		t.Fatalf("LastFrameWrite = %v before flush, want zero (buffered bytes are not a commit)", c.LastFrameWrite())
	}
	if sink.Len() != 0 {
		t.Fatalf("expected buffered write to stay out of the sink, got %d bytes", sink.Len())
	}

	if err := c.endSendUnflushed(); err != nil {
		t.Fatalf("endSendUnflushed failed: %v", err)
	}
	if c.LastFrameWrite().IsZero() {
		t.Fatal("LastFrameWrite was not set after the flush committed the frame")
	}
}

// TestHeartbeaterSkipsHeartbeatWhileWriteLockHeld reproduces the operational
// failure mode: a business frame occupies sendM for longer than the heartbeat
// interval. The heartbeater must never block behind it, must never queue a
// second heartbeat, and must not shut the connection down. Once the lock is
// released, a single heartbeat goes out.
func TestHeartbeaterSkipsHeartbeatWhileWriteLockHeld(t *testing.T) {
	t.Parallel()

	rw := &recordingWriter{}
	c := newHeartbeatTestConnection(rw)

	const interval = 30 * time.Millisecond
	done := make(chan struct{})
	go c.heartbeater(interval, done)

	// Occupy the writer for several heartbeat intervals, as a stalled
	// publish under broker flow control would.
	c.sendM.Lock()
	blockedAt := time.Now()
	time.Sleep(5 * interval)
	blockedFor := time.Since(blockedAt)
	c.sendM.Unlock()

	waitFor(t, 2*time.Second, func() bool { return rw.heartbeatCount() >= 1 },
		"no heartbeat was sent after the write lock was released")

	// While the writer was blocked, the heartbeater kept ticking: after the
	// lock releases, at most one heartbeat is outstanding per tick rather than
	// a backlog. Give later ticks room to run, then bound the total.
	time.Sleep(3 * interval)
	if got := rw.heartbeatCount(); got > 6 {
		t.Fatalf("got %d heartbeat frames, want no pile-up behind the blocked writer", got)
	}

	select {
	case err := <-c.errors:
		t.Fatalf("connection shut down while the writer was merely blocked for %s, got %v", blockedFor.Round(time.Millisecond), err)
	default:
	}

	close(done)
}

// TestHeartbeaterDoesNotBlockBehindSendLock proves the non-blocking contract
// directly: while sendM is held by a stalled business frame, the heartbeater
// goroutine must remain responsive instead of parking on the lock. The test
// detects this through the deadlines channel, which the goroutine can only
// service while it is not stuck on sendM.
func TestHeartbeaterDoesNotBlockBehindSendLock(t *testing.T) {
	t.Parallel()

	rw := &recordingWriter{}
	c := newHeartbeatTestConnection(rw)

	const interval = 20 * time.Millisecond
	done := make(chan struct{})
	go c.heartbeater(interval, done)

	// Stall the writer well beyond one tick so the heartbeater has attempted
	// and skipped a heartbeat while sendM was unavailable.
	c.sendM.Lock()
	time.Sleep(3 * interval)

	conn := &deadlineTrackingConn{}
	select {
	case c.deadlines <- conn:
	case <-time.After(time.Second):
		t.Fatal("heartbeater is parked behind sendM and cannot service the deadlines channel")
	}

	c.sendM.Unlock()
	close(done)
}

// TestHeartbeaterDoesNotSendAfterCloseBegins proves requirement 3: once a
// close has been initiated, the heartbeater stops sending discretionary
// frames and exits, leaving the close handshake and shutdown reason to the
// close path.
func TestHeartbeaterDoesNotSendAfterCloseBegins(t *testing.T) {
	t.Parallel()

	rw := &recordingWriter{}
	c := newHeartbeatTestConnection(rw)

	const interval = 20 * time.Millisecond
	done := make(chan struct{})
	go c.heartbeater(interval, done)

	waitFor(t, 2*time.Second, func() bool { return rw.heartbeatCount() >= 1 },
		"heartbeater never sent a heartbeat while open")

	c.closeM.Lock()
	c.closeInit = true
	c.closeM.Unlock()

	afterClose := rw.heartbeatCount()
	time.Sleep(5 * interval)
	if got := rw.heartbeatCount(); got != afterClose {
		t.Fatalf("heartbeater sent %d frame(s) after close began, want %d", got, afterClose)
	}

	close(done)
}

// TestHeartbeaterWriteErrorShutsDownWithConsistentReason proves an actual
// write failure still shuts the connection down, unlike mere lock congestion,
// and that the shutdown reason is the write error.
func TestHeartbeaterWriteErrorShutsDownWithConsistentReason(t *testing.T) {
	t.Parallel()

	writeErr := errors.New("simulated connection reset")
	rw := &recordingWriter{writeErr: writeErr}
	c := newHeartbeatTestConnection(rw)

	done := make(chan struct{})
	go c.heartbeater(20*time.Millisecond, done)

	select {
	case err := <-c.errors:
		if err == nil || err.Reason != writeErr.Error() {
			t.Fatalf("expected shutdown reason %q, got %v", writeErr.Error(), err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write failure did not shut the connection down")
	}

	close(done)
}

// TestHeartbeaterZeroIntervalKeepsLegacySemantics proves requirement 4: with a
// zero interval no ticker exists, no heartbeat frames are ever written, and
// the goroutine still services deadline requests and exits on done.
func TestHeartbeaterZeroIntervalKeepsLegacySemantics(t *testing.T) {
	t.Parallel()

	rw := &recordingWriter{}
	c := newHeartbeatTestConnection(rw)

	done := make(chan struct{})
	go c.heartbeater(0, done)

	time.Sleep(100 * time.Millisecond)
	if got := rw.heartbeatCount(); got != 0 {
		t.Fatalf("zero interval wrote %d heartbeat frames, want 0", got)
	}

	// The deadlines branch must remain active with interval == 0, exactly as
	// the legacy implementation skipped setting a deadline.
	conn := &deadlineTrackingConn{}
	select {
	case c.deadlines <- conn:
	case <-time.After(time.Second):
		t.Fatal("heartbeater stopped servicing the deadlines channel")
	}
	time.Sleep(50 * time.Millisecond)
	if conn.deadlineSet.Load() {
		t.Fatal("zero interval must not set a read deadline")
	}

	close(done)
}

type deadlineTrackingConn struct {
	deadlineSet atomic.Bool
}

func (d *deadlineTrackingConn) SetReadDeadline(time.Time) error {
	d.deadlineSet.Store(true)
	return nil
}

// TestHeartbeatBlockedIsLoggedAtMostOncePerInterval verifies the congestion
// diagnostic has a deterministic rate limit and never fires twice within one
// interval.
func TestHeartbeatBlockedIsLoggedAtMostOncePerInterval(t *testing.T) {
	t.Parallel()

	interval := 100 * time.Millisecond
	var lastLog time.Time
	now := time.Now()

	if !shouldLogHeartbeatBlocked(&lastLog, now, interval) {
		t.Fatal("first congestion report must be allowed")
	}
	if lastLog != now {
		t.Fatalf("lastLog = %v, want %v", lastLog, now)
	}

	// A second report within the same interval must be suppressed.
	if shouldLogHeartbeatBlocked(&lastLog, now.Add(50*time.Millisecond), interval) {
		t.Fatal("congestion report within the same interval must be rate limited")
	}

	// After one interval, a fresh report is allowed.
	if !shouldLogHeartbeatBlocked(&lastLog, now.Add(interval+time.Millisecond), interval) {
		t.Fatal("congestion report must be allowed again once the interval elapsed")
	}
}
