// Copyright (c) 2021 VMware, Inc. or its affiliates. All Rights Reserved.
// Copyright (c) 2012-2021, Sean Treadway, SoundCloud Ltd.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

// blockingConn is an in-memory transport for heartbeater tests: writes are
// recorded and never block, reads block until Close is called.
type blockingConn struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	closed chan struct{}
	once   sync.Once
}

func newBlockingConn() *blockingConn {
	return &blockingConn{closed: make(chan struct{})}
}

func (c *blockingConn) Read([]byte) (int, error) {
	<-c.closed
	return 0, io.EOF
}

func (c *blockingConn) Write(p []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.Write(p)
}

func (c *blockingConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func (c *blockingConn) writtenHeartbeatFrames() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	r := &reader{r: bytes.NewReader(c.buf.Bytes())}
	n := 0
	for {
		f, err := r.ReadFrame()
		if err != nil {
			return n
		}
		if _, ok := f.(*heartbeatFrame); ok {
			n++
		}
	}
}

// newHeartbeatTestConnection returns a Connection with just enough state for
// the heartbeater and reader to run against the given transport.
func newHeartbeatTestConnection(conn io.ReadWriteCloser, interval time.Duration) *Connection {
	c := &Connection{
		conn:                  conn,
		writer:                &writer{bufio.NewWriter(conn)},
		channels:              make(map[uint16]*Channel),
		topologyConfiguration: make(map[uint16]*TopologyConfiguration),
		rpc:                   make(chan message),
		errors:                make(chan *Error, 1),
		close:                 make(chan struct{}),
		heartbeat:             newHeartbeatState(),
		lifeCycle:             newLifeCycle(),
	}
	c.heartbeat.setInterval(interval)
	c.maxFrameSize.Store(frameMinSize)
	return c
}

func waitForHeartbeats(t *testing.T, conn *blockingConn, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if conn.writtenHeartbeatFrames() >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("expected at least %d heartbeat frames, got %d", n, conn.writtenHeartbeatFrames())
}

// TestHeartbeaterSendsHeartbeatWhenIdle verifies that the heartbeater fills
// idle periods with heartbeat frames at the negotiated interval.
func TestHeartbeaterSendsHeartbeatWhenIdle(t *testing.T) {
	bc := newBlockingConn()
	c := newHeartbeatTestConnection(bc, 40*time.Millisecond)
	defer c.shutdown(nil)

	go c.heartbeater(c.close)

	waitForHeartbeats(t, bc, 2)
}

// TestHeartbeaterBusinessTrafficSuppressesHeartbeats verifies that recently
// written business frames count as activity, so no heartbeat is sent while
// the connection is not idle.
func TestHeartbeaterBusinessTrafficSuppressesHeartbeats(t *testing.T) {
	bc := newBlockingConn()
	c := newHeartbeatTestConnection(bc, 100*time.Millisecond)
	defer c.shutdown(nil)

	go c.heartbeater(c.close)

	// Keep writing business frames faster than the send interval.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if err := c.send(&bodyFrame{ChannelId: 1, Body: []byte("x")}); err != nil {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}
	}()

	// Across several send intervals no heartbeat frame may appear.
	time.Sleep(300 * time.Millisecond)
	if n := bc.writtenHeartbeatFrames(); n != 0 {
		t.Fatalf("expected business traffic to suppress heartbeats, got %d", n)
	}
}

// TestHeartbeaterIdleTimeoutShutdown verifies that a peer which stops sending
// any frames is declared dead after two negotiated intervals, and that the
// reader goroutine unblocked by the shutdown keeps the same close reason
// instead of overwriting it with its own read error.
func TestHeartbeaterIdleTimeoutShutdown(t *testing.T) {
	bc := newBlockingConn()
	c := newHeartbeatTestConnection(bc, 20*time.Millisecond)
	notify := c.NotifyClose(make(chan *Error, 1))

	go c.reader(bc)
	go c.heartbeater(c.close)

	select {
	case err := <-notify:
		if err != ErrHeartbeatIdleTimeout {
			t.Fatalf("expected ErrHeartbeatIdleTimeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeater did not shut down the idle connection")
	}

	if !c.IsClosed() {
		t.Fatal("expected connection to be marked closed")
	}
}

// TestHeartbeaterSendBlockedOnCongestedWriter verifies that when a business
// write stalls on a congested transport and holds the write lock, the
// heartbeater does not queue a heartbeat behind it but reports the blocked
// write path once it is overdue.
func TestHeartbeaterSendBlockedOnCongestedWriter(t *testing.T) {
	pr, pw := io.Pipe()
	c := newHeartbeatTestConnection(pipe{r: pr, w: pw}, 20*time.Millisecond)
	notify := c.NotifyClose(make(chan *Error, 1))

	// A business write that stalls on the congested transport holds the
	// write lock, so the heartbeater cannot write its heartbeat.
	sendDone := make(chan error, 1)
	go func() { sendDone <- c.send(&bodyFrame{ChannelId: 1, Body: []byte("stall")}) }()

	// Give the send time to block inside the transport write.
	time.Sleep(50 * time.Millisecond)

	// The peer is still alive and sending (half-duplex congestion), so only
	// the write side is overdue.
	c.heartbeat.recordRead()

	go c.heartbeater(c.close)

	select {
	case err := <-notify:
		if err != ErrHeartbeatSendTimeout {
			t.Fatalf("expected ErrHeartbeatSendTimeout, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeater did not detect the blocked write path")
	}

	if err := <-sendDone; err == nil {
		t.Fatal("expected the blocked send to fail once the connection shut down")
	}
}

// TestSendHeartbeatSkippedWhileWriteLockHeld verifies the lock-free decision
// the heartbeater makes while another goroutine holds the write lock: a
// recent write counts as activity, a stale one past two intervals is
// reported as send-blocked, and no heartbeat piles up behind the lock.
func TestSendHeartbeatSkippedWhileWriteLockHeld(t *testing.T) {
	bc := newBlockingConn()
	c := newHeartbeatTestConnection(bc, time.Minute)

	// Simulate an in-flight business write holding the write lock.
	c.sendM.Lock()

	// A recent write counts as activity: the heartbeat is skipped.
	if err := c.sendHeartbeat(time.Now(), time.Minute); err != nil {
		t.Fatalf("expected skipped heartbeat to return nil, got %v", err)
	}
	if n := bc.writtenHeartbeatFrames(); n != 0 {
		t.Fatalf("expected no heartbeat frame while the write lock is held, got %d", n)
	}

	// A write path blocked past two negotiated intervals is send-blocked.
	c.heartbeat.lastWrite.Store(time.Now().Add(-3 * time.Minute).UnixNano())
	if err := c.sendHeartbeat(time.Now(), time.Minute); err != ErrHeartbeatSendTimeout {
		t.Fatalf("expected ErrHeartbeatSendTimeout, got %v", err)
	}

	// Once the lock is released, a single heartbeat is written.
	c.sendM.Unlock()
	if err := c.sendHeartbeat(time.Now(), time.Minute); err != nil {
		t.Fatalf("expected heartbeat send to succeed, got %v", err)
	}
	if n := bc.writtenHeartbeatFrames(); n != 1 {
		t.Fatalf("expected exactly one heartbeat frame, got %d", n)
	}
}

// TestHeartbeaterStopsDuringClose verifies that once the close handshake has
// been initiated the heartbeater exits instead of sending further heartbeats.
func TestHeartbeaterStopsDuringClose(t *testing.T) {
	bc := newBlockingConn()
	c := newHeartbeatTestConnection(bc, 20*time.Millisecond)

	// Mark the close handshake as initiated, as beginClose does.
	c.closeM.Lock()
	c.closeInit = true
	c.closeM.Unlock()

	exited := make(chan struct{})
	go func() {
		c.heartbeater(c.close)
		close(exited)
	}()

	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeater did not exit once the connection started closing")
	}

	if n := bc.writtenHeartbeatFrames(); n != 0 {
		t.Fatalf("expected no heartbeats during close, got %d", n)
	}
}

// TestHeartbeaterDisabledWithZeroInterval verifies that a zero negotiated
// interval keeps heartbeats fully disabled: no frames are written and the
// connection is never shut down for being idle.
func TestHeartbeaterDisabledWithZeroInterval(t *testing.T) {
	bc := newBlockingConn()
	c := newHeartbeatTestConnection(bc, 0)

	exited := make(chan struct{})
	go func() {
		c.heartbeater(c.close)
		close(exited)
	}()

	time.Sleep(100 * time.Millisecond)
	if c.IsClosed() {
		t.Fatal("heartbeat-disabled connection closed while idle")
	}
	if n := bc.writtenHeartbeatFrames(); n != 0 {
		t.Fatalf("expected no heartbeat frames with a zero interval, got %d", n)
	}

	close(c.close)
	select {
	case <-exited:
	case <-time.After(2 * time.Second):
		t.Fatal("heartbeater did not exit on close")
	}
}

// TestHeartbeatFramesInterleavedWithContent verifies that server heartbeat
// frames arriving between the method, header, and body frames of a delivery
// are parsed frame by frame on channel 0 without losing any of the delivery's
// content.
func TestHeartbeatFramesInterleavedWithContent(t *testing.T) {
	const tag = "consumer-tag"

	rwc, srv := newSession(t)
	defer rwc.Close()

	go func() {
		srv.connectionOpen()
		srv.channelOpen(1)

		srv.recv(1, &basicConsume{})
		srv.send(1, &basicConsumeOk{ConsumerTag: tag})

		write := func(f frame) {
			if err := srv.w.WriteFrame(f); err != nil {
				t.Errorf("WriteFrame error: %v", err)
			}
		}
		write(&methodFrame{ChannelId: 1, Method: &basicDeliver{ConsumerTag: tag, DeliveryTag: 1}})
		write(&heartbeatFrame{})
		write(&headerFrame{ChannelId: 1, ClassId: 60, Size: 5})
		write(&heartbeatFrame{})
		write(&bodyFrame{ChannelId: 1, Body: []byte("hel")})
		write(&heartbeatFrame{})
		write(&bodyFrame{ChannelId: 1, Body: []byte("lo")})
		write(&heartbeatFrame{})

		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
	}()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("could not create connection: %v", err)
	}

	ch, err := c.Channel()
	if err != nil {
		t.Fatalf("could not open channel: %v", err)
	}

	consumer, err := ch.Consume("queue", tag, false, false, false, false, nil)
	if err != nil {
		t.Fatalf("unexpected error during consumer: %v", err)
	}

	select {
	case d := <-consumer:
		if string(d.Body) != "hello" {
			t.Fatalf("unexpected delivery body: %q", d.Body)
		}
		if d.DeliveryTag != 1 {
			t.Fatalf("unexpected delivery tag: %d", d.DeliveryTag)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("delivery lost while interleaved with heartbeat frames")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("unexpected error during connection close: %v", err)
	}
}

// TestHeartbeatNegotiationSetsTrackedInterval verifies that the interval
// negotiated in connection.tune is what the heartbeater tracks.
func TestHeartbeatNegotiationSetsTrackedInterval(t *testing.T) {
	rwc, srv := newSession(t)
	defer rwc.Close()

	go func() {
		srv.expectAMQP()
		srv.connectionStart()
		srv.send(0, &connectionTune{ChannelMax: 11, FrameMax: 20000, Heartbeat: 7})
		srv.recv(0, &srv.tune)
		srv.recv(0, &connectionOpen{})
		srv.send(0, &connectionOpenOk{})
		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
	}()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("could not create connection: %v", err)
	}

	if got := c.heartbeat.negotiated(); got != 7*time.Second {
		t.Fatalf("expected negotiated heartbeat interval 7s, got %v", got)
	}
	if srv.tune.Heartbeat != 7 {
		t.Fatalf("expected tune-ok to advertise heartbeat 7, got %d", srv.tune.Heartbeat)
	}

	if err := c.Close(); err != nil {
		t.Fatalf("unexpected error during connection close: %v", err)
	}
}

// TestHeartbeatZeroNegotiationPreservesDisabledBehavior verifies that
// negotiating a zero heartbeat keeps heartbeats disabled end to end: the
// connection stays open while idle and closes gracefully.
func TestHeartbeatZeroNegotiationPreservesDisabledBehavior(t *testing.T) {
	rwc, srv := newSession(t)
	defer rwc.Close()

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
	config.Dial = func(network, addr string) (net.Conn, error) {
		return fakeNetConn{rwc}, nil
	}

	c, err := DialConfig("amqp://guest:guest@localhost/?heartbeat=0", config)
	if err != nil {
		t.Fatalf("DialConfig failed: %v", err)
	}

	if got := c.heartbeat.negotiated(); got != 0 {
		t.Fatalf("expected heartbeat tracking to be disabled, got interval %v", got)
	}

	// An idle connection with disabled heartbeats must not be shut down.
	time.Sleep(100 * time.Millisecond)
	if c.IsClosed() {
		t.Fatal("heartbeat-disabled connection closed while idle")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("unexpected error during connection close: %v", err)
	}
}

// TestHeartbeaterConcurrentClose verifies that closing the connection from
// multiple goroutines while the heartbeater is ticking is race-free, sends no
// further heartbeats, and reports a consistent close outcome.
func TestHeartbeaterConcurrentClose(t *testing.T) {
	rwc, srv := newSession(t)
	defer rwc.Close()

	go func() {
		srv.expectAMQP()
		srv.connectionStart()
		srv.send(0, &connectionTune{ChannelMax: 11, FrameMax: 20000, Heartbeat: 1})
		srv.recv(0, &srv.tune)
		srv.recv(0, &connectionOpen{})
		srv.send(0, &connectionOpenOk{})
		srv.recv(0, &connectionClose{})
		srv.send(0, &connectionCloseOk{})
	}()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("could not create connection: %v", err)
	}

	const goroutines = 5
	var wg sync.WaitGroup
	errs := make([]error, goroutines)
	start := make(chan struct{})
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			errs[i] = c.Close()
		}(i)
	}
	close(start)
	wg.Wait()

	ok := 0
	for i, err := range errs {
		if err == nil {
			ok++
		} else if err != ErrClosed {
			t.Fatalf("unexpected error from Close %d: %v", i, err)
		}
	}
	if ok != 1 {
		t.Fatalf("expected exactly one successful Close, got %d", ok)
	}

	// Let a heartbeater tick (half the 1s negotiated interval) fire after the
	// close: it must neither send heartbeats nor disturb the closed connection.
	time.Sleep(600 * time.Millisecond)
	if !c.IsClosed() {
		t.Fatal("expected connection to be closed")
	}
}
