// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

// bufferedPipeHalf is one direction of an in-memory connection whose writes
// land in a growing buffer instead of rendezvousing with a concurrent Read.
// This removes the write-blocks-until-read hazard of net.Pipe, so a fully
// scripted broker and the client can each run ahead of the other.
type bufferedPipeHalf struct {
	mu     sync.Mutex
	buf    bytes.Buffer
	cond   *sync.Cond
	closed bool
}

func newBufferedPipeHalf() *bufferedPipeHalf {
	h := &bufferedPipeHalf{}
	h.cond = sync.NewCond(&h.mu)
	return h
}

func (h *bufferedPipeHalf) write(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed {
		return 0, io.ErrClosedPipe
	}
	n, err := h.buf.Write(p)
	h.cond.Broadcast()
	return n, err
}

func (h *bufferedPipeHalf) read(p []byte) (int, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for h.buf.Len() == 0 {
		if h.closed {
			return 0, io.EOF
		}
		h.cond.Wait()
	}
	return h.buf.Read(p)
}

func (h *bufferedPipeHalf) close() {
	h.mu.Lock()
	h.closed = true
	h.cond.Broadcast()
	h.mu.Unlock()
}

type bufferedPipe struct {
	rd *bufferedPipeHalf
	wr *bufferedPipeHalf
}

func newBufferedPipe() (io.ReadWriteCloser, io.ReadWriteCloser) {
	a2b := newBufferedPipeHalf()
	b2a := newBufferedPipeHalf()
	return &bufferedPipe{rd: b2a, wr: a2b}, &bufferedPipe{rd: a2b, wr: b2a}
}

func (p *bufferedPipe) Read(b []byte) (int, error)  { return p.rd.read(b) }
func (p *bufferedPipe) Write(b []byte) (int, error) { return p.wr.write(b) }
func (p *bufferedPipe) Close() error {
	p.rd.close()
	p.wr.close()
	return nil
}

// CloseWrite closes only the write half, signalling EOF to the peer reader
// while leaving the peer's writes readable.
func (p *bufferedPipe) CloseWrite() error {
	p.wr.close()
	return nil
}

// rpcBroker is a scripted AMQP peer for the RPC tests. Unlike the shared
// server helper it has no one-second watchdog, so scheduling jitter under the
// race detector cannot kill it mid-handshake; failures are reported on a
// channel instead of from t.Fatalf in a broker goroutine.
type rpcBroker struct {
	r     reader
	w     writer
	s     io.ReadWriteCloser
	errCh chan error
}

func newRPCBroker(s io.ReadWriteCloser) *rpcBroker {
	return &rpcBroker{
		r:     reader{r: s},
		w:     writer{w: bufio.NewWriter(s)},
		s:     s,
		errCh: make(chan error, 16),
	}
}

func (b *rpcBroker) fail(format string, args ...any) {
	select {
	case b.errCh <- fmt.Errorf(format, args...):
	default:
	}
}

func (b *rpcBroker) err() error {
	select {
	case err := <-b.errCh:
		return err
	default:
		return nil
	}
}

func (b *rpcBroker) send(channel int, m message) {
	if err := b.w.WriteFrame(&methodFrame{ChannelId: uint16(channel), Method: m}); err != nil {
		b.fail("broker send %T: %v", m, err)
	}
}

// writeRaw puts arbitrary bytes on the wire, used to inject a malformed frame.
func (b *rpcBroker) writeRaw(p []byte) {
	if _, err := b.s.Write(p); err != nil {
		b.fail("broker raw write: %v", err)
	}
}

// closeWrite signals EOF to the client reader without tearing down the
// broker's read side, so an in-flight frame already written cannot be lost.
func (b *rpcBroker) closeWrite() {
	if c, ok := b.s.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
		return
	}
	_ = b.s.Close()
}

// recv reads frames until one matches the requested method type, copying it
// into m like the shared server.recv helper.
func (b *rpcBroker) recv(channel int, m message) {
	for {
		frame, err := b.r.ReadFrame()
		if err != nil {
			b.fail("broker read while waiting for %T: %v", m, err)
			return
		}
		if frame.channel() != uint16(channel) {
			b.fail("broker expected frame on channel %d, got %d", channel, frame.channel())
			return
		}
		mf, ok := frame.(*methodFrame)
		if !ok {
			continue
		}
		if reflect.TypeOf(mf.Method) != reflect.TypeOf(m) {
			b.fail("broker expected method %T, got %T", m, mf.Method)
			return
		}
		reflect.ValueOf(m).Elem().Set(reflect.ValueOf(mf.Method).Elem())
		return
	}
}

func (b *rpcBroker) handshake() {
	header := make([]byte, 8)
	if _, err := io.ReadFull(b.s, header); err != nil {
		b.fail("broker read protocol header: %v", err)
		return
	}

	b.send(0, &connectionStart{VersionMajor: 0, VersionMinor: 9, Mechanisms: "PLAIN", Locales: defaultLocale})
	b.recv(0, &connectionStartOk{})
	b.send(0, &connectionTune{ChannelMax: 11, FrameMax: 20000, Heartbeat: 0})
	b.recv(0, &connectionTuneOk{})
	b.recv(0, &connectionOpen{})
	b.send(0, &connectionOpenOk{})
	b.recv(1, &channelOpen{})
	b.send(1, &channelOpenOk{})
}

type rpcSession struct {
	conn   *Connection
	ch     *Channel
	client io.ReadWriteCloser
	broker *rpcBroker
}

// newRPCScriptedSession runs the handshake and the script on a single broker
// goroutine and returns a channel that is open and ready for the test body.
func newRPCScriptedSession(t *testing.T, script func(b *rpcBroker)) *rpcSession {
	t.Helper()

	clientConn, serverConn := newBufferedPipe()
	broker := newRPCBroker(serverConn)
	go func() {
		broker.handshake()
		if broker.err() == nil {
			script(broker)
		}
	}()

	cfg := defaultConfig()
	cfg.Heartbeat = 0
	conn, err := Open(clientConn, cfg)
	if err != nil {
		_ = clientConn.Close()
		t.Fatalf("could not create connection: %v (broker: %v)", err, broker.err())
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = clientConn.Close()
		t.Fatalf("could not open channel: %v (broker: %v)", err, broker.err())
	}

	// Closing the pipe makes the client reader observe EOF and shut the
	// connection down; no connection.close handshake is scripted.
	t.Cleanup(func() { _ = clientConn.Close() })

	return &rpcSession{conn: conn, ch: ch, client: clientConn, broker: broker}
}

// waitForClosed fails unless the channel reports closed within a short delay.
// The channel is taken out of use asynchronously after an RPC desync.
func (s *rpcSession) waitForClosed(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !s.ch.IsClosed() {
		if time.Now().After(deadline) {
			t.Fatal("channel was not taken out of use")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestChannelRPCMatchesOnlyTheCurrentWaiter covers requirement 1: a normal
// round trip through the real frame reader/writer hands the reply to the call
// that registered for it.
func TestChannelRPCMatchesOnlyTheCurrentWaiter(t *testing.T) {
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		b.send(1, &queueDeclareOk{Queue: "rpc-q", MessageCount: 7, ConsumerCount: 3})
	})

	q, err := s.ch.QueueDeclare("rpc-q", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare: %v (broker: %v)", err, s.broker.err())
	}
	if q.Name != "rpc-q" || q.Messages != 7 || q.Consumers != 3 {
		t.Fatalf("reply fields were not copied onto the result: %+v", q)
	}
}

// TestChannelRPCQueuedWaitersAreServedInOrder covers requirement 1 for
// concurrent callers: each reply goes to the oldest pending request, so every
// caller receives the queue name it asked for, never another caller's.
func TestChannelRPCQueuedWaitersAreServedInOrder(t *testing.T) {
	const callers = 4
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		for i := 0; i < callers; i++ {
			var declare queueDeclare
			b.recv(1, &declare)
			b.send(1, &queueDeclareOk{Queue: declare.Queue, MessageCount: uint32(i + 1)})
		}
	})

	names := make([]string, callers)
	errs := make([]error, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for i := 0; i < callers; i++ {
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("q%d", i)
			q, err := s.ch.QueueDeclare(name, false, false, false, false, nil)
			names[i] = q.Name
			errs[i] = err
		}(i)
	}
	wg.Wait()

	for i := 0; i < callers; i++ {
		want := fmt.Sprintf("q%d", i)
		if errs[i] != nil {
			t.Fatalf("call %d (%s): %v", i, want, errs[i])
		}
		if names[i] != want {
			t.Fatalf("call %d received the reply for %q", i, names[i])
		}
	}
}

// TestChannelRPCLateReplyIsDroppedAndCannotReachNextCall covers requirement 3:
// after a timeout the late reply is read completely and dropped, and the
// channel is taken out of use so the stale reply can never answer a later
// call.
func TestChannelRPCLateReplyIsDroppedAndCannotReachNextCall(t *testing.T) {
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		// Reply only after the client has given up, then close the write half
		// so the client reader sees a clean EOF after the late frame instead
		// of parsing it as part of anything else.
		time.Sleep(150 * time.Millisecond)
		b.send(1, &queueDeclareOk{Queue: "late"})
		b.closeWrite()
	})
	s.ch.rpcTimeout = 25 * time.Millisecond

	if _, err := s.ch.QueueDeclare("late", false, false, false, false, nil); !errors.Is(err, ErrRPCTimeout) {
		t.Fatalf("first call error = %v, want ErrRPCTimeout", err)
	}
	s.waitForClosed(t)

	// The late reply must not be queued for a later call. Since the channel is
	// closed, a follow-up call returns ErrClosed without consuming anything.
	if _, err := s.ch.QueueDeclare("late-2", false, false, false, false, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("call after late reply = %v, want ErrClosed", err)
	}
	if err := s.broker.err(); err != nil {
		t.Fatalf("broker saw an unexpected error: %v", err)
	}
}

// TestChannelRPCTimeoutWakesWaiterWithReason covers requirement 2: a timeout
// wakes the blocked caller with ErrRPCTimeout promptly and takes the channel
// out of reuse.
func TestChannelRPCTimeoutWakesWaiterWithReason(t *testing.T) {
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		// Never answer; the client must not wait forever.
		<-time.After(2 * time.Second)
	})
	s.ch.rpcTimeout = 25 * time.Millisecond

	start := time.Now()
	_, err := s.ch.QueueDeclare("stuck", false, false, false, false, nil)
	elapsed := time.Since(start)

	if !errors.Is(err, ErrRPCTimeout) {
		t.Fatalf("error = %v, want ErrRPCTimeout", err)
	}
	if elapsed > time.Second {
		t.Fatalf("timeout took %v, expected roughly %v", elapsed, s.ch.rpcTimeout)
	}
	s.waitForClosed(t)
}

// TestChannelRPCTimeoutDisabledWaits covers requirement 5: a zero timeout
// keeps the historical behavior of waiting for the reply with no deadline.
func TestChannelRPCTimeoutDisabledWaits(t *testing.T) {
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		time.Sleep(100 * time.Millisecond)
		b.send(1, &queueDeclareOk{Queue: "eventual"})
	})
	if s.ch.rpcTimeout != 0 {
		t.Fatalf("default rpcTimeout = %v, want 0", s.ch.rpcTimeout)
	}

	q, err := s.ch.QueueDeclare("eventual", false, false, false, false, nil)
	if err != nil {
		t.Fatalf("QueueDeclare: %v (broker: %v)", err, s.broker.err())
	}
	if q.Name != "eventual" {
		t.Fatalf("reply = %+v, want queue eventual", q)
	}
}

// TestChannelRPCServerChannelCloseWakesWaiterWithReason covers requirement 2: a
// broker-initiated channel.close wakes an in-flight RPC and hands back the
// broker's reply code and reason.
func TestChannelRPCServerChannelCloseWakesWaiterWithReason(t *testing.T) {
	const (
		code   = PreconditionFailed
		reason = "PRECONDITION_FAILED - inequivalent arg"
	)
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		b.send(1, &channelClose{ReplyCode: code, ReplyText: reason})
		b.recv(1, &channelCloseOk{})
	})

	_, err := s.ch.QueueDeclare("precondition", false, false, false, false, nil)
	var amqpErr *Error
	if !errors.As(err, &amqpErr) {
		t.Fatalf("error = %T %v, want *Error", err, err)
	}
	if amqpErr.Code != int(code) || amqpErr.Reason != reason {
		t.Fatalf("error = code %d reason %q, want code %d reason %q",
			amqpErr.Code, amqpErr.Reason, code, reason)
	}
	if !s.ch.IsClosed() {
		t.Fatal("channel should be marked closed after a broker channel.close")
	}
}

// TestChannelRPCConnectionCloseWakesWaiterWithReason covers requirement 2: a
// hard connection error while an RPC is in flight wakes the channel waiter and
// preserves the broker's reason.
func TestChannelRPCConnectionCloseWakesWaiterWithReason(t *testing.T) {
	const (
		code   = ConnectionForced
		reason = "CONNECTION_FORCED - broker admin shutdown"
	)
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		b.send(0, &connectionClose{ReplyCode: code, ReplyText: reason})
		b.recv(0, &connectionCloseOk{})
	})

	_, err := s.ch.QueueDeclare("forced", false, false, false, false, nil)
	var amqpErr *Error
	if !errors.As(err, &amqpErr) {
		t.Fatalf("error = %T %v, want *Error", err, err)
	}
	if amqpErr.Code != int(code) || amqpErr.Reason != reason {
		t.Fatalf("error = code %d reason %q, want code %d reason %q",
			amqpErr.Code, amqpErr.Reason, code, reason)
	}
	if !s.conn.IsClosed() {
		t.Fatal("connection should be closed after a broker connection.close")
	}
}

// TestChannelRPCCorruptFrameTakesConnectionOutOfUse covers requirement 3: when
// the wire carries a malformed frame, reading it fails at the frame boundary,
// the connection fails with a FrameError, and the in-flight RPC is woken
// rather than left wedged.
func TestChannelRPCCorruptFrameTakesConnectionOutOfUse(t *testing.T) {
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		// A heartbeat frame whose declared payload is two bytes short and
		// whose frame-end byte is wrong: the reader must fail cleanly.
		b.writeRaw([]byte{
			frameHeartbeat, 0, 0, // type, channel 0
			0, 0, 0, 4, // declared size
			0, 0, // only two payload bytes
			0, // not frameEnd
		})
	})
	s.ch.rpcTimeout = 0

	_, err := s.ch.QueueDeclare("corrupt", false, false, false, false, nil)
	var amqpErr *Error
	if !errors.As(err, &amqpErr) {
		t.Fatalf("error = %T %v, want *Error", err, err)
	}
	if amqpErr.Code != FrameError {
		t.Fatalf("error code = %d, want FrameError (%d)", amqpErr.Code, FrameError)
	}
	if !s.conn.IsClosed() {
		t.Fatal("connection should be closed after a corrupt frame")
	}
	if !s.ch.IsClosed() {
		t.Fatal("channel should be out of use after a corrupt frame")
	}
}

// TestChannelRPCUnexpectedReplyTypeTakesChannelOutOfUse covers requirement 3:
// a reply that cannot match the expected response type means the streams are
// out of sync, so the channel is taken out of reuse.
func TestChannelRPCUnexpectedReplyTypeTakesChannelOutOfUse(t *testing.T) {
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		// Wrong method type for a queue.declare request.
		b.send(1, &basicQosOk{})
		// The client's courtesy channel.close, then a close-ok.
		b.recv(1, &channelClose{})
		b.send(1, &channelCloseOk{})
	})

	_, err := s.ch.QueueDeclare("wrong", false, false, false, false, nil)
	if !errors.Is(err, ErrCommandInvalid) {
		t.Fatalf("error = %v, want ErrCommandInvalid", err)
	}
	s.waitForClosed(t)
	if _, err := s.ch.QueueDeclare("wrong-2", false, false, false, false, nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("call after desync = %v, want ErrClosed", err)
	}
}

// TestChannelRPCWaiterDoesNotBlockControlFrames covers requirement 4: a
// blocked synchronous RPC never holds the connection writer, so the channel's
// mandatory channel.close-ok response to a broker channel.close still goes out
// while the RPC waits.
func TestChannelRPCWaiterDoesNotBlockControlFrames(t *testing.T) {
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		// Broker-initiated close while the client RPC is still waiting.
		b.send(1, &channelClose{ReplyCode: replySuccess, ReplyText: "control frames must still flow"})
		// The client's mandatory close-ok proves the writer stayed open.
		b.recv(1, &channelCloseOk{})
	})
	s.ch.rpcTimeout = 0

	_, err := s.ch.QueueDeclare("blocking", false, false, false, false, nil)
	if err == nil {
		t.Fatal("blocked RPC returned nil after the channel closed")
	}
	if !s.ch.IsClosed() {
		t.Fatal("channel should be closed after the broker-initiated close")
	}
	if err := s.broker.err(); err != nil {
		t.Fatalf("broker saw an unexpected error: %v", err)
	}
}

// TestChannelRPCWaiterDoesNotBlockPublish covers requirement 4 for business
// frames: while one call waits for a reply the broker never sends, another
// goroutine can still publish on the same channel, so the channel lock is not
// held across the wait.
func TestChannelRPCWaiterDoesNotBlockPublish(t *testing.T) {
	published := make(chan struct{})
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &queueDeclare{})
		// Do not answer the declare. Wait for a publish that must still get
		// through, then let the test finish by closing the write half.
		b.recv(1, &basicPublish{})
		close(published)
		b.closeWrite()
	})
	s.ch.rpcTimeout = 0

	rpcDone := make(chan error, 1)
	go func() {
		_, err := s.ch.QueueDeclare("blocked", false, false, false, false, nil)
		rpcDone <- err
	}()

	// Give the declare a moment to reach its wait, then publish.
	time.Sleep(20 * time.Millisecond)
	if err := s.ch.Publish("", "q", false, false, Publishing{Body: []byte("body")}); err != nil {
		t.Fatalf("Publish while an RPC waits: %v", err)
	}

	select {
	case <-published:
	case <-time.After(2 * time.Second):
		t.Fatal("a blocked RPC prevented an unrelated publish from reaching the broker")
	}

	// Closing the write half ends the blocked RPC with the connection
	// shutdown reason, so the test can finish without leaking the waiter.
	select {
	case err := <-rpcDone:
		if err == nil {
			t.Fatal("blocked RPC returned nil after the connection ended")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("blocked RPC was not woken when the connection ended")
	}
}

// TestChannelRPCTimeoutFromConfig verifies requirement 5: the timeout comes
// from Config.RPCTimeout when the channel is created, and its zero default
// keeps the historical no-deadline behavior.
func TestChannelRPCTimeoutFromConfig(t *testing.T) {
	ch := newChannel(&Connection{Config: Config{RPCTimeout: 3 * time.Second}}, 1)
	if ch.rpcTimeout != 3*time.Second {
		t.Fatalf("rpcTimeout = %v, want 3s from Config.RPCTimeout", ch.rpcTimeout)
	}

	ch = newChannel(&Connection{}, 1)
	if ch.rpcTimeout != 0 {
		t.Fatalf("default rpcTimeout = %v, want 0", ch.rpcTimeout)
	}
}

// TestChannelRPCGracefulCloseHandshake covers requirement 5: the local Close
// path still drives its channel.close/close-ok handshake through the
// correlation machinery on a healthy channel.
func TestChannelRPCGracefulCloseHandshake(t *testing.T) {
	s := newRPCScriptedSession(t, func(b *rpcBroker) {
		b.recv(1, &channelClose{})
		b.send(1, &channelCloseOk{})
	})

	if err := s.ch.Close(); err != nil {
		t.Fatalf("Close: %v (broker: %v)", err, s.broker.err())
	}
	if !s.ch.IsClosed() {
		t.Fatal("channel should be closed after a graceful Close")
	}
}
