// Copyright (c) 2026 Broadcom, Inc. All Rights Reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package amqp091

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"go.uber.org/goleak"
)

// sessionQueue serves pre-built in-memory sessions to Config.Dial, one per
// reconnect attempt, in order.
type sessionQueue struct {
	mu      sync.Mutex
	clients []io.ReadWriteCloser
}

func (q *sessionQueue) add(client io.ReadWriteCloser) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.clients = append(q.clients, client)
}

func (q *sessionQueue) dial(network, addr string) (net.Conn, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.clients) == 0 {
		return nil, errors.New("sessionQueue: no session left for dial")
	}
	client := q.clients[0]
	q.clients = q.clients[1:]
	return fakeNetConn{client}, nil
}

// openTestConnection opens a connection over an in-memory pipe, serving the
// AMQP handshake from a goroutine, and arms it for manual recovery: the URL
// Reconnect parses, a fast reconnection config, and the session queue Reconnect
// dials from. No connection watcher is registered, so the test drives
// Reconnect itself.
func openTestConnection(t *testing.T, sessions *sessionQueue) *Connection {
	t.Helper()

	rwc, srv := newSession(t)
	t.Cleanup(func() { _ = rwc.Close() })
	go srv.connectionOpen()

	c, err := Open(rwc, defaultConfig())
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c.url = "amqp://guest:guest@localhost/"
	c.Config.Recovery = &Recovery{
		ReconnectionConfig: &ReconnectionConfig{MaxRetryCount: 3, RetryInterval: time.Millisecond},
	}
	c.Config.Dial = sessions.dial
	return c
}

func waitForCondition(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func currentGen(c *Connection) uint64 {
	return c.res.Load().gen
}

// TestReconnectAssignsNewGenerationPerEstablishedConnection verifies that
// every established connection — the initial Open and each successful
// reconnect — is assigned its own monotonically increasing recovery
// generation.
func TestReconnectAssignsNewGenerationPerEstablishedConnection(t *testing.T) {
	sessions := &sessionQueue{}
	c := openTestConnection(t, sessions)

	if gen := currentGen(c); gen != 1 {
		t.Fatalf("initial connection generation = %d, want 1", gen)
	}

	for want := uint64(2); want <= 3; want++ {
		rwc, srv := newSession(t)
		t.Cleanup(func() { _ = rwc.Close() })
		sessions.add(rwc)
		if want == 3 {
			// The last generation also serves the final Close() handshake.
			go func() {
				srv.connectionOpen()
				srv.connectionClose()
			}()
		} else {
			go srv.connectionOpen()
		}

		// Drop the current generation and recover onto the next one.
		prev := c.res.Load()
		prev.conn.Close()
		waitForCondition(t, 2*time.Second, "connection to be marked closed", c.IsClosed)

		if err := c.Reconnect(); err != nil {
			t.Fatalf("Reconnect to generation %d failed: %v", want, err)
		}
		if gen := currentGen(c); gen != want {
			t.Fatalf("generation after reconnect = %d, want %d", gen, want)
		}
		if c.IsClosed() {
			t.Fatalf("connection must be open on generation %d", want)
		}
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestStaleGenerationCannotAffectPublishedConnection verifies that frames,
// RPCs, and teardowns belonging to a superseded generation resolve to that
// generation's own terminal state and never disturb the published one.
func TestStaleGenerationCannotAffectPublishedConnection(t *testing.T) {
	c := &Connection{channels: make(map[uint16]*Channel)}

	var staleBuf, currentBuf bytes.Buffer
	stale := newConnResources(dummyReadWriteCloser{}, 1)
	stale.writer = &writer{bufio.NewWriter(&staleBuf)}
	current := newConnResources(dummyReadWriteCloser{}, 2)
	current.writer = &writer{bufio.NewWriter(&currentBuf)}
	c.res.Store(current)

	// A channel.close for an unknown channel makes dispatchClosed answer
	// channel.close-ok on the frame's own generation, which is observable in
	// that generation's writer.
	close7 := &methodFrame{ChannelId: 7, Method: &channelClose{ReplyCode: NotFound, ReplyText: "gone"}}

	c.demux(stale, close7)
	if staleBuf.Len() != 0 {
		t.Fatal("stale generation frame was dispatched and answered")
	}

	c.demux(current, close7)
	if currentBuf.Len() == 0 {
		t.Fatal("current generation frame was not dispatched")
	}

	// A teardown from the stale generation must leave the published
	// generation fully intact.
	staleErr := &Error{Code: FrameError, Reason: "stale read error"}
	c.shutdownGen(staleErr, stale)

	if c.IsClosed() {
		t.Fatal("stale generation shutdown closed the published connection")
	}
	select {
	case <-current.close:
		t.Fatal("stale generation shutdown tore down the current generation")
	default:
	}
	select {
	case <-stale.close:
	default:
		t.Fatal("stale generation was not torn down")
	}

	// An RPC pinned to the stale generation resolves with that generation's
	// terminal error, not with anything from the current one.
	if err := c.callOn(stale, nil, &connectionOpenOk{}); err != staleErr {
		t.Fatalf("stale generation RPC = %v, want the generation's terminal error %v", err, staleErr)
	}
}

// TestReconnectCandidateNotPublishedBeforeCommit verifies that while a
// recovery candidate is being handshaken, callers keep observing the old
// closed generation: IsClosed stays true, no new generation is published, and
// new channels are rejected. Only once the handshake completes is the
// candidate committed as the current generation.
func TestReconnectCandidateNotPublishedBeforeCommit(t *testing.T) {
	sessions := &sessionQueue{}
	c := openTestConnection(t, sessions)

	rwc, srv := newSession(t)
	t.Cleanup(func() { _ = rwc.Close() })
	sessions.add(rwc)

	handshakeStarted := make(chan struct{})
	proceedWithHandshake := make(chan struct{})
	go func() {
		srv.expectAMQP()
		close(handshakeStarted)
		<-proceedWithHandshake
		srv.connectionStart()
		srv.connectionTune()
		srv.recv(0, &connectionOpen{})
		srv.send(0, &connectionOpenOk{})
		srv.connectionClose()
	}()

	// Drop generation 1 and start recovery.
	c.res.Load().conn.Close()
	waitForCondition(t, 2*time.Second, "connection to be marked closed", c.IsClosed)

	reconnectDone := make(chan error, 1)
	go func() { reconnectDone <- c.Reconnect() }()

	select {
	case <-handshakeStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Reconnect() never reached the AMQP handshake")
	}

	// The candidate is mid-handshake: nothing of it may be observable.
	if !c.IsClosed() {
		t.Fatal("IsClosed() = false while the candidate is still handshaking")
	}
	if gen := currentGen(c); gen != 1 {
		t.Fatalf("candidate published before commit: current generation = %d, want 1", gen)
	}
	if _, err := c.Channel(); err != ErrClosed {
		t.Fatalf("Channel() during candidate handshake = %v, want ErrClosed", err)
	}
	if state := c.lifeCycle.State(); state != StateReconnecting {
		t.Fatalf("lifecycle state = %v, want StateReconnecting", state)
	}

	close(proceedWithHandshake)

	select {
	case err := <-reconnectDone:
		if err != nil {
			t.Fatalf("Reconnect() failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Reconnect() did not complete after the handshake was allowed to proceed")
	}

	if gen := currentGen(c); gen != 2 {
		t.Fatalf("generation after commit = %d, want 2", gen)
	}
	if c.IsClosed() {
		t.Fatal("connection must be open after the candidate committed")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestReconnectFailedCandidateReleasesResources verifies that a candidate
// whose handshake fails is cancelled without being published: the old
// generation stays current, the candidate's socket is closed (the server
// observes EOF), and its reader/writer goroutines are gone.
func TestReconnectFailedCandidateReleasesResources(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	sessions := &sessionQueue{}
	c := openTestConnection(t, sessions)
	c.Config.Recovery.ReconnectionConfig.MaxRetryCount = 1

	rwc, srv := newSession(t)
	t.Cleanup(func() { _ = rwc.Close() })
	sessions.add(rwc)

	candidateReleased := make(chan error, 1)
	go func() {
		srv.expectAMQP()
		srv.connectionStart()
		// Reject the handshake; the client must then cancel the candidate.
		srv.send(0, &connectionClose{ReplyCode: AccessRefused, ReplyText: "ACCESS_REFUSED - Login was refused"})
		srv.recv(0, &connectionCloseOk{})
		_, err := srv.S.Read(make([]byte, 1))
		candidateReleased <- err
	}()

	// Drop generation 1 and recover.
	c.res.Load().conn.Close()
	waitForCondition(t, 2*time.Second, "connection to be marked closed", c.IsClosed)

	err := c.Reconnect()
	var amqpErr *Error
	if !errors.As(err, &amqpErr) || amqpErr.Code != AccessRefused {
		t.Fatalf("Reconnect() = %v, want ACCESS_REFUSED", err)
	}

	if gen := currentGen(c); gen != 1 {
		t.Fatalf("failed candidate was published: current generation = %d, want 1", gen)
	}
	if !c.IsClosed() {
		t.Fatal("connection must remain closed after the candidate failed")
	}

	select {
	case err := <-candidateReleased:
		if err != io.EOF {
			t.Fatalf("expected EOF on the server side after candidate cancellation, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("failed candidate did not release its socket")
	}
}

// failOnceTopologyRecovery fails its first RecoverTopology call without any
// broker interaction — leaving the committed generation's socket and channels
// fully alive — and delegates to the default implementation afterwards. It
// simulates a recovery attempt that fails after channels were already
// re-opened, the case rollbackGeneration must unwind.
type failOnceTopologyRecovery struct {
	calls  int
	failed chan struct{}
}

func (f *failOnceTopologyRecovery) RecoverTopology(conn *Connection, channels []*Channel) ([]TopologyRecoveryEntity, error) {
	f.calls++
	if f.calls == 1 {
		close(f.failed)
		return nil, errors.New("topology recovery aborted on first attempt")
	}
	return (&DefaultTopologyRecovery{}).RecoverTopology(conn, channels)
}

// TestReconnectRollsBackCommittedGenerationOnChannelFailure verifies the
// rollback half of the state transition: when a committed generation's
// topology recovery fails while its socket and channels are still alive, the
// whole generation is torn back down — the socket is released (the server
// observes EOF) and the already-recovered channel is shut down — so the next
// attempt re-opens the channel on a fresh generation instead of skipping it
// as already-open and leaving it bound to the dead one.
func TestReconnectRollsBackCommittedGenerationOnChannelFailure(t *testing.T) {
	sessions := &sessionQueue{}
	c := openTestConnection(t, sessions)
	topology := &failOnceTopologyRecovery{failed: make(chan struct{})}
	c.Config.Recovery.TopologyRecovery = topology

	rwc2, srv2 := newSession(t)
	t.Cleanup(func() { _ = rwc2.Close() })
	sessions.add(rwc2)
	rwc3, srv3 := newSession(t)
	t.Cleanup(func() { _ = rwc3.Close() })
	sessions.add(rwc3)

	// Attempt 1: the handshake and the channel re-open succeed, then topology
	// recovery fails. The connection stays up until the client rolls the
	// generation back, which the server observes as EOF.
	attempt1Released := make(chan error, 1)
	go func() {
		srv2.connectionOpen()
		srv2.channelOpen(1)
		<-topology.failed
		_, err := srv2.S.Read(make([]byte, 1))
		attempt1Released <- err
	}()

	// Attempt 2: full recovery succeeds. The channel must be re-opened on
	// this generation even though attempt 1 already opened it once.
	go func() {
		srv3.connectionOpen()
		srv3.channelOpen(1)
		srv3.recv(1, &queueDeclare{})
		srv3.send(1, &queueDeclareOk{Queue: "q"})
		srv3.connectionClose()
	}()

	// Initial topology: one channel with one declared queue.
	rwc1, srv1 := newSession(t)
	t.Cleanup(func() { _ = rwc1.Close() })
	go func() {
		srv1.connectionOpen()
		srv1.channelOpen(1)
		srv1.recv(1, &queueDeclare{})
		srv1.send(1, &queueDeclareOk{Queue: "q"})
	}()

	c2, err := Open(rwc1, c.Config)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	c2.url = c.url

	ch, err := c2.Channel()
	if err != nil {
		t.Fatalf("Channel failed: %v", err)
	}
	if _, err := ch.QueueDeclare("q", false, false, false, false, nil); err != nil {
		t.Fatalf("QueueDeclare failed: %v", err)
	}

	// Drop generation 1 and recover.
	c2.res.Load().conn.Close()
	waitForCondition(t, 2*time.Second, "connection to be marked closed", c2.IsClosed)

	reconnectDone := make(chan error, 1)
	go func() { reconnectDone <- c2.Reconnect() }()

	select {
	case err := <-attempt1Released:
		if err != io.EOF {
			t.Fatalf("expected EOF on the server side after rollback, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("rolled-back generation did not release its socket")
	}

	select {
	case err := <-reconnectDone:
		if err != nil {
			t.Fatalf("Reconnect() failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Reconnect() did not complete")
	}

	// Open=1, rolled-back attempt=2, successful attempt=3.
	if gen := currentGen(c2); gen != 3 {
		t.Fatalf("current generation = %d, want 3", gen)
	}
	if ch.IsClosed() {
		t.Fatal("channel must be open after the successful attempt")
	}
	if state := c2.lifeCycle.State(); state != StateOpen {
		t.Fatalf("lifecycle state = %v, want StateOpen", state)
	}

	if err := c2.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// TestConcurrentReconnectAndClosePublishSingleConnection hammers Reconnect()
// and Close() concurrently: no interleaving may ever publish a candidate —
// the connection has exactly one current generation before, during, and after
// the race — and no goroutines may leak.
func TestConcurrentReconnectAndClosePublishSingleConnection(t *testing.T) {
	defer goleak.VerifyNone(t, goleak.IgnoreCurrent())

	for i := 0; i < 10; i++ {
		sessions := &sessionQueue{}
		c := openTestConnection(t, sessions)
		c.Config.Recovery.ReconnectionConfig.MaxRetryCount = 2

		// Every dial "succeeds" onto a socket whose peer is already gone, so
		// each candidate is built and then cancelled during the handshake.
		c.Config.Dial = func(network, addr string) (net.Conn, error) {
			pr, pw := io.Pipe()
			_ = pw.Close()
			return fakeNetConn{pipe{pr, pw}}, nil
		}

		// Drop generation 1.
		c.res.Load().conn.Close()
		waitForCondition(t, 2*time.Second, "connection to be marked closed", c.IsClosed)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); _ = c.Reconnect() }()
		go func() { defer wg.Done(); _ = c.Close() }()
		wg.Wait()

		if !c.IsClosed() {
			t.Fatalf("iteration %d: connection must be closed after concurrent Reconnect/Close against a dead broker", i)
		}
		if gen := currentGen(c); gen != 1 {
			t.Fatalf("iteration %d: a candidate was published during the race: current generation = %d, want 1", i, gen)
		}
	}
}
