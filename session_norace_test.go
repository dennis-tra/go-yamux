//go:build !race

package yamux

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

func TestSession_PingOfDeath(t *testing.T) {
	conf := testConfNoKeepAlive()
	// This test is slow and can easily time out on writes on CI.
	//
	// In the future, we might want to prioritize ping-replies over even
	// other control messages, but that seems like overkill for now.
	conf.ConnectionWriteTimeout = 1 * time.Second
	client, server := testClientServerConfig(conf)
	defer client.Close()
	defer server.Close()

	count := 10000

	var wg sync.WaitGroup
	begin := make(chan struct{})
	for i := 0; i < count; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-begin
			if _, err := server.Ping(); err != nil {
				t.Error(err)
			}
		}()
		go func() {
			defer wg.Done()
			<-begin
			if _, err := client.Ping(); err != nil {
				t.Error(err)
			}
		}()
	}
	close(begin)
	wg.Wait()
}

func TestSendData_VeryLarge(t *testing.T) {
	client, server := testClientServer()
	defer client.Close()
	defer server.Close()

	var n int64 = 1 * 1024 * 1024 * 1024
	var workers int = 16

	wg := &sync.WaitGroup{}
	wg.Add(workers * 2)

	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			stream, err := server.AcceptStream()
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			defer stream.Close()

			buf := make([]byte, 4)
			_, err = io.ReadFull(stream, buf)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if !bytes.Equal(buf, []byte{0, 1, 2, 3}) {
				t.Errorf("bad header")
				return
			}

			recv, err := io.Copy(io.Discard, stream)
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if recv != n {
				t.Errorf("bad: %v", recv)
				return
			}
		}()
	}
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			stream, err := client.Open(context.Background())
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			defer stream.Close()

			_, err = stream.Write([]byte{0, 1, 2, 3})
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}

			unlimited := &UnlimitedReader{}
			sent, err := io.Copy(stream, io.LimitReader(unlimited, n))
			if err != nil {
				t.Errorf("err: %v", err)
				return
			}
			if sent != n {
				t.Errorf("bad: %v", sent)
				return
			}
		}()
	}

	doneCh := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneCh)
	}()
	select {
	case <-doneCh:
	case <-time.After(20 * time.Second):
		server.Close()
		client.Close()
		wg.Wait()
		t.Fatal("timeout")
	}
}

func TestLargeWindow(t *testing.T) {
	conf := DefaultConfig()
	conf.MaxStreamWindowSize *= 2

	client, server := testClientServerConfig(conf)
	defer client.Close()
	defer server.Close()

	stream, err := client.Open(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream.Close()

	stream2, err := server.Accept()
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	defer stream2.Close()

	err = stream.SetWriteDeadline(time.Now().Add(10 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, initialStreamWindow)
	n, err := stream.Write(buf)
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if n != len(buf) {
		t.Fatalf("short write: %d", n)
	}
}

func TestCloseRace(t *testing.T) {
	// initialize client
	clientConn, remoteConn := testConn()

	// Block all writes to simulate a slow connection
	clientConn.(*pipeConn).BlockWrites()

	conf := testConf()
	client, err := Client(clientConn, conf, nil)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	// fill the send buffer with messages that the client
	// wants to send until the buffer is full
	for j := 0; j < 64; j++ {
		client.sendCh <- []byte("some bytes")
	}

	// construct another random stream that both parties use
	streamID := uint32(100)
	_ = client.incomingStream(streamID)

	// for synchronization purposes in this test we take the streamLock lock
	// here. This allows us to semi-reliably wait until the message that we
	// are about to send to the client gets handled in handleStreamMessage.
	client.streamLock.Lock()

	// construct dummy message to the client
	hdr := encode(typeData, flagACK, streamID, 200)
	remoteConn.Write(hdr[:])

	// wait until we reach the streamLock in handleStreamMessage
	time.Sleep(10 * time.Millisecond)

	// close the connection so that stream.readData will fail
	clientConn.Close()

	// release the lock to continue execution of the handleStreamMessage method
	client.streamLock.Unlock()

	// unblock writes in the sendLoop. The connection is closed now so the
	// write should fail which will then lets the sendLoop method to return.
	clientConn.(*pipeConn).UnblockWrites()

	// after sendLoop has returned the "send" method will wait until recvDoneCh
	// is closed. However, this will never happen because handleStreamMessage
	// is waiting to send a goAway message on the sendCh which isn't read anymore
	time.Sleep(10 * time.Millisecond)

	// calling close here then deadlocks.
	closeDone := make(chan struct{})
	go func() {
		client.Close()
		close(closeDone)
	}()

	select {
	case <-closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout")
	}
}
