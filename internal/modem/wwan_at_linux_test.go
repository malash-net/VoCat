//go:build linux

package modem

import (
	"bytes"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestNativeWWANATTransportReadPeerEOF(t *testing.T) {
	readFD, writeFD := socketpair(t)
	transport := &nativeWWANATTransport{fd: readFD, readTimeout: 20 * time.Millisecond}
	defer transport.Close()
	t.Cleanup(func() {
		if writeFD >= 0 {
			_ = unix.Close(writeFD)
		}
	})

	buffer := make([]byte, 64)
	if count, err := transport.Read(buffer); count != 0 || err != nil {
		t.Fatalf("Read timeout = %d, %v; want 0, nil", count, err)
	}
	payload := []byte("\r\nRING\r\n")
	if count, err := unix.Write(writeFD, payload); err != nil || count != len(payload) {
		t.Fatalf("seed pending bytes = %d, %v", count, err)
	}
	if err := unix.Close(writeFD); err != nil {
		t.Fatalf("close peer: %v", err)
	}
	writeFD = -1
	if count, err := transport.Read(buffer); err != nil || !bytes.Equal(buffer[:count], payload) {
		t.Fatalf("Read buffered data = %q, %v; want %q", buffer[:count], err, payload)
	}
	if count, err := transport.Read(nil); count != 0 || err != nil {
		t.Fatalf("zero-length Read = %d, %v; want 0, nil", count, err)
	}
	if count, err := transport.Read(buffer); count != 0 || !errors.Is(err, io.EOF) {
		t.Fatalf("Read closed peer = %d, %v; want 0, EOF", count, err)
	}
}

func TestNativeWWANATTransportDrainPreservesPendingBytes(t *testing.T) {
	readFD, writeFD := socketpair(t)
	transport := &nativeWWANATTransport{fd: readFD, readTimeout: -1}
	defer transport.Close()
	defer unix.Close(writeFD)

	payload := []byte("\r\nRING\r\n+CLIP: \"123456789\",129\r\n")
	if count, err := unix.Write(writeFD, payload); err != nil || count != len(payload) {
		t.Fatalf("seed pending bytes = %d, %v", count, err)
	}
	output := []byte("AT\r")
	if count, err := transport.Write(output); err != nil || count != len(output) {
		t.Fatalf("queue output = %d, %v", count, err)
	}
	done := make(chan error, 1)
	go func() { done <- transport.Drain() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Drain while output pending = %v; want nil", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Drain waited for WWAN output")
	}
	buffer := make([]byte, len(payload))
	count, err := unix.Read(readFD, buffer)
	if err != nil || count != len(payload) || !bytes.Equal(buffer, payload) {
		t.Fatalf("read after Drain = %q, %d, %v; want %q", buffer, count, err, payload)
	}
	if count, err := unix.Read(writeFD, buffer); err != nil || !bytes.Equal(buffer[:max(count, 0)], output) {
		t.Fatalf("read output = %q, %v; want %q", buffer[:max(count, 0)], err, output)
	}
	if err := transport.Drain(); err != nil {
		t.Fatalf("second Drain: %v", err)
	}
}

func TestNativeWWANATTransportRejectsClosedTransport(t *testing.T) {
	readFD, writeFD := socketpair(t)
	defer unix.Close(writeFD)
	transport := &nativeWWANATTransport{fd: readFD, readTimeout: -1}
	if err := transport.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := transport.Drain(); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Drain after Close = %v, want ErrClosedPipe", err)
	}
	if _, err := transport.Read(make([]byte, 1)); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Read after Close = %v, want ErrClosedPipe", err)
	}
	if _, err := transport.Write([]byte("AT\r")); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("Write after Close = %v, want ErrClosedPipe", err)
	}
	if err := transport.SetReadTimeout(time.Second); !errors.Is(err, io.ErrClosedPipe) {
		t.Fatalf("SetReadTimeout after Close = %v, want ErrClosedPipe", err)
	}
	if err := transport.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestNativeWWANATTransportCloseInterruptsIO(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			readFD, writeFD := socketpair(t)
			transport := &nativeWWANATTransport{fd: readFD, readTimeout: -1}
			defer transport.Close()
			defer unix.Close(writeFD)
			if operation == "write" {
				fillTransportOutput(t, readFD)
			}
			ioDone := make(chan error, 1)
			go func() {
				var err error
				if operation == "read" {
					_, err = transport.Read(make([]byte, 1))
				} else {
					_, err = transport.Write([]byte("AT\r"))
				}
				ioDone <- err
			}()
			waitForTransportReadLock(t, &transport.mu)
			closeDone := make(chan error, 1)
			go func() { closeDone <- transport.Close() }()
			select {
			case err := <-closeDone:
				if err != nil {
					t.Fatalf("Close: %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("Close did not interrupt pending I/O")
			}
			select {
			case err := <-ioDone:
				if !errors.Is(err, io.ErrClosedPipe) {
					t.Fatalf("%s after Close = %v, want ErrClosedPipe", operation, err)
				}
			case <-time.After(time.Second):
				t.Fatal("I/O did not return after Close")
			}
		})
	}
}

func socketpair(t *testing.T) (int, int) {
	t.Helper()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_STREAM|unix.SOCK_NONBLOCK|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	return fds[0], fds[1]
}

func fillTransportOutput(t *testing.T, fd int) {
	t.Helper()
	buffer := make([]byte, 4096)
	for {
		_, err := unix.Write(fd, buffer)
		if errors.Is(err, unix.EAGAIN) {
			return
		}
		if err != nil {
			t.Fatalf("fill output buffer: %v", err)
		}
	}
}

func waitForTransportReadLock(t *testing.T, mu *sync.RWMutex) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for mu.TryLock() {
		mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("I/O did not acquire transport lock")
		}
		time.Sleep(time.Millisecond)
	}
}
