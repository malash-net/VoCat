//go:build linux

package modem

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func linuxTestSerialPath(t *testing.T) (string, int) {
	t.Helper()
	master, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY|unix.O_NONBLOCK|unix.O_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(master) })
	if err := unix.IoctlSetPointerInt(master, unix.TIOCSPTLCK, 0); err != nil {
		t.Fatal(err)
	}
	number, err := unix.IoctlGetInt(master, unix.TIOCGPTN)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("/dev/pts/%d", number), master
}

// A long-lived exec child models qmi-proxy without accessing real modems.
func TestLinuxSerialExecHelper(t *testing.T) {
	if os.Getenv("VOCAT_TEST_SERIAL_EXEC_HELPER") != "1" {
		return
	}
	fmt.Fprintln(os.Stdout, "ready")
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}

func TestLinuxSerialCloseInterruptsWrite(t *testing.T) {
	fd, peerFD := socketpair(t)
	transport := &linuxSerialTransport{}
	transport.fd.Store(int64(fd))
	defer transport.Close()
	defer unix.Close(peerFD)
	fillTransportOutput(t, fd)
	writeDone := make(chan error, 1)
	go func() {
		_, err := transport.Write([]byte("AT\r"))
		writeDone <- err
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
		t.Fatal("Close did not interrupt pending Write")
	}
	select {
	case err := <-writeDone:
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("Write after Close = %v, want ErrClosed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Write did not return after Close")
	}
}

func TestLinuxSerialDrainWaitsForOutputOrClose(t *testing.T) {
	for _, action := range []string{"read", "close"} {
		t.Run(action, func(t *testing.T) {
			fd, peerFD := socketpair(t)
			transport := &linuxSerialTransport{}
			transport.fd.Store(int64(fd))
			defer transport.Close()
			defer unix.Close(peerFD)
			payload := []byte("pending output")
			if count, err := transport.Write(payload); err != nil || count != len(payload) {
				t.Fatalf("queue output = %d, %v", count, err)
			}
			if pending, err := unix.IoctlGetInt(fd, unix.TIOCOUTQ); err != nil || pending == 0 {
				t.Fatalf("pending output = %d, %v; want nonzero", pending, err)
			}
			drainDone := make(chan error, 1)
			go func() { drainDone <- transport.Drain() }()
			deadline := time.Now().Add(time.Second)
			for transport.mu.TryLock() {
				transport.mu.Unlock()
				select {
				case err := <-drainDone:
					t.Fatalf("Drain returned with output pending: %v", err)
				default:
				}
				if time.Now().After(deadline) {
					t.Fatal("Drain did not acquire transport lock")
				}
				runtime.Gosched()
			}
			var wantErr error
			if action == "close" {
				wantErr = os.ErrClosed
				closeDone := make(chan error, 1)
				go func() { closeDone <- transport.Close() }()
				select {
				case err := <-closeDone:
					if err != nil {
						t.Fatalf("Close: %v", err)
					}
				case <-time.After(time.Second):
					t.Fatal("Close did not interrupt pending Drain")
				}
			} else {
				buffer := make([]byte, len(payload))
				if count, err := unix.Read(peerFD, buffer); err != nil || count != len(payload) || !bytes.Equal(buffer, payload) {
					t.Fatalf("read queued output = %q, %d, %v; want %q", buffer, count, err, payload)
				}
			}
			select {
			case err := <-drainDone:
				if !errors.Is(err, wantErr) {
					t.Fatalf("Drain after %s = %v, want %v", action, err, wantErr)
				}
			case <-time.After(time.Second):
				t.Fatalf("Drain did not return after %s", action)
			}
		})
	}
}

func TestLinuxNativeWritePreservesAcceptedCount(t *testing.T) {
	for _, kind := range []string{"serial", "wwan"} {
		t.Run(kind, func(t *testing.T) {
			fd, peerFD := socketpair(t)
			defer unix.Close(peerFD)
			var transport Transport
			if kind == "wwan" {
				transport = &nativeWWANATTransport{fd: fd, readTimeout: -1}
			} else {
				serial := &linuxSerialTransport{}
				serial.fd.Store(int64(fd))
				transport = serial
			}
			defer transport.Close()
			if err := unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_SNDBUF, 4096); err != nil {
				t.Fatal(err)
			}
			payload := bytes.Repeat([]byte("AT\r"), 1<<18)
			count, err := transport.Write(payload)
			if count <= 0 || count >= len(payload) || err != nil {
				t.Fatalf("partial Write = %d, %v; want 0 < n < %d, nil", count, err, len(payload))
			}
			received := make([]byte, len(payload))
			readCount, err := unix.Read(peerFD, received)
			if err != nil || readCount != count || !bytes.Equal(received[:readCount], payload[:count]) {
				t.Fatalf("accepted bytes = %d, %v; want %d matching bytes", readCount, err, count)
			}
			if extra, err := unix.Read(peerFD, received); extra > 0 || !errors.Is(err, unix.EAGAIN) {
				t.Fatalf("unexpected repeated bytes = %d, %v", extra, err)
			}
		})
	}
}

func TestLinuxSerialDrainPreservesInputAndOutput(t *testing.T) {
	path, master := linuxTestSerialPath(t)
	transport, err := openSerialTransport(path, 115200)
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	input := []byte("\r\nRING\r\n")
	if count, err := unix.Write(master, input); err != nil || count != len(input) {
		t.Fatalf("queue input = %d, %v", count, err)
	}
	output := []byte("AT\r")
	if count, err := transport.Write(output); err != nil || count != len(output) {
		t.Fatalf("Write = %d, %v", count, err)
	}
	if err := transport.Drain(); err != nil {
		t.Fatalf("Drain: %v", err)
	}
	fds := []unix.PollFd{{Fd: int32(master), Events: unix.POLLIN}}
	if ready, err := unix.Poll(fds, 1000); err != nil || ready != 1 {
		t.Fatalf("wait for output = %d, %v", ready, err)
	}
	buffer := make([]byte, 64)
	if count, err := unix.Read(master, buffer); err != nil || !bytes.Equal(buffer[:max(count, 0)], output) {
		t.Fatalf("read output = %q, %v; want %q", buffer[:max(count, 0)], err, output)
	}
	if count, err := transport.Read(buffer); err != nil || !bytes.Equal(buffer[:max(count, 0)], input) {
		t.Fatalf("read input = %q, %v; want %q", buffer[:max(count, 0)], err, input)
	}
}

func TestLinuxSerialCloseReleasesTTYWhileExecChildRemainsAlive(t *testing.T) {
	for cycle := 0; cycle < 5; cycle++ {
		t.Run(fmt.Sprintf("reset-%d", cycle), func(t *testing.T) {
			path, master := linuxTestSerialPath(t)
			transport, err := openSerialTransport(path, 115200)
			if err != nil {
				t.Fatal(err)
			}
			defer transport.Close()
			fd, err := transport.(*linuxSerialTransport).currentFD()
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestLinuxSerialExecHelper$")
			child.Env = append(os.Environ(), "VOCAT_TEST_SERIAL_EXEC_HELPER=1")
			stdin, err := child.StdinPipe()
			if err != nil {
				t.Fatal(err)
			}
			stdout, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			child.Stderr = os.Stderr
			if err := child.Start(); err != nil {
				t.Fatal(err)
			}
			defer func() {
				_ = stdin.Close()
				if err := child.Wait(); err != nil {
					t.Errorf("exec child: %v", err)
				}
			}()
			if ready, err := bufio.NewReader(stdout).ReadString('\n'); err != nil || ready != "ready\n" {
				t.Fatalf("exec child readiness = %q, %v", ready, err)
			}
			inheritedPath, _ := os.Readlink(fmt.Sprintf("/proc/%d/fd/%d", child.Process.Pid, fd))
			if inheritedPath == path {
				t.Errorf("exec child inherited serial descriptor %d for %s", fd, path)
			}
			if err := transport.Close(); err != nil {
				t.Fatal(err)
			}
			// EIO means the last slave descriptor has closed; an inherited
			// descriptor keeps it alive. The PTY master retains TIOCEXCL itself,
			// so reopening this synthetic slave cannot test USB tty recovery.
			if _, err := unix.Read(master, make([]byte, 1)); !errors.Is(err, unix.EIO) {
				t.Fatalf("serial tty remains open after transport close: master read = %v, want EIO", err)
			}
		})
	}
}
