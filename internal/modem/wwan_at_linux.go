//go:build linux

package modem

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"
)

// nativeWWANATTransport adapts a Linux WWAN AT character device to Session's
// serial-like transport contract. WWAN ports are not TTYs, so termios ioctls
// used by ordinary serial libraries fail even though raw AT read/write works.
type nativeWWANATTransport struct {
	mu          sync.RWMutex
	fd          int
	readTimeout time.Duration
	closed      atomic.Bool
}

func openNativeWWANATTransport(path string) (Transport, error) {
	fd, err := unix.Open(path, unix.O_RDWR|unix.O_NONBLOCK|unix.O_NOCTTY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	return &nativeWWANATTransport{fd: fd, readTimeout: -1}, nil
}

func (transport *nativeWWANATTransport) Read(buffer []byte) (int, error) {
	transport.mu.RLock()
	defer transport.mu.RUnlock()
	if transport.closed.Load() {
		return 0, io.ErrClosedPipe
	}

	deadline := time.Time{}
	if transport.readTimeout >= 0 {
		deadline = time.Now().Add(transport.readTimeout)
	}
	for {
		if transport.closed.Load() {
			return 0, io.ErrClosedPipe
		}
		timeout := 100
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if remaining <= 0 {
				return 0, nil
			}
			timeout = int((min(remaining, 100*time.Millisecond) + time.Millisecond - 1) / time.Millisecond)
		}
		fds := []unix.PollFd{{Fd: int32(transport.fd), Events: unix.POLLIN}}
		ready, err := unix.Poll(fds, timeout)
		if transport.closed.Load() {
			return 0, io.ErrClosedPipe
		}
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil {
			return 0, err
		}
		if ready == 0 {
			continue
		}
		if fds[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 &&
			fds[0].Revents&unix.POLLIN == 0 {
			return 0, io.EOF
		}
		count, err := unix.Read(transport.fd, buffer)
		if errors.Is(err, unix.EINTR) || errors.Is(err, unix.EAGAIN) {
			continue
		}
		if count < 0 {
			count = 0
		}
		if len(buffer) > 0 && count == 0 && err == nil && fds[0].Revents&(unix.POLLHUP|unix.POLLERR) != 0 {
			return 0, io.EOF
		}
		return count, err
	}
}

func (transport *nativeWWANATTransport) Write(buffer []byte) (int, error) {
	if transport.closed.Load() {
		return 0, io.ErrClosedPipe
	}
	transport.mu.RLock()
	defer transport.mu.RUnlock()
	return nativeWrite(transport.fd, buffer, &transport.closed, io.ErrClosedPipe)
}

func (transport *nativeWWANATTransport) Drain() error {
	if transport.closed.Load() {
		return io.ErrClosedPipe
	}
	return nil
}

func (transport *nativeWWANATTransport) SetReadTimeout(timeout time.Duration) error {
	if timeout < -1 {
		return fmt.Errorf("invalid read timeout %s", timeout)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.closed.Load() {
		return io.ErrClosedPipe
	}
	transport.readTimeout = timeout
	return nil
}

func (transport *nativeWWANATTransport) Close() error {
	transport.closed.Store(true)
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.fd < 0 {
		return nil
	}
	err := unix.Close(transport.fd)
	transport.fd = -1
	return err
}
