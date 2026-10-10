package modem

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Transport interface {
	io.ReadWriteCloser
	Drain() error
	SetReadTimeout(time.Duration) error
}

type SessionOptions struct {
	ReadTimeout    time.Duration
	CommandTimeout time.Duration
	MaxURCs        int
}

func (options SessionOptions) withDefaults() SessionOptions {
	if options.ReadTimeout <= 0 {
		options.ReadTimeout = 100 * time.Millisecond
	}
	if options.CommandTimeout <= 0 {
		options.CommandTimeout = 3 * time.Second
	}
	if options.MaxURCs <= 0 {
		options.MaxURCs = 256
	}
	return options
}

type Session struct {
	lifetime  context.Context
	cancel    context.CancelFunc
	transport Transport
	options   SessionOptions
	requests  chan *atRequest
	received  chan atReadBatch
	stop      chan struct{}
	stopOnce  sync.Once
	closeOnce sync.Once
	workers   sync.WaitGroup

	mu         sync.Mutex
	closed     bool
	failure    error
	closeErr   error
	urcs       []string
	urcChanged chan struct{}
}

type atReadBatch struct {
	data []byte
	err  error
}

type PoisonedClient interface {
	Poisoned() bool
}

func NewSession(transport Transport, options SessionOptions) (*Session, error) {
	if transport == nil {
		return nil, errors.New("modem: transport is required")
	}
	options = options.withDefaults()
	if err := transport.SetReadTimeout(options.ReadTimeout); err != nil {
		return nil, fmt.Errorf("set serial read timeout: %w", err)
	}
	lifetime, cancel := context.WithCancel(context.Background())
	session := &Session{
		lifetime:   lifetime,
		cancel:     cancel,
		transport:  transport,
		options:    options,
		requests:   make(chan *atRequest, 16),
		received:   make(chan atReadBatch, 8),
		stop:       make(chan struct{}),
		urcChanged: make(chan struct{}),
	}
	session.workers.Add(2)
	go session.readLoop()
	go session.runLoop()
	return session, nil
}

func (session *Session) Poisoned() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.stopped() && !errors.Is(session.failure, ErrSessionUnsynchronized)
}

func (session *Session) Execute(ctx context.Context, command string) (Response, error) {
	command, err := normalizeATCommand(command)
	if err != nil {
		return Response{}, err
	}
	return session.submit(ctx, command, nil, false)
}

func (session *Session) ExecutePrompt(ctx context.Context, command string, payload []byte) (Response, error) {
	command, err := normalizeATCommand(command)
	if err != nil {
		return Response{}, err
	}
	if !strings.HasPrefix(strings.ToUpper(command), "AT+CMGS=") {
		return Response{}, errors.New("modem: prompt command must be AT+CMGS")
	}
	if len(payload) > 8192 {
		return Response{}, errors.New("modem: prompt payload exceeds 8192 bytes")
	}
	if bytes.IndexByte(payload, 0x1a) >= 0 || bytes.IndexByte(payload, 0x1b) >= 0 {
		return Response{}, errors.New("modem: prompt payload contains a terminator")
	}
	return session.submit(ctx, command, append([]byte(nil), payload...), true)
}

func (session *Session) submit(ctx context.Context, command string, payload []byte, interactive bool) (Response, error) {
	ctx, cancel := session.commandContext(ctx)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return failedResponse(command, commandContextError(err))
	}
	if err := session.Err(); err != nil {
		return failedResponse(command, err)
	}
	request := newATRequest(ctx, command, payload, interactive)
	select {
	case session.requests <- request:
	case <-ctx.Done():
		return failedResponse(command, commandContextError(ctx.Err()))
	case <-session.stop:
		return failedResponse(command, session.Err())
	}
	select {
	case <-request.done:
		result := request.resultSnapshot(nil)
		return result.response, result.err
	case <-ctx.Done():
		result := request.canceledResult(commandContextError(ctx.Err()))
		return result.response, result.err
	case <-session.stop:
		result := request.resultSnapshot(session.Err())
		return result.response, result.err
	}
}

func (session *Session) commandContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, session.options.CommandTimeout)
}

func commandContextError(err error) error {
	if errors.Is(err, context.DeadlineExceeded) {
		return errors.Join(ErrCommandTimeout, err)
	}
	return err
}

func failedResponse(command string, err error) (Response, error) {
	return Response{Command: command}, err
}

func (session *Session) readLoop() {
	defer session.workers.Done()
	buffer := make([]byte, 1024)
	for {
		select {
		case <-session.stop:
			return
		default:
		}
		count, err := session.transport.Read(buffer)
		if count == 0 && errors.Is(err, syscall.EINTR) {
			continue
		}
		if count == 0 && err == nil {
			continue
		}
		batch := atReadBatch{err: err}
		if count > 0 {
			batch.data = append([]byte(nil), buffer[:count]...)
		}
		if err != nil {
			session.mu.Lock()
			if !session.closed && session.failure == nil {
				session.failure = fmt.Errorf("read serial response: %w", err)
			}
			session.mu.Unlock()
		}
		select {
		case session.received <- batch:
		case <-session.stop:
			return
		}
		if err != nil {
			return
		}
	}
}

func (session *Session) enqueueURC(lines []string) {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return
	}
	text := strings.Join(lines, "\n")
	if len(session.urcs) >= session.options.MaxURCs {
		copy(session.urcs, session.urcs[1:])
		session.urcs[len(session.urcs)-1] = text
	} else {
		session.urcs = append(session.urcs, text)
	}
	close(session.urcChanged)
	session.urcChanged = make(chan struct{})
}

// WaitURC blocks until a stored URC matches predicate, returns that text,
// and removes the URC from the buffer. The predicate runs while the session
// mutex is held, so it must not call back into the session.
func (session *Session) WaitURC(ctx context.Context, predicate func(string) bool) (string, error) {
	if predicate == nil {
		return "", errors.New("modem: URC predicate is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		session.mu.Lock()
		if session.closed {
			err := session.unavailableErrorLocked()
			session.mu.Unlock()
			return "", err
		}
		for index, text := range session.urcs {
			if predicate(text) {
				session.urcs = append(session.urcs[:index], session.urcs[index+1:]...)
				session.mu.Unlock()
				return text, nil
			}
		}
		failure := session.unavailableErrorLocked()
		notify := session.urcChanged
		session.mu.Unlock()
		if failure != nil {
			return "", failure
		}
		select {
		case <-notify:
		case <-ctx.Done():
			return "", ctx.Err()
		case <-session.stop:
			return "", session.Err()
		}
	}
}

func (session *Session) Err() error {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.unavailableErrorLocked()
}

func (session *Session) unavailableErrorLocked() error {
	if session.failure != nil {
		if errors.Is(session.failure, ErrSessionUnsynchronized) && !session.closed {
			return session.failure
		}
		return errors.Join(ErrSessionClosed, session.failure)
	}
	if session.closed {
		return ErrSessionClosed
	}
	return nil
}

func (session *Session) beginRequest(request *atRequest) error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if err := session.unavailableErrorLocked(); err != nil {
		return err
	}
	request.mu.Lock()
	defer request.mu.Unlock()
	if err := request.ctx.Err(); err != nil {
		return commandContextError(err)
	}
	if request.completed {
		return ErrSessionClosed
	}
	request.started = time.Now()
	return nil
}

func (session *Session) failProtocol(reason string, cause error) {
	session.mu.Lock()
	if !session.closed && !errors.Is(session.failure, ErrSessionUnsynchronized) {
		session.failure = errors.Join(fmt.Errorf("%w: %s", ErrSessionUnsynchronized, reason), cause)
	}
	failure := session.failure
	session.mu.Unlock()
	if failure != nil {
		session.terminate(failure, false)
	}
}

func (session *Session) stopped() bool {
	select {
	case <-session.stop:
		return true
	default:
		return false
	}
}

func (session *Session) terminate(err error, explicit bool) {
	session.mu.Lock()
	if explicit {
		session.closed = true
	}
	if err != nil && session.failure == nil {
		session.failure = err
	}
	// A closed stop channel wakes every pending WaitURC and submit; the
	// unavailableError check inside them returns before they can select on
	// urcChanged again, so nothing needs resetting here.
	session.stopOnce.Do(func() {
		session.cancel()
		close(session.stop)
	})
	session.mu.Unlock()
	session.closeOnce.Do(func() {
		closeErr := session.transport.Close()
		session.mu.Lock()
		session.closeErr = closeErr
		session.mu.Unlock()
	})
}

func (session *Session) Close() error {
	session.terminate(nil, true)
	session.workers.Wait()
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.closeErr
}

func drainTransport(ctx context.Context, transport Transport) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := transport.Drain()
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err == nil {
			err = ctx.Err()
		}
		return err
	}
}

func normalizeATCommand(command string) (string, error) {
	command = strings.TrimSpace(command)
	if command == "" {
		return "", errors.New("modem: AT command is empty")
	}
	if len(command) > 512 {
		return "", errors.New("modem: AT command exceeds 512 bytes")
	}
	if strings.ContainsAny(command, "\r\n\x00") {
		return "", errors.New("modem: AT command contains a control delimiter")
	}
	if !strings.HasPrefix(strings.ToUpper(command), "AT") {
		return "", errors.New("modem: command must start with AT")
	}
	return command, nil
}

func expectedResponsePrefix(command string) string {
	upper := strings.ToUpper(strings.TrimSpace(command))
	if strings.HasPrefix(upper, "AT+CUSD=") {
		return "\x00"
	}
	body := strings.TrimPrefix(upper, "AT")
	if body == "" || body == "I" {
		return "\x00"
	}
	end := len(body)
	for index, character := range body {
		if character == '?' || character == '=' || character == ',' {
			end = index
			break
		}
	}
	name := body[:end]
	if name == "" {
		return "\x00"
	}
	return name + ":"
}

var urcPrefixes = [...]string{
	"+CMTI:", "+CMT:", "+CDS:", "+CREG:", "+CGREG:", "+CEREG:",
	"+CUSD:", "+CLIP:", "+CRING:", "CIEV:", "+CIEV:", "+QIND:", "+QIURC:",
	"+QSIMSTAT:", "+QUSIM:", "+QNWINFO:",
}

func normalizeATLine(line string) string {
	return strings.ToUpper(strings.TrimSpace(line))
}

func isFinalResult(line string) bool {
	upper := normalizeATLine(line)
	return upper == "OK" || upper == "ERROR" ||
		strings.HasPrefix(upper, "+CME ERROR:") || strings.HasPrefix(upper, "+CMS ERROR:")
}

func isCallResult(line string) bool {
	switch normalizeATLine(line) {
	case "NO CARRIER", "BUSY", "NO ANSWER":
		return true
	default:
		return false
	}
}

func isURC(line string) bool {
	upper := normalizeATLine(line)
	if upper == "RING" || upper == "RDY" || upper == "CALL READY" ||
		upper == "SMS READY" || upper == "PB DONE" {
		return true
	}
	for _, prefix := range urcPrefixes {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

func writeAll(ctx context.Context, transport Transport, payload []byte) (int, error) {
	accepted := 0
	for len(payload) > 0 {
		if err := ctx.Err(); err != nil {
			return accepted, err
		}
		count, err := transport.Write(payload)
		if count < 0 || count > len(payload) {
			return accepted, io.ErrShortWrite
		}
		accepted += count
		payload = payload[count:]
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return accepted, err
		}
		if count == 0 {
			return accepted, io.ErrShortWrite
		}
	}
	return accepted, ctx.Err()
}
