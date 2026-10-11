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
	ResetInputBuffer() error
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

// Session serializes all access to one physical AT port. A timed-out command
// keeps ownership of its response until a later operation consumes its final.
type Session struct {
	mu        sync.Mutex
	transport Transport
	options   SessionOptions
	readBuf   []byte
	urcs      []string
	pending   *atTransaction
	closed    bool
	poisoned  bool
}

type atTransaction struct {
	command        string
	responsePrefix string
	payloadEcho    string
	awaitPrompt    bool
}

// PoisonedClient reports transport failures that require reopening the port.
// Caller cancellation alone does not poison a session or release its pending reply.
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
	return &Session{
		transport: transport,
		options:   options,
	}, nil
}

// Poisoned reports whether this session has hit a transport-fatal error and
// should be discarded rather than reused. It is safe to call concurrently.
func (session *Session) Poisoned() bool {
	session.mu.Lock()
	defer session.mu.Unlock()
	return session.poisoned || session.closed
}

func (session *Session) Execute(ctx context.Context, command string) (Response, error) {
	command, err := normalizeATCommand(command)
	if err != nil {
		return Response{}, err
	}
	ctx, cancel := session.commandContext(ctx)
	defer cancel()

	session.mu.Lock()
	defer session.mu.Unlock()
	return session.executeLocked(ctx, command, nil, false)
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
	ctx, cancel := session.commandContext(ctx)
	defer cancel()

	session.mu.Lock()
	defer session.mu.Unlock()
	return session.executeLocked(ctx, command, payload, true)
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

func (session *Session) executeLocked(ctx context.Context, command string, payload []byte, interactive bool) (response Response, err error) {
	response.Command = command
	if session.closed || session.poisoned {
		return response, ErrSessionClosed
	}
	if err := ctx.Err(); err != nil {
		return response, commandContextError(err)
	}
	// Never assign an old reply (including a partial trailing line) to a new
	// command. No buffer flush or port reopen can isolate bytes still in flight.
	for session.pending != nil || len(session.readBuf) > 0 {
		if session.pending == nil {
			session.readBuf = bytes.TrimLeft(session.readBuf, "\r\n\x00 \t")
			if len(session.readBuf) == 0 {
				break
			}
		}
		if err := session.readNextLocked(ctx, nil, nil); err != nil {
			return response, commandContextError(err)
		}
	}
	if err := drainTransport(ctx, session.transport); err != nil {
		session.poisoned = true
		return response, fmt.Errorf("drain %s: %w", command, err)
	}
	if err := ctx.Err(); err != nil {
		return response, commandContextError(err)
	}
	started := time.Now()
	defer func() { response.Duration = time.Since(started) }()
	transaction := &atTransaction{
		command: command, responsePrefix: expectedResponsePrefix(command),
		payloadEcho: string(payload), awaitPrompt: interactive,
	}
	session.pending = transaction
	// Once writing starts, finish the command even if the caller cancels;
	// otherwise the next write could be appended to half an AT command.
	if err := writeAll(session.transport, []byte(command+"\r")); err != nil {
		session.poisoned = true
		return response, fmt.Errorf("write %s: %w", command, err)
	}
	if interactive {
		if err := drainTransport(context.Background(), session.transport); err != nil {
			session.poisoned = true
			return response, fmt.Errorf("drain %s: %w", command, err)
		}
	}
	for session.pending != nil {
		if err := session.readNextLocked(ctx, &response, payload); err != nil {
			return response, commandContextError(err)
		}
	}
	if !response.OK() {
		return response, &CommandError{
			Command: command,
			Final:   response.Final,
			Lines:   append([]string(nil), response.Lines...),
		}
	}
	if transaction.awaitPrompt {
		return response, fmt.Errorf("%w: %s", ErrPromptNotReceived, command)
	}
	return response, nil
}

// A nil response means the caller of the pending transaction has already left.
// Its remaining lines are discarded, but URCs and the final still get processed.
func (session *Session) readNextLocked(ctx context.Context, response *Response, payload []byte) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if session.pending != nil && session.pending.awaitPrompt && len(session.readBuf) > 0 && session.readBuf[0] == '>' {
			session.readBuf = session.readBuf[1:]
			return session.sendPayloadLocked(ctx, response != nil, payload)
		}
		if line, ok := popLine(&session.readBuf); ok {
			session.consumeLineLocked(line, response)
			return nil
		}
		buffer := make([]byte, 1024)
		count, err := session.transport.Read(buffer)
		if count > 0 {
			// Keep bytes even if the deadline expired during Read. The next
			// operation must still be able to finish this transaction.
			session.readBuf = append(session.readBuf, buffer[:count]...)
			continue
		}
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			session.poisoned = true
			return fmt.Errorf("read serial response: %w", err)
		}
	}
}

func (session *Session) sendPayloadLocked(ctx context.Context, active bool, payload []byte) error {
	terminator := byte(0x1b) // ESC aborts a prompt whose caller has left.
	if active && ctx.Err() == nil {
		if err := writeAll(session.transport, payload); err != nil {
			session.poisoned = true
			return fmt.Errorf("write SMS payload: %w", err)
		}
		if ctx.Err() == nil {
			terminator = 0x1a
		}
	}
	if err := writeAll(session.transport, []byte{terminator}); err != nil {
		session.poisoned = true
		return fmt.Errorf("terminate SMS payload: %w", err)
	}
	session.pending.awaitPrompt = false
	if err := drainTransport(context.Background(), session.transport); err != nil {
		session.poisoned = true
		return fmt.Errorf("drain SMS payload: %w", err)
	}
	return nil
}

func (session *Session) consumeLineLocked(line string, response *Response) {
	line = strings.TrimSpace(strings.Trim(line, "\x00"))
	if line == "" {
		return
	}
	pending := session.pending
	if pending == nil {
		if !isFinalResult(line) {
			session.enqueueURCLocked(line)
		}
		return
	}
	if strings.EqualFold(line, pending.command) ||
		(!pending.awaitPrompt && pending.payloadEcho != "" && line == pending.payloadEcho) {
		return
	}
	callResult := isCallResult(line)
	if callResult || (isURC(line) && !strings.HasPrefix(strings.ToUpper(line), pending.responsePrefix)) {
		session.enqueueURCLocked(line)
		if response != nil {
			response.URCs = append(response.URCs, line)
		}
		if !callResult || !isCallCommand(pending.command) {
			return
		}
	}
	if isFinalResult(line) || callResult {
		session.pending = nil
		if response != nil {
			response.Final = line
		}
	} else if response != nil {
		response.Lines = append(response.Lines, line)
	}
}

// WaitURC waits for an unsolicited result matching predicate. Non-matching URCs
// remain queued for another consumer. Like Execute, it reads under session.mu.
func (session *Session) WaitURC(ctx context.Context, predicate func(string) bool) (string, error) {
	if predicate == nil {
		return "", errors.New("modem: URC predicate is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed || session.poisoned {
		return "", ErrSessionClosed
	}
	for {
		for index, line := range session.urcs {
			if predicate(line) {
				session.urcs = append(session.urcs[:index], session.urcs[index+1:]...)
				return line, nil
			}
		}
		if err := session.readNextLocked(ctx, nil, nil); err != nil {
			return "", err
		}
	}
}

func (session *Session) enqueueURCLocked(line string) {
	if len(session.urcs) >= session.options.MaxURCs {
		copy(session.urcs, session.urcs[1:])
		session.urcs[len(session.urcs)-1] = line
		return
	}
	session.urcs = append(session.urcs, line)
}

// drainTransport retries tcdrain/TCSBRK when interrupted by a signal.
func drainTransport(ctx context.Context, transport Transport) error {
	for {
		err := transport.Drain()
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}

func popLine(buffer *[]byte) (string, bool) {
	data := *buffer
	for index, character := range data {
		if character != '\r' && character != '\n' {
			continue
		}
		line := string(data[:index])
		next := index + 1
		for next < len(data) && (data[next] == '\r' || data[next] == '\n') {
			next++
		}
		*buffer = data[next:]
		return line, true
	}
	return "", false
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
		// +CUSD is asynchronous even when it arrives before the command's OK.
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

func isCallCommand(command string) bool {
	command = strings.ToUpper(strings.TrimSpace(command))
	return command == "ATA" || command == "ATA0" || strings.HasPrefix(command, "ATD")
}

func isCallResult(line string) bool {
	switch strings.ToUpper(strings.TrimSpace(line)) {
	case "NO CARRIER", "BUSY", "NO ANSWER":
		return true
	default:
		return false
	}
}

func isFinalResult(line string) bool {
	upper := strings.ToUpper(strings.TrimSpace(line))
	return upper == "OK" ||
		upper == "ERROR" ||
		strings.HasPrefix(upper, "+CME ERROR:") ||
		strings.HasPrefix(upper, "+CMS ERROR:")
}

func isURC(line string) bool {
	upper := strings.ToUpper(strings.TrimSpace(line))
	if upper == "RING" ||
		upper == "RDY" ||
		upper == "CALL READY" ||
		upper == "SMS READY" ||
		upper == "PB DONE" {
		return true
	}
	for _, prefix := range []string{
		"+CMTI:",
		"+CMT:",
		"+CDS:",
		"+CREG:",
		"+CGREG:",
		"+CEREG:",
		"+CUSD:",
		"+CLIP:",
		"+CRING:",
		"CIEV:",
		"+CIEV:",
		"+QIND:",
		"+QIURC:",
		"+QSIMSTAT:",
		"+QUSIM:",
		"+QNWINFO:",
	} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

func writeAll(writer io.Writer, payload []byte) error {
	for len(payload) > 0 {
		count, err := writer.Write(payload)
		if count < 0 || count > len(payload) {
			return io.ErrShortWrite
		}
		payload = payload[count:]
		if errors.Is(err, syscall.EINTR) {
			continue
		}
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

func (session *Session) Close() error {
	session.mu.Lock()
	defer session.mu.Unlock()
	if session.closed {
		return nil
	}
	session.closed = true
	return session.transport.Close()
}
