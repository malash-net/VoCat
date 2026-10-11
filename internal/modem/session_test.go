package modem

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"
)

type transportStep struct {
	write  string
	chunks []string
	result func() (int, error)
}

type sessionTestResult struct {
	response Response
	err      error
}

type transcriptTransport struct {
	mu          sync.Mutex
	steps       []transportStep
	reads       []atReadBatch
	readReady   chan struct{}
	closed      bool
	unexpected  error
	writeEvents chan string
	drainErrors []error
	ops         []string
	accepted    string
}

func (transport *transcriptTransport) Write(payload []byte) (int, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	defer transport.wakeReaderLocked()
	if transport.closed {
		return 0, io.ErrClosedPipe
	}
	transport.ops = append(transport.ops, "write:"+string(payload))
	if len(transport.steps) == 0 {
		transport.unexpected = fmt.Errorf("unexpected write %q", payload)
		return 0, transport.unexpected
	}
	step := transport.steps[0]
	transport.steps = transport.steps[1:]
	if string(payload) != step.write {
		transport.unexpected = fmt.Errorf("write %q, want %q", payload, step.write)
		return 0, transport.unexpected
	}
	if transport.writeEvents != nil {
		transport.writeEvents <- string(payload)
	}
	count, err := len(payload), error(nil)
	if step.result != nil {
		transport.mu.Unlock()
		count, err = step.result()
		transport.mu.Lock()
	}
	transport.accepted += string(payload[:count])
	for _, chunk := range step.chunks {
		transport.reads = append(transport.reads, atReadBatch{data: []byte(chunk)})
	}
	return count, err
}

func (transport *transcriptTransport) enqueue(chunks ...string) {
	var batches []atReadBatch
	for _, chunk := range chunks {
		batches = append(batches, atReadBatch{data: []byte(chunk)})
	}
	transport.enqueueRead(batches...)
}

func (transport *transcriptTransport) enqueueRead(batches ...atReadBatch) {
	transport.mu.Lock()
	transport.reads = append(transport.reads, batches...)
	transport.wakeReaderLocked()
	transport.mu.Unlock()
}

func (transport *transcriptTransport) wakeReaderLocked() {
	if transport.readReady != nil {
		close(transport.readReady)
		transport.readReady = nil
	}
}

func (transport *transcriptTransport) Read(buffer []byte) (int, error) {
	for {
		transport.mu.Lock()
		if transport.closed {
			transport.mu.Unlock()
			return 0, io.EOF
		}
		if len(transport.reads) > 0 {
			batch := transport.reads[0]
			count := copy(buffer, batch.data)
			if count == len(batch.data) {
				transport.reads = transport.reads[1:]
			} else {
				transport.reads[0].data = batch.data[count:]
				batch.err = nil
			}
			transport.mu.Unlock()
			return count, batch.err
		}
		if transport.readReady == nil {
			transport.readReady = make(chan struct{})
		}
		ready := transport.readReady
		transport.mu.Unlock()
		<-ready
	}
}

func (transport *transcriptTransport) Drain() error {
	transport.mu.Lock()
	transport.ops = append(transport.ops, "drain")
	var err error
	if len(transport.drainErrors) > 0 {
		err = transport.drainErrors[0]
		transport.drainErrors = transport.drainErrors[1:]
	}
	transport.mu.Unlock()
	return err
}

func (transport *transcriptTransport) ResetInputBuffer() error {
	return errors.New("a running session must not discard input")
}

func (transport *transcriptTransport) SetReadTimeout(time.Duration) error {
	return nil
}

func (transport *transcriptTransport) Close() error {
	transport.mu.Lock()
	transport.closed = true
	transport.wakeReaderLocked()
	transport.mu.Unlock()
	return nil
}

func (transport *transcriptTransport) assertDone(t *testing.T) {
	t.Helper()
	transport.mu.Lock()
	unexpected := transport.unexpected
	steps := len(transport.steps)
	transport.mu.Unlock()
	if unexpected != nil || steps != 0 {
		t.Fatalf("unfinished transcript: steps=%d err=%v", steps, unexpected)
	}
}

func (transport *transcriptTransport) assertAccepted(t *testing.T, want string) {
	t.Helper()
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.accepted != want {
		t.Fatalf("accepted writes = %q, want %q; operations = %q", transport.accepted, want, transport.ops)
	}
}

func TestSessionWritesCommand(t *testing.T) {
	final := []string{"\r\n+CSQ: 24,99\r\nOK\r\n"}
	for _, test := range []struct {
		name        string
		steps       []transportStep
		drainErrors []error
		wantOps     string
	}{
		{name: "drain before write", steps: []transportStep{{write: "AT+CSQ\r", chunks: final}},
			wantOps: "drain|write:AT+CSQ\r"},
		{name: "interrupted drain", steps: []transportStep{{write: "AT+CSQ\r", chunks: final}}, drainErrors: []error{syscall.EINTR},
			wantOps: "drain|drain|write:AT+CSQ\r"},
		{name: "partial and interrupted writes", steps: []transportStep{
			{write: "AT+CSQ\r", result: func() (int, error) { return 2, nil }},
			{write: "+CSQ\r", result: func() (int, error) { return 1, syscall.EINTR }},
			{write: "CSQ\r", chunks: final},
		}, wantOps: "drain|write:AT+CSQ\r|write:+CSQ\r|write:CSQ\r"},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &transcriptTransport{steps: test.steps, drainErrors: test.drainErrors}
			session := newTestSession(t, transport)
			response, err := session.Execute(context.Background(), "AT+CSQ")
			if err != nil || !response.OK() || response.Text() != "+CSQ: 24,99" {
				t.Fatalf("Execute = %#v, %v", response, err)
			}
			transport.assertAccepted(t, "AT+CSQ\r")
			transport.mu.Lock()
			defer transport.mu.Unlock()
			if got := strings.Join(transport.ops, "|"); got != test.wantOps {
				t.Fatalf("transport operations = %q, want %q", got, test.wantOps)
			}
		})
	}
}

func TestSessionParsesResponses(t *testing.T) {
	for _, test := range []struct {
		name, command, text, final, urc string
		chunks                          []string
	}{
		{name: "interleaved URC and split line", command: "AT+CSQ", text: "+CSQ: 24,99", final: "OK", urc: `+CMTI: "SM",7`,
			chunks: []string{"\r\nAT+CSQ\r", "\n+CMTI: \"SM\",7\r\n+CS", "Q: 24,99\r\nOK\r", "\n"}},
		{name: "expected registration", command: "AT+CEREG?", text: "+CEREG: 0,5", final: "OK", chunks: []string{"\r\n+CEREG: 0,5\r\nOK\r\n"}},
		{name: "asynchronous CUSD", command: `AT+CUSD=1,"*100#",15`, final: "OK", urc: `+CUSD: 0,"004F004B",72`, chunks: []string{"\r\n+CUSD: 0,\"004F004B\",72\r\nOK\r\n"}},
		{name: "command error", command: "AT+CPIN?", final: "+CME ERROR: 10", chunks: []string{"\r\n+CME ERROR: 10\r\n"}},
		{name: "call final", command: "ATD12345;", final: "NO CARRIER", urc: "NO CARRIER", chunks: []string{"NO CARRIER\r\n"}},
		{name: "ordinary CONNECT line", command: "ATD12345;", text: "CONNECT", final: "OK", chunks: []string{"CONNECT\r\nOK\r\n"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &transcriptTransport{steps: []transportStep{{write: test.command + "\r", chunks: test.chunks}}}
			session := newTestSession(t, transport)
			response, err := session.Execute(context.Background(), test.command)
			if response.Text() != test.text || response.Final != test.final || session.Poisoned() {
				t.Fatalf("Execute = %#v, %v", response, err)
			}
			var commandErr *CommandError
			if test.final == "OK" {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.As(err, &commandErr) || commandErr.Command != test.command || commandErr.Final != test.final {
				t.Fatalf("command error = %v", err)
			}
			if test.urc == "" {
				if len(response.URCs) != 0 {
					t.Fatalf("unexpected URCs: %q", response.URCs)
				}
			} else {
				if len(response.URCs) != 1 || response.URCs[0] != test.urc {
					t.Fatalf("URCs = %q, want %q", response.URCs, test.urc)
				}
				urc, err := session.WaitURC(context.Background(), func(line string) bool { return line == test.urc })
				if err != nil || urc != test.urc {
					t.Fatalf("WaitURC = %q, %v", urc, err)
				}
			}
		})
	}
}

func TestSessionExecutePromptQueuesURCsAndReturnsCMGS(t *testing.T) {
	const pdu = "00010005912143F50008044F60597D"
	transport := &transcriptTransport{steps: []transportStep{
		{
			write: "AT+CMGS=14\r",
			chunks: []string{
				"\r", "\nAT+CMGS=14\r\n+CMTI: \"SM\",7\r", "\n>", " ",
			},
		},
		{write: pdu},
		{
			write: string([]byte{0x1a}),
			chunks: []string{
				"\r\n" + pdu + "\r\n+CMGS: 42\r\n",
				"+CMTI: \"SM\",8\r\nOK\r\n",
			},
		},
	}}
	session := newTestSession(t, transport)
	response, err := session.ExecutePrompt(
		context.Background(),
		"AT+CMGS=14",
		[]byte(pdu),
	)
	if err != nil {
		t.Fatalf("ExecutePrompt: %v", err)
	}
	if response.Text() != "+CMGS: 42" || !response.OK() {
		t.Fatalf("response = %#v", response)
	}
	if len(response.URCs) != 2 {
		t.Fatalf("URCs = %#v", response.URCs)
	}
	for _, wanted := range []string{`+CMTI: "SM",7`, `+CMTI: "SM",8`} {
		line, waitErr := session.WaitURC(
			context.Background(),
			func(line string) bool { return line == wanted },
		)
		if waitErr != nil || line != wanted {
			t.Fatalf("WaitURC(%q) = %q, %v", wanted, line, waitErr)
		}
	}
}

func TestSessionExecutePromptSerializesConcurrentCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events := make(chan string, 4)
		transport := &transcriptTransport{
			writeEvents: events,
			steps: []transportStep{
				{write: "AT+CMGS=\"12345\"\r"},
				{write: "HELLO"},
				{write: "\x1a", chunks: []string{"\r\n+CMGS: 9\r\nOK\r\n"}},
				{write: "AT+CSQ\r", chunks: []string{"\r\n+CSQ: 20,99\r\nOK\r\n"}},
			},
		}
		session := newTestSession(t, transport)
		promptResult := runPrompt(context.Background(), session, `AT+CMGS="12345"`, []byte("HELLO"))
		if first := <-events; first != "AT+CMGS=\"12345\"\r" {
			t.Fatalf("first write = %q", first)
		}
		normalResult := runExecute(context.Background(), session, "AT+CSQ")
		synctest.Wait()
		transport.enqueue("\r\n> ")
		if err := (<-promptResult).err; err != nil {
			t.Fatalf("ExecutePrompt: %v", err)
		}
		if err := (<-normalResult).err; err != nil {
			t.Fatalf("concurrent Execute: %v", err)
		}
		writes := []string{<-events, <-events, <-events}
		want := []string{"HELLO", "\x1a", "AT+CSQ\r"}
		for index := range want {
			if writes[index] != want[index] {
				t.Fatalf("write[%d] = %q, want %q", index, writes[index], want[index])
			}
		}
	})
}

func TestSessionExecutePromptRejectsUnsafeInput(t *testing.T) {
	transport := &transcriptTransport{}
	session := newTestSession(t, transport)
	if _, err := session.Execute(context.Background(), "AT\rAT+CFUN=0"); err == nil {
		t.Fatal("expected command delimiter rejection")
	}
	if _, err := session.ExecutePrompt(
		context.Background(),
		"AT+CSQ",
		[]byte("payload"),
	); err == nil {
		t.Fatal("expected non-CMGS prompt command rejection")
	}
	if _, err := session.ExecutePrompt(
		context.Background(),
		"AT+CMGS=1",
		[]byte{'A', 0x1a},
	); err == nil {
		t.Fatal("expected Ctrl-Z payload rejection")
	}
}

func TestSessionKeepsCallURCsUntilFinal(t *testing.T) {
	for command, text := range map[string]string{"AT+CLCC": `+CLCC: 1,1,4,0,0,"12345",129`, "ATH": ""} {
		t.Run(command, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				transport := &transcriptTransport{steps: []transportStep{{
					write: command + "\r", chunks: []string{"\r\nRING\r\n+CLIP: \"12345\",129\r\nNO CARRIER\r\n"},
				}}}
				session := newTestSession(t, transport)
				transport.enqueue("\r\nCIEV: CALL 1\r\n")
				synctest.Wait()
				result := runExecute(context.Background(), session, command)
				synctest.Wait()
				select {
				case got := <-result:
					t.Fatalf("call URCs completed %s: %#v, %v", command, got.response, got.err)
				default:
				}
				for _, want := range []string{"CIEV: CALL 1", "RING", `+CLIP: "12345",129`, "NO CARRIER"} {
					line, err := session.WaitURC(context.Background(), func(line string) bool { return line == want })
					if err != nil || line != want {
						t.Fatalf("WaitURC(%q) = %q, %v", want, line, err)
					}
				}
				transport.enqueue("\r\n" + text + "\r\nOK\r\n")
				got := <-result
				if got.err != nil || !got.response.OK() || got.response.Text() != text || len(got.response.URCs) != 3 {
					t.Fatalf("%s = %#v, %v", command, got.response, got.err)
				}
			})
		})
	}
}

func TestSessionWaitURCDoesNotBlockCommandsOrQueuedCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events := make(chan string, 2)
		transport := &transcriptTransport{
			writeEvents: events,
			steps: []transportStep{
				{write: "AT\r"},
				{write: "AT+CSQ\r", chunks: []string{"\r\n+CSQ: 20,99\r\nOK\r\n"}},
			},
		}
		session := newTestSession(t, transport)
		type urcResult struct {
			line string
			err  error
		}
		urcs := make(chan urcResult, 1)
		go func() {
			line, err := session.WaitURC(context.Background(), func(line string) bool { return line == `+CUSD: 0,"OK",15` })
			urcs <- urcResult{line, err}
		}()
		synctest.Wait()
		activeResult := runExecute(context.Background(), session, "AT")
		<-events
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		queuedResult := runExecute(ctx, session, "AT+CPIN?")
		synctest.Wait()
		cancel()
		synctest.Wait()
		select {
		case got := <-queuedResult:
			if !errors.Is(got.err, context.Canceled) {
				t.Fatalf("queued Execute() = %v", got.err)
			}
		default:
			t.Fatal("queued cancellation waited for the active command")
		}
		transport.enqueue("\r\n+CUSD: 0,\"OK\",15\r\nOK\r\n")
		if got := <-activeResult; got.err != nil || !got.response.OK() {
			t.Fatalf("AT = %#v, %v", got.response, got.err)
		}
		if got := <-urcs; got.err != nil || got.line != `+CUSD: 0,"OK",15` {
			t.Fatalf("WaitURC = %q, %v", got.line, got.err)
		}
		response, err := session.Execute(context.Background(), "AT+CSQ")
		if err != nil || response.Text() != "+CSQ: 20,99" {
			t.Fatalf("CSQ = %#v, %v", response, err)
		}
	})
}

func TestSessionDoesNotAssignOldBatchTailToQueuedCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events := make(chan string, 2)
		transport := &transcriptTransport{
			writeEvents: events,
			steps:       []transportStep{{write: "AT+CFUN?\r"}, {write: "AT+CPIN?\r"}},
		}
		session := newTestSession(t, transport)
		first := runExecute(context.Background(), session, "AT+CFUN?")
		<-events
		next := runExecute(context.Background(), session, "AT+CPIN?")
		synctest.Wait()
		transport.enqueue("\r\n+CFUN: 1\r\nOK\r\n+CFUN: STA")
		if got := <-first; got.err != nil || !got.response.OK() || got.response.Text() != "+CFUN: 1" {
			t.Fatalf("CFUN = %#v, %v", got.response, got.err)
		}
		synctest.Wait()
		select {
		case write := <-events:
			t.Fatalf("wrote with old partial pending: %q", write)
		default:
		}
		transport.enqueue("LE\r\nOK\r\n")
		if write := <-events; write != "AT+CPIN?\r" {
			t.Fatalf("next write = %q", write)
		}
		transport.enqueue("\r\n+CPIN: READY\r\nOK\r\n")
		if got := <-next; got.err != nil || !got.response.OK() || got.response.Text() != "+CPIN: READY" {
			t.Fatalf("CPIN = %#v, %v", got.response, got.err)
		}
	})
}

func TestSessionIdlePaddingDoesNotBlockNextCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{steps: []transportStep{
			{write: "AT\r", chunks: []string{"OK\r\n\x00 \t"}},
			{write: "AT+CPIN?\r", chunks: []string{"+CPIN: READY\r\nOK\r\n"}},
		}}
		session := newTestSession(t, transport)
		for _, command := range []string{"AT", "AT+CPIN?"} {
			if response, err := session.Execute(context.Background(), command); err != nil || !response.OK() {
				t.Fatalf("%s after padding = %#v, %v", command, response, err)
			}
		}
	})
}

func TestSessionExpiredTransactionKeepsWireOwnership(t *testing.T) {
	for _, test := range []struct {
		name                             string
		prompt, submitted, duringPayload bool
		timeout                          bool
	}{
		{name: "query canceled"},
		{name: "query timeout", timeout: true},
		{name: "late prompt canceled", prompt: true},
		{name: "late prompt timeout", prompt: true, timeout: true},
		{name: "payload canceled", prompt: true, duringPayload: true},
		{name: "submitted SMS canceled", prompt: true, submitted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				command := "AT+CSQ"
				if test.prompt {
					command = "AT+CMGS=5"
				}
				steps := []transportStep{{write: command + "\r"}}
				if test.submitted || test.duringPayload {
					steps[0].chunks = []string{"\r\n> "}
					payload := transportStep{write: "001122"}
					if test.duringPayload {
						payload.result = func() (int, error) { cancel(); return 6, nil }
					}
					steps = append(steps, payload)
				}
				if test.submitted {
					steps = append(steps, transportStep{write: "\x1a"})
				} else if test.prompt {
					steps = append(steps, transportStep{write: "\x1b"})
				}
				steps = append(steps, transportStep{write: "AT+CPIN?\r", chunks: []string{"\r\n+CPIN: READY\r\nOK\r\n"}})
				events := make(chan string, 4)
				transport := &transcriptTransport{steps: steps, writeEvents: events}
				session := newTestSession(t, transport)
				var result <-chan sessionTestResult
				if test.prompt {
					result = runPrompt(ctx, session, command, []byte("001122"))
				} else {
					result = runExecute(ctx, session, command)
				}
				<-events
				if test.submitted || test.duringPayload {
					<-events
					<-events
				}
				wantErr := context.Canceled
				if test.timeout {
					wantErr = ErrCommandTimeout
				} else {
					cancel()
				}
				if got := <-result; !errors.Is(got.err, wantErr) || got.response.Final != "" {
					t.Fatalf("expired transaction = %#v, %v", got.response, got.err)
				}
				queuedCtx, cancelQueued := context.WithTimeout(context.Background(), time.Millisecond)
				_, err := session.Execute(queuedCtx, "AT")
				cancelQueued()
				if !errors.Is(err, ErrCommandTimeout) {
					t.Fatalf("queued command deadline = %v", err)
				}
				next := runExecute(context.Background(), session, "AT+CPIN?")
				synctest.Wait()
				if test.prompt && !test.submitted && !test.duringPayload {
					transport.enqueue("\r\n> ")
					if write := <-events; write != "\x1b" {
						t.Fatalf("late prompt write = %q, want ESC", write)
					}
					synctest.Wait()
				}
				select {
				case write := <-events:
					t.Fatalf("wrote before late final: %q", write)
				default:
				}
				if session.Poisoned() {
					t.Fatal("caller cancellation poisoned the session")
				}
				transport.enqueue("\r\nlate reply\r\nOK\r\n")
				if got := <-next; got.err != nil || !got.response.OK() || got.response.Text() != "+CPIN: READY" {
					t.Fatalf("query after late final = %#v, %v", got.response, got.err)
				}
			})
		})
	}
}

func TestSessionCloseWakesActiveAndWaitingRequests(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		events := make(chan string, 1)
		transport := &transcriptTransport{steps: []transportStep{{write: "AT\r"}}, writeEvents: events}
		session := newTestSession(t, transport)
		active := runExecute(context.Background(), session, "AT")
		if write := <-events; write != "AT\r" {
			t.Fatalf("write = %q", write)
		}
		queued := runExecute(context.Background(), session, "AT+CPIN?")
		urcResult := make(chan error, 1)
		go func() {
			_, err := session.WaitURC(context.Background(), func(string) bool { return true })
			urcResult <- err
		}()
		synctest.Wait()
		if err := session.Close(); err != nil {
			t.Fatalf("Close() = %v", err)
		}
		for _, err := range []error{(<-active).err, (<-queued).err, <-urcResult} {
			if !errors.Is(err, ErrSessionClosed) {
				t.Fatalf("request after Close() = %v", err)
			}
		}
		if !session.Poisoned() {
			t.Fatal("closed session is not poisoned")
		}
	})
}

func TestSessionCanceledCallerCompletesCommittedWrite(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const command = "AT+CSQ\r"
		started, release := make(chan struct{}), make(chan struct{})
		releaseWrite := sync.OnceFunc(func() { close(release) })
		transport := &transcriptTransport{steps: []transportStep{
			{write: command, result: func() (int, error) { return 2, nil }},
			{write: command[2:], result: func() (int, error) {
				close(started)
				<-release
				return len(command) - 2, nil
			}},
			{write: "AT\r", chunks: []string{"\r\nOK\r\n"}},
		}}
		session := newTestSession(t, transport)
		t.Cleanup(releaseWrite)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := runExecute(ctx, session, strings.TrimSuffix(command, "\r"))
		<-started
		cancel()
		synctest.Wait()
		select {
		case got := <-result:
			if !errors.Is(got.err, context.Canceled) || got.response.Final != "" {
				t.Fatalf("canceled command = %#v, %v", got.response, got.err)
			}
		default:
			t.Fatal("caller cancellation waited for committed I/O")
		}
		transport.assertAccepted(t, command[:2])
		releaseWrite()
		synctest.Wait()
		transport.assertAccepted(t, command)
		next := runExecute(context.Background(), session, "AT")
		synctest.Wait()
		transport.assertAccepted(t, command)
		transport.enqueue("\r\n+CSQ: 24,99\r\nOK\r\n")
		if got := <-next; got.err != nil || !got.response.OK() || got.response.Text() != "" {
			t.Fatalf("next command after late final = %#v, %v", got.response, got.err)
		}
	})
}

func TestSessionPayloadWriteFailureDoesNotSendTerminator(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{steps: []transportStep{
			{write: "AT+CMGS=5\r", chunks: []string{"\r\n> "}},
			{write: "HELLO", result: func() (int, error) { return 2, syscall.EIO }},
		}}
		session := newTestSession(t, transport)
		response, err := session.ExecutePrompt(context.Background(), "AT+CMGS=5", []byte("HELLO"))
		var commandErr *CommandError
		if !errors.Is(err, ErrSessionClosed) || !errors.Is(err, syscall.EIO) || errors.As(err, &commandErr) || response.Final != "" {
			t.Fatalf("payload write failure = %#v, %v", response, err)
		}
		synctest.Wait()
		if !session.Poisoned() {
			t.Fatal("transport write failure must allow the manager to reopen the port")
		}
		transport.assertAccepted(t, "AT+CMGS=5\rHE")
		transport.mu.Lock()
		defer transport.mu.Unlock()
		for _, op := range transport.ops {
			if strings.ContainsAny(op, "\x1a\x1b") {
				t.Fatalf("terminator after payload failure: %q", op)
			}
		}
	})
}

func TestSessionFinalPrecedesReadEOF(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const command = "AT+CSQ\r"
		started, release := make(chan struct{}), make(chan struct{})
		releaseWrite := sync.OnceFunc(func() { close(release) })
		transport := &transcriptTransport{steps: []transportStep{{write: command, result: func() (int, error) {
			close(started)
			<-release
			return len(command), nil
		}}}}
		session := newTestSession(t, transport)
		t.Cleanup(releaseWrite)
		result := runExecute(context.Background(), session, "AT+CSQ")
		<-started
		next := runExecute(context.Background(), session, "AT+CPIN?")
		transport.enqueueRead(atReadBatch{data: []byte("\r\n+CSQ: 24,99\r\nOK\r\n"), err: io.EOF})
		synctest.Wait()
		if session.Poisoned() {
			t.Fatal("reader failure allowed reopening during committed write")
		}
		releaseWrite()
		if got := <-result; got.err != nil || !got.response.OK() || got.response.Text() != "+CSQ: 24,99" {
			t.Fatalf("final preceding EOF = %#v, %v", got.response, got.err)
		}
		if got := <-next; !errors.Is(got.err, io.EOF) || !errors.Is(got.err, ErrSessionClosed) || got.response.Final != "" {
			t.Fatalf("queued command after EOF = %#v, %v", got.response, got.err)
		}
		synctest.Wait()
		if !session.Poisoned() {
			t.Fatal("physical EOF did not poison the session")
		}
		if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, io.EOF) || !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("new command after EOF = %v", err)
		}
	})
}

func runExecute(ctx context.Context, session *Session, command string) <-chan sessionTestResult {
	result := make(chan sessionTestResult, 1)
	go func() {
		response, err := session.Execute(ctx, command)
		result <- sessionTestResult{response, err}
	}()
	return result
}

func runPrompt(ctx context.Context, session *Session, command string, payload []byte) <-chan sessionTestResult {
	result := make(chan sessionTestResult, 1)
	go func() {
		response, err := session.ExecutePrompt(ctx, command, payload)
		result <- sessionTestResult{response, err}
	}()
	return result
}

func newTestSession(t *testing.T, transport *transcriptTransport) *Session {
	t.Helper()
	session, err := NewSession(transport, SessionOptions{
		ReadTimeout:    time.Millisecond,
		CommandTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { transport.assertDone(t) })
	t.Cleanup(func() { _ = session.Close() })
	return session
}
