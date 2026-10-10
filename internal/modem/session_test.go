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
	mu           sync.Mutex
	steps        []transportStep
	reads        []atReadBatch
	readReady    chan struct{}
	closed       bool
	unexpected   error
	writeEvents  chan string
	drainErrors  []error
	drainCount   int
	drainGate    chan struct{}
	drainStarted chan struct{}
	ops          []string
	accepted     string
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
	transport.drainCount++
	var err error
	if len(transport.drainErrors) > 0 {
		err = transport.drainErrors[0]
		transport.drainErrors = transport.drainErrors[1:]
	}
	if transport.drainStarted != nil {
		close(transport.drainStarted)
		transport.drainStarted = nil
	}
	gate := transport.drainGate
	transport.drainGate = nil
	transport.mu.Unlock()
	if gate != nil {
		<-gate
	}
	return err
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

func TestSessionRetriesInterruptedDrain(t *testing.T) {
	transport := &transcriptTransport{
		steps: []transportStep{{
			write:  "AT+CSQ\r",
			chunks: []string{"\r\nAT+CSQ\r\n+CSQ: 24,99\r\nOK\r\n"},
		}},
		drainErrors: []error{syscall.EINTR},
	}
	session := newTestSession(t, transport)

	response, err := session.Execute(context.Background(), "AT+CSQ")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if response.Final != "OK" {
		t.Fatalf("response final = %q", response.Final)
	}
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if transport.drainCount != 2 {
		t.Fatalf("Drain() calls = %d, want 2", transport.drainCount)
	}
}

func TestSessionSeparatesInterleavedURCs(t *testing.T) {
	transport := &transcriptTransport{steps: []transportStep{{
		write: "AT+CSQ\r",
		chunks: []string{
			"\r\nAT+CSQ\r\n+CMTI: \"SM\",7\r\n",
			"+CSQ: 24,99\r\nOK\r\n",
		},
	}}}
	session := newTestSession(t, transport)
	response, err := session.Execute(context.Background(), "AT+CSQ")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := response.Text(); got != "+CSQ: 24,99" {
		t.Fatalf("response = %q", got)
	}
	if len(response.URCs) != 1 || response.URCs[0] != `+CMTI: "SM",7` {
		t.Fatalf("URCs = %#v", response.URCs)
	}
	urc, err := session.WaitURC(context.Background(), func(line string) bool {
		return line == `+CMTI: "SM",7`
	})
	if err != nil || urc == "" {
		t.Fatalf("WaitURC = %q, %v", urc, err)
	}
}

func TestSessionKeepsExpectedRegistrationLineInResponse(t *testing.T) {
	transport := &transcriptTransport{steps: []transportStep{{
		write:  "AT+CEREG?\r",
		chunks: []string{"\r\n+CEREG: 0,5\r\nOK\r\n"},
	}}}
	session := newTestSession(t, transport)
	response, err := session.Execute(context.Background(), "AT+CEREG?")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if response.Text() != "+CEREG: 0,5" || len(response.URCs) != 0 {
		t.Fatalf("response = %#v", response)
	}
}

func TestSessionQueuesCUSDThatArrivesBeforeOK(t *testing.T) {
	transport := &transcriptTransport{steps: []transportStep{{
		write:  "AT+CUSD=1,\"*100#\",15\r",
		chunks: []string{"\r\n+CUSD: 0,\"004F004B\",72\r\nOK\r\n"},
	}}}
	session := newTestSession(t, transport)
	response, err := session.Execute(context.Background(), `AT+CUSD=1,"*100#",15`)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(response.URCs) != 1 {
		t.Fatalf("URCs = %#v", response.URCs)
	}
	urc, err := session.WaitURC(context.Background(), func(line string) bool {
		return len(line) >= 6 && line[:6] == "+CUSD:"
	})
	if err != nil || urc != `+CUSD: 0,"004F004B",72` {
		t.Fatalf("WaitURC = %q, %v", urc, err)
	}
}

func TestSessionReturnsTypedCommandError(t *testing.T) {
	transport := &transcriptTransport{steps: []transportStep{{
		write:  "AT+CPIN?\r",
		chunks: []string{"\r\n+CME ERROR: 10\r\n"},
	}}}
	session := newTestSession(t, transport)
	_, err := session.Execute(context.Background(), "AT+CPIN?")
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Final != "+CME ERROR: 10" {
		t.Fatalf("error = %#v", err)
	}
}

func TestSessionTimeoutRetainsInputAndRejectsCommandInjection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{steps: []transportStep{{write: "AT\r"}}}
		session := newTestSession(t, transport)
		if _, err := session.Execute(context.Background(), "AT\rAT+CFUN=0"); err == nil {
			t.Fatal("expected command delimiter rejection")
		}
		if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, ErrCommandTimeout) {
			t.Fatalf("Execute() error = %v", err)
		}
		if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, ErrSessionUnsynchronized) {
			t.Fatalf("Execute() after timeout = %v", err)
		}
		if session.Poisoned() {
			t.Fatal("timeout poisoned a readable transport")
		}
	})
}

func TestSessionHandlesPartialWrites(t *testing.T) {
	transport := &transcriptTransport{steps: []transportStep{
		{write: "AT+CSQ\r", result: func() (int, error) { return 2, nil }},
		{write: "+CSQ\r", result: func() (int, error) { return 1, syscall.EINTR }},
		{write: "CSQ\r", chunks: []string{"\r\n+CSQ: 1,99\r\nOK\r\n"}},
	}}
	session := newTestSession(t, transport)
	response, err := session.Execute(context.Background(), "AT+CSQ")
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if response.Text() != "+CSQ: 1,99" {
		t.Fatalf("response = %#v", response)
	}
	transport.assertAccepted(t, "AT+CSQ\r")
}

func TestSessionExecutePromptQueuesURCsAndReturnsCMGS(t *testing.T) {
	const pdu = "00010005912143F50008044F60597D"
	transport := &transcriptTransport{steps: []transportStep{
		{
			write: "AT+CMGS=14\r",
			chunks: []string{
				"\r\nAT+CMGS=14\r\n+CMTI: \"SM\",7\r\n> ",
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

func TestSessionExecutePromptRecognizesErrorWithoutEcho(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{steps: []transportStep{
			{write: "AT+CMGS=5\r", chunks: []string{"\r\n> "}},
			{write: "ERROR\nHELLO"},
			{write: "\x1a", chunks: []string{"\r\nERROR\r\n"}},
		}}
		session := newTestSession(t, transport)
		response, err := session.ExecutePrompt(context.Background(), "AT+CMGS=5", []byte("ERROR\nHELLO"))
		var commandErr *CommandError
		if !errors.As(err, &commandErr) || commandErr.Final != "ERROR" || response.Final != "ERROR" || response.Text() != "" {
			t.Fatalf("ExecutePrompt() = %#v, %v", response, err)
		}
	})
}

func TestSessionExecutePromptTimeoutDoesNotWritePayload(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{
			steps: []transportStep{{write: "AT+CMGS=5\r"}},
		}
		session := newTestSession(t, transport)
		_, err := session.ExecutePrompt(
			context.Background(),
			"AT+CMGS=5",
			[]byte("001122"),
		)
		if !errors.Is(err, ErrCommandTimeout) {
			t.Fatalf("error = %v", err)
		}
		if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, ErrSessionUnsynchronized) {
			t.Fatalf("Execute() after prompt timeout = %v", err)
		}
		if session.Poisoned() {
			t.Fatal("prompt timeout poisoned the transport")
		}
	})
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

func TestSessionQueryKeepsCallURCsUntilFinal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{steps: []transportStep{{
			write: "AT+CLCC\r", chunks: []string{"\r\nRING\r\n+CLIP: \"12345\",129\r\nNO CARRIER\r\n"},
		}}}
		session := newTestSession(t, transport)
		transport.enqueue("\r\nCIEV: CALL 1\r\n")
		synctest.Wait()
		result := runExecute(context.Background(), session, "AT+CLCC")
		synctest.Wait()
		select {
		case got := <-result:
			t.Fatalf("call URCs completed CLCC: %#v, %v", got.response, got.err)
		default:
		}
		for _, want := range []string{"CIEV: CALL 1", "RING", `+CLIP: "12345",129`, "NO CARRIER"} {
			line, err := session.WaitURC(context.Background(), func(line string) bool { return line == want })
			if err != nil || line != want {
				t.Fatalf("WaitURC(%q) = %q, %v", want, line, err)
			}
		}
		transport.enqueue("\r\n+CLCC: 1,1,4,0,0,\"12345\",129\r\nOK\r\n")
		got := <-result
		if got.err != nil || !got.response.OK() || got.response.Text() != `+CLCC: 1,1,4,0,0,"12345",129` || len(got.response.URCs) != 3 {
			t.Fatalf("CLCC = %#v, %v", got.response, got.err)
		}
	})
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

func TestSessionCanceledPromptKeepsWireOwnership(t *testing.T) {
	for _, submitted := range []bool{false, true} {
		t.Run(fmt.Sprintf("submitted_%t", submitted), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				events := make(chan string, 4)
				steps := []transportStep{{write: "AT+CMGS=5\r"}}
				if submitted {
					steps[0].chunks = []string{"\r\n> "}
					steps = append(steps, transportStep{write: "001122"}, transportStep{write: "\x1a"})
				} else {
					steps = append(steps, transportStep{write: "\x1b"})
				}
				steps = append(steps, transportStep{write: "AT\r", chunks: []string{"\r\nOK\r\n"}})
				transport := &transcriptTransport{steps: steps, writeEvents: events}
				session := newTestSession(t, transport)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := runPrompt(ctx, session, "AT+CMGS=5", []byte("001122"))
				<-events
				if submitted {
					<-events
					<-events
				}
				cancel()
				synctest.Wait()
				select {
				case got := <-result:
					if !errors.Is(got.err, context.Canceled) || got.response.Final != "" {
						t.Fatalf("ExecutePrompt() = %#v, %v", got.response, got.err)
					}
				default:
					t.Fatal("canceled CMGS waited for the prompt or final")
				}
				if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, ErrSessionUnsynchronized) {
					t.Fatalf("AT before prompt completion = %v", err)
				}
				if !submitted {
					transport.enqueue("\r\n> ")
					if write := <-events; write != "\x1b" {
						t.Fatalf("late prompt write = %q, want ESC", write)
					}
					if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, ErrSessionUnsynchronized) {
						t.Fatalf("AT before abort acknowledgement = %v", err)
					}
				}
				transport.enqueue("\r\nOK\r\n")
				synctest.Wait()
				if response, err := session.Execute(context.Background(), "AT"); err != nil || !response.OK() {
					t.Fatalf("AT after prompt completion = %#v, %v", response, err)
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

func TestSessionCallCommandResults(t *testing.T) {
	transport := &transcriptTransport{steps: []transportStep{
		{write: "ATH\r", chunks: []string{"NO CARRIER\r\n"}},
		{write: "ATD12345;\r", chunks: []string{"CONNECT\r\nOK\r\n"}},
	}}
	session := newTestSession(t, transport)
	response, err := session.Execute(context.Background(), "ATH")
	var commandErr *CommandError
	if !errors.As(err, &commandErr) || commandErr.Command != "ATH" || commandErr.Final != "NO CARRIER" || response.Final != "NO CARRIER" {
		t.Fatalf("call final = %#v, %v", response, err)
	}
	response, err = session.Execute(context.Background(), "ATD12345;")
	if err != nil || !response.OK() || response.Text() != "" || session.Poisoned() {
		t.Fatalf("voice CONNECT = %#v, %v", response, err)
	}
}

func TestSessionUnsupportedInputStopsBatch(t *testing.T) {
	for _, test := range []struct {
		command string
		input   string
		cause   error
	}{
		{command: "AT+CSQ", input: "> \r\nOK\r\n", cause: ErrSessionUnsynchronized},
		{command: "ATD12345", input: "CONNECT 115200\r\nOK\r\n", cause: ErrUnsupportedDataMode},
	} {
		t.Run(test.command, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				transport := &transcriptTransport{steps: []transportStep{{write: test.command + "\r", chunks: []string{test.input}}}}
				session := newTestSession(t, transport)
				response, err := session.Execute(context.Background(), test.command)
				if !errors.Is(err, test.cause) || response.Final != "" {
					t.Fatalf("Execute() = %#v, %v", response, err)
				}
				synctest.Wait()
				assertSessionUnsynchronized(t, session, transport)
			})
		})
	}
}

func TestSessionDrainsBeforeWritingCommand(t *testing.T) {
	transport := &transcriptTransport{steps: []transportStep{{
		write:  "AT+CSQ\r",
		chunks: []string{"\r\n+CSQ: 24,99\r\nOK\r\n"},
	}}}
	session := newTestSession(t, transport)
	if _, err := session.Execute(context.Background(), "AT+CSQ"); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	transport.mu.Lock()
	ops := append([]string(nil), transport.ops...)
	transport.mu.Unlock()
	if len(ops) != 2 || ops[0] != "drain" || ops[1] != "write:AT+CSQ\r" {
		t.Fatalf("transport operations = %q, want drain before the command write", ops)
	}
}

func TestSessionConsumesRXPublishedDuringPreWriteDrain(t *testing.T) {
	for _, cancelPending := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_%t", cancelPending), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				gate, started := make(chan struct{}), make(chan struct{})
				releaseDrain := sync.OnceFunc(func() { close(gate) })
				events := make(chan string, 2)
				steps := []transportStep{{write: "AT\r", chunks: []string{"\r\nOK\r\n"}}}
				if !cancelPending {
					steps = append([]transportStep{{write: "AT+CPIN?\r", chunks: []string{"\r\n+CPIN: READY\r\nOK\r\n"}}}, steps...)
				}
				transport := &transcriptTransport{
					steps:        steps,
					writeEvents:  events,
					drainGate:    gate,
					drainStarted: started,
				}
				session := newTestSession(t, transport)
				t.Cleanup(releaseDrain)
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				result := runExecute(ctx, session, "AT+CPIN?")
				<-started
				next := runExecute(context.Background(), session, "AT")
				transport.enqueue("\r\n+CFUN: STA")
				synctest.Wait()
				releaseDrain()
				synctest.Wait()
				select {
				case write := <-events:
					t.Fatalf("wrote before pre-drain frame completed: %q", write)
				default:
				}
				if cancelPending {
					cancel()
					if got := <-result; !errors.Is(got.err, context.Canceled) {
						t.Fatalf("pending cancellation = %v", got.err)
					}
				}
				transport.enqueue("LE\r\nOK\r\n")
				if !cancelPending {
					if got := <-result; got.err != nil || !got.response.OK() || got.response.Text() != "+CPIN: READY" {
						t.Fatalf("CPIN = %#v, %v", got.response, got.err)
					}
				}
				if got := <-next; got.err != nil || !got.response.OK() {
					t.Fatalf("command behind pending = %#v, %v", got.response, got.err)
				}
			})
		})
	}
}

func TestSessionCanceledPayloadEchoDoesNotAcknowledgeAbort(t *testing.T) {
	for _, payload := range []string{"ERROR\nHELLO", ">\nHELLO"} {
		t.Run(payload, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				transport := &transcriptTransport{steps: []transportStep{
					{write: "AT+CMGS=5\r", chunks: []string{"AT+CMGS=5\r\n> "}},
					{write: payload, chunks: []string{strings.ReplaceAll(payload, "\n", "\r\n") + "\r\n"}, result: func() (int, error) {
						cancel()
						return len(payload), nil
					}},
					{write: "\x1b"},
					{write: "AT+CPIN?\r", chunks: []string{"\r\n+CPIN: READY\r\nOK\r\n"}},
				}}
				session := newTestSession(t, transport)
				response, err := session.ExecutePrompt(ctx, "AT+CMGS=5", []byte(payload))
				var commandErr *CommandError
				if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrSessionUnsynchronized) || errors.As(err, &commandErr) || response.Final != "" {
					t.Fatalf("canceled payload = %#v, %v", response, err)
				}
				synctest.Wait()
				if _, err := session.Execute(context.Background(), "AT+CPIN?"); !errors.Is(err, ErrSessionUnsynchronized) {
					t.Fatalf("CPIN before ESC acknowledgement = %v", err)
				}
				transport.enqueue("\r\nERROR\r\n")
				synctest.Wait()
				response, err = session.Execute(context.Background(), "AT+CPIN?")
				if err != nil || !response.OK() || response.Text() != "+CPIN: READY" {
					t.Fatalf("CPIN after ESC acknowledgement = %#v, %v", response, err)
				}
			})
		})
	}
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
			if !errors.Is(got.err, context.Canceled) || !errors.Is(got.err, ErrSessionUnsynchronized) || got.response.Final != "" {
				t.Fatalf("canceled command = %#v, %v", got.response, got.err)
			}
		default:
			t.Fatal("caller cancellation waited for committed I/O")
		}
		transport.assertAccepted(t, command[:2])
		releaseWrite()
		synctest.Wait()
		transport.assertAccepted(t, command)
		if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, ErrSessionUnsynchronized) {
			t.Fatalf("next command before late final = %v", err)
		}
		if session.Poisoned() {
			t.Fatal("caller cancellation poisoned the session")
		}
		transport.enqueue("\r\n+CSQ: 24,99\r\nOK\r\n")
		synctest.Wait()
		if response, err := session.Execute(context.Background(), "AT"); err != nil || !response.OK() {
			t.Fatalf("next command after late final = %#v, %v", response, err)
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
		if !errors.Is(err, ErrSessionUnsynchronized) || !errors.Is(err, syscall.EIO) || errors.As(err, &commandErr) || response.Final != "" {
			t.Fatalf("payload write failure = %#v, %v", response, err)
		}
		synctest.Wait()
		assertSessionUnsynchronized(t, session, transport)
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

func TestSessionPendingWriteFailureOverridesReadFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const command = "AT+CSQ\r"
		started, release := make(chan struct{}), make(chan struct{})
		releaseWrite := sync.OnceFunc(func() { close(release) })
		transport := &transcriptTransport{steps: []transportStep{{write: command, result: func() (int, error) {
			close(started)
			<-release
			return 2, io.ErrShortWrite
		}}}}
		session := newTestSession(t, transport)
		t.Cleanup(releaseWrite)
		result := runExecute(context.Background(), session, strings.TrimSuffix(command, "\r"))
		<-started
		transport.enqueueRead(atReadBatch{err: syscall.EIO})
		synctest.Wait()
		if !errors.Is(session.Err(), syscall.EIO) {
			t.Fatal("reader did not observe injected EIO")
		}
		if session.Poisoned() {
			t.Fatal("reader failure allowed reopening while committed write was pending")
		}
		select {
		case got := <-result:
			t.Fatalf("pending write completed before accepted count was known: %#v, %v", got.response, got.err)
		default:
		}
		releaseWrite()
		got := <-result
		if !errors.Is(got.err, ErrSessionUnsynchronized) || !errors.Is(got.err, io.ErrShortWrite) || errors.Is(got.err, syscall.EIO) || got.response.Final != "" {
			t.Fatalf("partial write after reader failure = %#v, %v", got.response, got.err)
		}
		synctest.Wait()
		assertSessionUnsynchronized(t, session, transport)
		transport.assertAccepted(t, command[:2])
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
		if !session.Poisoned() {
			t.Fatal("physical EOF did not poison the session")
		}
		if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, io.EOF) || !errors.Is(err, ErrSessionClosed) {
			t.Fatalf("new command after EOF = %v", err)
		}
	})
}

func assertSessionUnsynchronized(t *testing.T, session *Session, transport *transcriptTransport) {
	t.Helper()
	if session.Poisoned() {
		t.Fatal("logical failure poisoned the session")
	}
	if _, err := session.Execute(context.Background(), "AT"); !errors.Is(err, ErrSessionUnsynchronized) || errors.Is(err, ErrSessionClosed) {
		t.Fatalf("Execute after logical failure = %v", err)
	}
	transport.mu.Lock()
	closed := transport.closed
	transport.mu.Unlock()
	if !closed {
		t.Fatal("logical failure did not close the transport")
	}
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
