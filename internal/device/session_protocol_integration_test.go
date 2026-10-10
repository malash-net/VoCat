package device

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"vocat/internal/modem"
)

type protocolTransportStep struct {
	write, reply string
	cancel       context.CancelFunc
}

type protocolTestTransport struct {
	mu         sync.Mutex
	steps      []protocolTransportStep
	unexpected error
	input      chan string
	stop       chan struct{}
	closeOnce  sync.Once
	remaining  string
}

func newProtocolTestSession(t *testing.T, steps []protocolTransportStep) (*modem.Session, *protocolTestTransport) {
	t.Helper()
	transport := &protocolTestTransport{steps: steps, input: make(chan string, 16), stop: make(chan struct{})}
	session, err := modem.NewSession(transport, modem.SessionOptions{CommandTimeout: 10 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = session.Close()
		transport.mu.Lock()
		defer transport.mu.Unlock()
		if transport.unexpected != nil || len(transport.steps) != 0 {
			t.Errorf("unfinished protocol transcript: steps=%d error=%v", len(transport.steps), transport.unexpected)
		}
	})
	return session, transport
}

func (transport *protocolTestTransport) Read(buffer []byte) (int, error) {
	if transport.remaining == "" {
		select {
		case transport.remaining = <-transport.input:
		case <-transport.stop:
			return 0, io.EOF
		}
	}
	count := copy(buffer, transport.remaining)
	transport.remaining = transport.remaining[count:]
	return count, nil
}

func (transport *protocolTestTransport) Write(payload []byte) (int, error) {
	transport.mu.Lock()
	defer transport.mu.Unlock()
	if len(transport.steps) == 0 || transport.steps[0].write != string(payload) {
		transport.unexpected = fmt.Errorf("unexpected write %q", payload)
		return 0, transport.unexpected
	}
	step := transport.steps[0]
	transport.steps = transport.steps[1:]
	if step.reply != "" {
		transport.input <- step.reply
	}
	if step.cancel != nil {
		step.cancel()
	}
	return len(payload), nil
}

func (transport *protocolTestTransport) Drain() error                       { return nil }
func (transport *protocolTestTransport) SetReadTimeout(time.Duration) error { return nil }
func (transport *protocolTestTransport) Close() error {
	transport.closeOnce.Do(func() { close(transport.stop) })
	return nil
}

func textSMSProtocolSetup() []protocolTransportStep {
	return []protocolTransportStep{
		{write: "AT+CMGF=1\r", reply: "\r\nOK\r\n"},
		{write: "AT+CSCS=\"GSM\"\r", reply: "\r\nOK\r\n"},
		{write: "AT+CSMP=49,167,0,0\r", reply: "\r\nOK\r\n"},
	}
}

func TestManagerSessionSMSCallURCLeavesSubmissionUnknown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		steps := append(textSMSProtocolSetup(),
			protocolTransportStep{write: "AT+CMGS=\"+15551234567\"\r", reply: "\r\n> "},
			protocolTransportStep{write: "hello"},
			protocolTransportStep{write: "\x1a", reply: "\r\nNO CARRIER\r\n"},
			protocolTransportStep{write: "AT\r", reply: "\r\nOK\r\n"},
		)
		session, transport := newProtocolTestSession(t, steps)
		manager, id := newStartedTestManager(t, session)
		manager.smsTimeout = time.Second
		result, err := manager.SendSMS(context.Background(), id, "+15551234567", "hello")
		var commandErr *modem.CommandError
		if !errors.Is(err, modem.ErrCommandTimeout) || !errors.Is(err, modem.ErrSessionUnsynchronized) || errors.As(err, &commandErr) {
			t.Fatalf("SendSMS error = %v", err)
		}
		if result.SubmissionStatus != "unknown" || result.ModemFinal != "" || result.ReferenceKnown || result.AcceptedByModem {
			t.Fatalf("uncertain SMS = %#v", result)
		}
		if _, err := manager.ExecuteAT(context.Background(), id, "AT"); !errors.Is(err, modem.ErrSessionUnsynchronized) {
			t.Fatalf("AT before SMS final = %v", err)
		}
		synctest.Wait()
		if len(transport.steps) != 1 || transport.unexpected != nil {
			t.Fatal("wrote before SMS final")
		}
		transport.input <- "\r\n+CMGS: 42\r\nOK\r\n"
		synctest.Wait()
		if response, err := manager.ExecuteAT(context.Background(), id, "AT"); err != nil || !response.OK() {
			t.Fatalf("AT after SMS final = %#v, %v", response, err)
		}
	})
}

func TestManagerSessionCanceledTextEchoWaitsForAbortFinal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		steps := append(textSMSProtocolSetup(),
			protocolTransportStep{write: "AT+CMGS=\"+15551234567\"\r", reply: "AT+CMGS=\"+15551234567\"\r\n> "},
			protocolTransportStep{write: "OK\nHELLO", reply: "OK\r\nHELLO\r\n", cancel: cancel},
			protocolTransportStep{write: "\x1b"},
			protocolTransportStep{write: "AT+CPIN?\r", reply: "\r\n+CPIN: READY\r\nOK\r\n"},
		)
		session, transport := newProtocolTestSession(t, steps)
		manager, id := newStartedTestManager(t, session)
		result, err := manager.SendSMS(ctx, id, "+15551234567", "OK\nHELLO")
		if !errors.Is(err, context.Canceled) || !errors.Is(err, modem.ErrSessionUnsynchronized) || result.SubmissionStatus != "unknown" {
			t.Fatalf("SendSMS cancellation = %#v, %v", result, err)
		}
		synctest.Wait()
		if _, err := manager.ExecuteAT(context.Background(), id, "AT+CPIN?"); !errors.Is(err, modem.ErrSessionUnsynchronized) {
			t.Fatalf("CPIN before ESC acknowledgement = %v", err)
		}
		synctest.Wait()
		if len(transport.steps) != 1 || transport.unexpected != nil {
			t.Fatal("wrote before ESC acknowledgement")
		}
		transport.input <- "\r\nERROR\r\n"
		synctest.Wait()
		if response, err := manager.ExecuteAT(context.Background(), id, "AT+CPIN?"); err != nil || !response.OK() || response.Text() != "+CPIN: READY" {
			t.Fatalf("CPIN after ESC acknowledgement = %#v, %v", response, err)
		}
	})
}

type settledProtocolClient struct{ *modem.Session }

func (client *settledProtocolClient) Execute(ctx context.Context, command string) (modem.Response, error) {
	response, err := client.Session.Execute(ctx, command)
	synctest.Wait()
	return response, err
}

func TestManagerSessionRebootRetainsTrailingProtocolFailure(t *testing.T) {
	for _, final := range []string{"OK", "ERROR"} {
		t.Run(final, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				session, _ := newProtocolTestSession(t, []protocolTransportStep{
					{write: "AT+CFUN?\r", reply: "\r\n+CFUN: 1\r\nOK\r\n"},
					{write: "AT+CFUN=1,1\r", reply: "\r\n" + final + "\r\n> "},
				})
				client := &settledProtocolClient{session}
				manager, id := newStartedTestManager(t, client)
				err := manager.Reboot(context.Background(), id)
				var commandErr *modem.CommandError
				if !errors.Is(err, modem.ErrSessionUnsynchronized) || errors.As(err, &commandErr) != (final == "ERROR") {
					t.Fatalf("Reboot after final and unexpected prompt = %v", err)
				}
				if manager.devices[id].client != client || session.Poisoned() {
					t.Fatal("reboot discarded the logically failed owner")
				}
				if _, err := manager.ExecuteAT(context.Background(), id, "AT"); !errors.Is(err, modem.ErrSessionUnsynchronized) {
					t.Fatalf("AT after trailing protocol failure = %v", err)
				}
			})
		})
	}
}
