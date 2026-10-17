package device

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/modem"
)

type urcTranscriptClient struct {
	*transcriptClient
	mu      sync.Mutex
	handler func(string)
}

func (client *urcTranscriptClient) SetURCHandler(handler func(string)) {
	client.mu.Lock()
	defer client.mu.Unlock()
	client.handler = handler
}

func (client *urcTranscriptClient) emit(line string) {
	client.mu.Lock()
	handler := client.handler
	client.mu.Unlock()
	if handler != nil {
		handler(line)
	}
}

func TestManagerPublishesSMSArrivalURCs(t *testing.T) {
	client := &urcTranscriptClient{transcriptClient: &transcriptClient{steps: []clientStep{
		{command: "AT+CMGF=0", response: okResponse()},
		{command: `AT+CPMS="SM"`, response: okResponse("+CPMS: 0,40,0,23,0,40")},
		{command: "AT+CMGL=4", response: okResponse()},
		{command: `AT+CPMS="ME"`, response: okResponse("+CPMS: 0,23,0,40,0,40")},
		{command: "AT+CMGL=4", response: okResponse()},
		{command: `AT+CPMS="SM"`, response: okResponse("+CPMS: 0,40,0,23,0,40")},
		// Already announcing with +CMTI: no AT+CNMI write is expected.
		{command: "AT+CNMI?", response: okResponse("+CNMI: 2,1,0,2,0")},
	}}}
	manager, id := newStartedTestManager(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := manager.SubscribeSMSEvents(ctx)
	if err != nil {
		t.Fatalf("SubscribeSMSEvents: %v", err)
	}
	if _, err := manager.ListSMS(context.Background(), id); err != nil {
		t.Fatalf("ListSMS: %v", err)
	}

	client.emit("+CEREG: 1")
	select {
	case got := <-events:
		t.Fatalf("non-SMS URC published event for %q", got)
	default:
	}
	client.emit(`+CMTI: "SM",5`)
	select {
	case got := <-events:
		if got != id {
			t.Fatalf("event device = %q, want %q", got, id)
		}
	case <-time.After(time.Second):
		t.Fatal("+CMTI did not publish an SMS event")
	}

	cancel()
	if _, open := <-events; open {
		t.Fatal("SMS event channel stayed open after cancellation")
	}
}

func TestEnsureSMSIndicationsFallsBackToShortForm(t *testing.T) {
	client := &transcriptClient{steps: []clientStep{
		{command: "AT+CNMI?", response: okResponse("+CNMI: 1,2")},
		{command: "AT+CNMI=2,1,0,0,0", err: errors.New("+CMS ERROR: 303")},
		{command: "AT+CNMI=2,1", response: okResponse()},
	}}
	manager := &Manager{commandTimeout: time.Second}
	state := &managedDevice{}
	manager.ensureSMSIndications(context.Background(), state, client)
	if client.unexpected != nil || len(client.steps) != 0 {
		t.Fatalf("unexpected = %v, remaining = %#v", client.unexpected, client.steps)
	}
	if !state.smsIndicationRetryAt.IsZero() {
		t.Fatalf("successful fallback scheduled a retry backoff at %v", state.smsIndicationRetryAt)
	}
}

// hangingCNMIClient answers storage commands from a transcript but never
// answers AT+CNMI, like a modem whose reply is lost.
type hangingCNMIClient struct {
	*transcriptClient
	mu        sync.Mutex
	cnmiCalls int
}

func (client *hangingCNMIClient) Execute(ctx context.Context, command string) (modem.Response, error) {
	if strings.HasPrefix(command, "AT+CNMI") {
		client.mu.Lock()
		client.cnmiCalls++
		client.mu.Unlock()
		<-ctx.Done()
		return modem.Response{Command: command}, errors.Join(modem.ErrCommandTimeout, ctx.Err())
	}
	return client.transcriptClient.Execute(ctx, command)
}

func TestListSMSSurvivesUnresponsiveCNMIAndBacksOff(t *testing.T) {
	scan := []clientStep{
		{command: "AT+CMGF=0", response: okResponse()},
		{command: `AT+CPMS="SM"`, response: okResponse("+CPMS: 0,40,0,23,0,40")},
		{command: "AT+CMGL=4", response: okResponse()},
		{command: `AT+CPMS="ME"`, response: okResponse("+CPMS: 0,23,0,40,0,40")},
		{command: "AT+CMGL=4", response: okResponse()},
		{command: `AT+CPMS="SM"`, response: okResponse("+CPMS: 0,40,0,23,0,40")},
	}
	client := &hangingCNMIClient{transcriptClient: &transcriptClient{
		steps: append(append([]clientStep(nil), scan...), scan...),
	}}
	manager, id := newStartedTestManager(t, client)
	for pass := 1; pass <= 2; pass++ {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		started := time.Now()
		listing, err := manager.ListSMS(ctx, id)
		cancel()
		if err != nil {
			t.Fatalf("pass %d ListSMS: %v", pass, err)
		}
		if listing.Storage.SM.Total != 40 || listing.Storage.ME.Total != 23 {
			t.Fatalf("pass %d storage = %#v", pass, listing.Storage)
		}
		if elapsed := time.Since(started); elapsed > 5*time.Second {
			t.Fatalf("pass %d took %v; CNMI consumed the scan budget", pass, elapsed)
		}
	}
	client.mu.Lock()
	calls := client.cnmiCalls
	client.mu.Unlock()
	if calls != 1 || client.unexpected != nil {
		t.Fatalf("CNMI calls = %d (want 1 with backoff), unexpected = %v", calls, client.unexpected)
	}
}
