package device

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
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
		// Already announcing with +CMTI: no AT+CNMI write is expected.
		{command: "AT+CNMI?", response: okResponse("+CNMI: 2,1,0,2,0")},
		{command: `AT+CPMS="SM"`, response: okResponse("+CPMS: 0,40,0,23,0,40")},
		{command: "AT+CMGL=4", response: okResponse()},
		{command: `AT+CPMS="ME"`, response: okResponse("+CPMS: 0,23,0,40,0,40")},
		{command: "AT+CMGL=4", response: okResponse()},
		{command: `AT+CPMS="SM"`, response: okResponse("+CPMS: 0,40,0,23,0,40")},
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
	manager.ensureSMSIndications(context.Background(), client)
	if client.unexpected != nil || len(client.steps) != 0 {
		t.Fatalf("unexpected = %v, remaining = %#v", client.unexpected, client.steps)
	}
}
