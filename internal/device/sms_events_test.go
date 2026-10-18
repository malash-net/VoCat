package device

import (
	"context"
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
		{command: `AT+CPMS="SM"`, response: okResponse("+CPMS: 0,40,0,23,0,40")},
		{command: "AT+CMGL=4", response: okResponse()},
		{command: `AT+CPMS="ME"`, response: okResponse("+CPMS: 0,23,0,40,0,40")},
		{command: "AT+CMGL=4", response: okResponse()},
		{command: `AT+CPMS="SM"`, response: okResponse("+CPMS: 0,40,0,23,0,40")},
	}}}
	manager, id := newStartedTestManager(t, client)
	if _, err := manager.ListSMS(context.Background(), id); err != nil {
		t.Fatalf("ListSMS: %v", err)
	}

	// Only +CMTI announces a stored SMS under the CNMI mode VoCat configures.
	for _, line := range []string{"+CEREG: 1", `+CMT: "+447700900123",,"26/10/18,10:00:00+00"`, "+CDSI: \"SM\",3"} {
		client.emit(line)
	}
	select {
	case <-manager.SMSArrivals():
		t.Fatal("non-+CMTI URC signalled an SMS arrival")
	default:
	}
	client.emit(`+CMTI: "SM",5`)
	client.emit(`+CMTI: "SM",6`)
	select {
	case <-manager.SMSArrivals():
	case <-time.After(time.Second):
		t.Fatal("+CMTI did not signal an SMS arrival")
	}
	// The burst collapses into the single wake-up already consumed.
	select {
	case <-manager.SMSArrivals():
		t.Fatal("+CMTI burst produced more than one wake-up")
	default:
	}
}
