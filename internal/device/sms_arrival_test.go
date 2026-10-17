package device

import (
	"context"
	"errors"
	"testing"
	"time"

	"vocat/internal/modem"
)

// smsArrivalClient stands in for a modem.Session that sniffs +CMTI.
type smsArrivalClient struct {
	handler func()
}

func (client *smsArrivalClient) Execute(context.Context, string) (modem.Response, error) {
	return modem.Response{}, nil
}

func (client *smsArrivalClient) WaitURC(context.Context, func(string) bool) (string, error) {
	return "", errors.New("not implemented")
}

func (client *smsArrivalClient) Close() error { return nil }

func (client *smsArrivalClient) SetSMSArrivalHandler(handler func()) { client.handler = handler }

func TestSMSArrivalEventPublishesDeviceID(t *testing.T) {
	manager := &Manager{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := manager.SubscribeSMSArrivalEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	client := &smsArrivalClient{}
	manager.registerSMSArrivalHandler("physical-1", client)
	if client.handler == nil {
		t.Fatal("arrival handler was not registered on the client")
	}
	client.handler()
	select {
	case event := <-events:
		if len(event.DeviceIDs) != 1 || event.DeviceIDs[0] != "physical-1" {
			t.Fatalf("event device IDs = %v, want [physical-1]", event.DeviceIDs)
		}
	case <-time.After(time.Second):
		t.Fatal("SMS arrival event was not published")
	}
}

// Two devices signalling before the consumer reads must both be reported.
func TestSMSArrivalEventKeepsEveryDevice(t *testing.T) {
	manager := &Manager{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := manager.SubscribeSMSArrivalEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	manager.publishSMSArrivalEvent("physical-b")
	manager.publishSMSArrivalEvent("physical-c")

	seen := map[string]bool{}
	for len(seen) < 2 {
		select {
		case event := <-events:
			for _, id := range event.DeviceIDs {
				seen[id] = true
			}
		case <-time.After(time.Second):
			t.Fatalf("only received %v; a device wake-up was lost", seen)
		}
	}
}

// A client without the optional capability must be left alone, so existing
// fakes need no update and the periodic scan remains its path.
func TestSMSArrivalEventSkipsClientWithoutCapability(t *testing.T) {
	manager := &Manager{}
	manager.registerSMSArrivalHandler("physical-1", &transcriptClient{})
}
