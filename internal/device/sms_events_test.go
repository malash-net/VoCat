package device

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"vocat/internal/modem"
)

type smsNotificationTranscriptClient struct {
	*transcriptClient
	notificationMu sync.Mutex
	handler        func()
	bindCount      int
	poisoned       bool
}

func (client *smsNotificationTranscriptClient) SetSMSNotificationHandler(handler func()) {
	client.notificationMu.Lock()
	defer client.notificationMu.Unlock()
	client.handler = handler
	client.bindCount++
}

func (client *smsNotificationTranscriptClient) notifySMS() {
	client.notificationMu.Lock()
	handler := client.handler
	client.notificationMu.Unlock()
	if handler != nil {
		handler()
	}
}

func (client *smsNotificationTranscriptClient) Poisoned() bool {
	client.notificationMu.Lock()
	defer client.notificationMu.Unlock()
	return client.poisoned
}

func (client *smsNotificationTranscriptClient) Close() error {
	client.notificationMu.Lock()
	client.handler = nil
	client.notificationMu.Unlock()
	return client.transcriptClient.Close()
}

func TestManagerSMSNotificationsPublishPhysicalDeviceID(t *testing.T) {
	client := &smsNotificationTranscriptClient{transcriptClient: &transcriptClient{steps: []clientStep{
		{command: "AT", response: okResponse()},
		{command: "AT", response: okResponse()},
	}}}
	manager, id := newStartedTestManager(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	beforeOpen, err := manager.SubscribeSMSReceivedEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ExecuteAT(ctx, id, "AT"); err != nil {
		t.Fatal(err)
	}
	afterOpen, err := manager.SubscribeSMSReceivedEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ExecuteAT(ctx, id, "AT"); err != nil {
		t.Fatal(err)
	}
	if client.bindCount != 1 {
		t.Fatalf("handler bound %d times for a cached client, want 1", client.bindCount)
	}

	state, err := manager.lookup(id)
	if err != nil {
		t.Fatal(err)
	}
	state.opMu.Lock()
	manager.mu.Lock()
	state.snapshot = &Snapshot{DeviceID: "logical-sim-id", ICCID: "previous-sim-iccid"}
	done := make(chan struct{})
	go func() {
		client.notifySMS()
		close(done)
	}()
	completed := false
	select {
	case <-done:
		completed = true
	case <-time.After(time.Second):
	}
	manager.mu.Unlock()
	state.opMu.Unlock()
	if !completed {
		t.Fatal("SMS callback blocked on device operation or manager locks")
	}

	for _, events := range []<-chan SMSReceivedEvent{beforeOpen, afterOpen} {
		select {
		case event := <-events:
			if event != (SMSReceivedEvent{DeviceID: id}) {
				t.Fatalf("SMS event = %+v, want physical ID %q", event, id)
			}
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for SMS event")
		}
	}
	client.assertDone(t)
}

func TestManagerSMSNotificationsSupportLegacyClient(t *testing.T) {
	client := &transcriptClient{steps: []clientStep{{command: "AT", response: okResponse()}}}
	if _, ok := any(client).(modem.SMSNotificationClient); ok {
		t.Fatal("legacy test client unexpectedly supports SMS notifications")
	}
	manager, id := newStartedTestManager(t, client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := manager.SubscribeSMSReceivedEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ExecuteAT(ctx, id, "AT"); err != nil {
		t.Fatal(err)
	}
	select {
	case event := <-events:
		t.Fatalf("unexpected SMS event from legacy client: %+v", event)
	default:
	}
	client.assertDone(t)
}

func TestManagerSMSNotificationsRebindAfterPoisonedReopen(t *testing.T) {
	first := &smsNotificationTranscriptClient{transcriptClient: &transcriptClient{steps: []clientStep{
		{command: "AT", response: okResponse()},
	}}}
	second := &smsNotificationTranscriptClient{transcriptClient: &transcriptClient{steps: []clientStep{
		{command: "AT", response: okResponse()},
	}}}
	manager, id := newStartedTestManager(t, first)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := manager.SubscribeSMSReceivedEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.ExecuteAT(ctx, id, "AT"); err != nil {
		t.Fatal(err)
	}
	first.notificationMu.Lock()
	first.poisoned = true
	first.notificationMu.Unlock()
	opener := manager.opener.(*staticOpener)
	opener.mu.Lock()
	opener.client = second
	opener.mu.Unlock()
	if _, err := manager.ExecuteAT(ctx, id, "AT"); err != nil {
		t.Fatal(err)
	}
	if first.bindCount != 1 || second.bindCount != 1 || first.closeCount != 1 || opener.openCount != 2 {
		t.Fatalf("reopen counts: first binds=%d closes=%d, second binds=%d, opens=%d",
			first.bindCount, first.closeCount, second.bindCount, opener.openCount)
	}
	first.notifySMS()
	select {
	case event := <-events:
		t.Fatalf("closed client published an SMS event: %+v", event)
	default:
	}
	second.notifySMS()
	select {
	case event := <-events:
		if event != (SMSReceivedEvent{DeviceID: id}) {
			t.Fatalf("reopened client SMS event = %+v, want physical ID %q", event, id)
		}
	case <-time.After(time.Second):
		t.Fatal("reopened client did not publish SMS event")
	}
	first.assertDone(t)
	second.assertDone(t)
}

func TestManagerSMSReceivedEventsIgnoreEmptyDeviceID(t *testing.T) {
	manager := &Manager{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := manager.SubscribeSMSReceivedEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"", " \t\n"} {
		manager.publishSMSReceivedEvent(id)
	}
	select {
	case event := <-events:
		t.Fatalf("empty physical ID published an event: %+v", event)
	default:
	}
}

func TestManagerSMSReceivedEventsOverflowRetainsRescanAll(t *testing.T) {
	manager := &Manager{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := manager.SubscribeSMSReceivedEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cap(events) != smsReceivedEventBufferSize {
		t.Fatalf("event capacity = %d, want %d", cap(events), smsReceivedEventBufferSize)
	}
	for index := 0; index < cap(events); index++ {
		manager.publishSMSReceivedEvent(fmt.Sprintf("physical-%d", index))
	}
	manager.publishSMSReceivedEvent("overflow-device")
	// Fill and overflow again without consuming the first full-rescan marker.
	for index := 0; index < cap(events); index++ {
		manager.publishSMSReceivedEvent(fmt.Sprintf("later-physical-%d", index))
	}
	select {
	case event := <-events:
		if event != (SMSReceivedEvent{RescanAll: true}) {
			t.Fatalf("overflow event = %+v, want explicit full rescan", event)
		}
	default:
		t.Fatal("overflow lost the full-rescan request")
	}
	manager.publishSMSReceivedEvent("after-rescan")
	select {
	case event := <-events:
		if event != (SMSReceivedEvent{DeviceID: "after-rescan"}) {
			t.Fatalf("event after consuming rescan = %+v", event)
		}
	default:
		t.Fatal("ordinary events did not resume after rescan")
	}
}

func TestManagerSMSReceivedEventsSlowSubscriberDoesNotBlock(t *testing.T) {
	manager := &Manager{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	slow, err := manager.SubscribeSMSReceivedEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	fast, err := manager.SubscribeSMSReceivedEvents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		for index := 0; index < smsReceivedEventBufferSize*8; index++ {
			id := fmt.Sprintf("physical-%d", index)
			manager.publishSMSReceivedEvent(id)
			select {
			case event := <-fast:
				if event != (SMSReceivedEvent{DeviceID: id}) {
					done <- fmt.Errorf("fast subscriber event = %+v, want device %q", event, id)
					return
				}
			default:
				done <- errors.New("fast subscriber missed SMS event")
				return
			}
		}
		done <- nil
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("slow SMS subscriber blocked publication")
	}
	select {
	case event := <-slow:
		if !event.RescanAll {
			t.Fatalf("slow subscriber event = %+v, want full rescan", event)
		}
	default:
		t.Fatal("slow subscriber lost its rescan request")
	}
}

func TestManagerSMSReceivedEventsCancelDuringConcurrentPublication(t *testing.T) {
	manager := &Manager{}
	var subscriptions []<-chan SMSReceivedEvent
	var cancellations []context.CancelFunc
	for range 16 {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		events, err := manager.SubscribeSMSReceivedEvents(ctx)
		if err != nil {
			t.Fatal(err)
		}
		subscriptions = append(subscriptions, events)
		cancellations = append(cancellations, cancel)
	}
	var workers sync.WaitGroup
	start := make(chan struct{})
	for publisher := range 4 {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			for range 256 {
				manager.publishSMSReceivedEvent(fmt.Sprintf("physical-%d", publisher))
			}
		}()
	}
	for _, cancel := range cancellations {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			cancel()
		}()
	}
	close(start)
	workers.Wait()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for _, events := range subscriptions {
	drain:
		for {
			select {
			case _, ok := <-events:
				if !ok {
					break drain
				}
			case <-deadline.C:
				t.Fatal("canceled SMS subscription was not closed")
			}
		}
	}
	manager.smsEventsMu.Lock()
	remaining := len(manager.smsEventSubscribers)
	manager.smsEventsMu.Unlock()
	if remaining != 0 {
		t.Fatalf("%d canceled subscriptions remain registered", remaining)
	}
	manager.publishSMSReceivedEvent("after-cancellation")
}

func TestManagerSMSReceivedEventsSubscriptionContexts(t *testing.T) {
	var absent *Manager
	if _, err := absent.SubscribeSMSReceivedEvents(context.Background()); !errors.Is(err, ErrNotStarted) {
		t.Fatalf("nil manager subscription error = %v", err)
	}
	manager := &Manager{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := manager.SubscribeSMSReceivedEvents(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context subscription error = %v", err)
	}
	events, err := manager.SubscribeSMSReceivedEvents(nil)
	if err != nil {
		t.Fatal(err)
	}
	manager.publishSMSReceivedEvent("physical-device")
	select {
	case event := <-events:
		if event != (SMSReceivedEvent{DeviceID: "physical-device"}) {
			t.Fatalf("nil context subscription event = %+v", event)
		}
	default:
		t.Fatal("nil context subscription did not receive event")
	}
}
