package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"vocat/internal/developer"
	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
	"vocat/internal/vowifi"
)

type smsSyncTestCall struct {
	kind       string
	physicalID string
	command    string
	ctxErr     error
	deadline   time.Duration
}

type smsSyncTestController struct {
	fakeDeviceController
	mu           sync.Mutex
	entries      []device.Device
	calls        []smsSyncTestCall
	active       int
	maxActive    int
	started      chan string
	listing      device.SMSListing
	listHandler  func(context.Context, string) (device.SMSListing, error)
	setupHandler func(context.Context, string) error
}

func (controller *smsSyncTestController) List() []device.Device {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return append([]device.Device(nil), controller.entries...)
}

func (controller *smsSyncTestController) Get(id string) (device.Device, error) {
	for _, entry := range controller.List() {
		if entry.ID == id {
			return entry, nil
		}
	}
	return device.Device{}, device.ErrNotFound
}

func (controller *smsSyncTestController) ExecuteAT(ctx context.Context, id, command string) (modem.Response, error) {
	deadline, _ := ctx.Deadline()
	controller.mu.Lock()
	controller.calls = append(controller.calls, smsSyncTestCall{
		kind: "setup", physicalID: id, command: command, ctxErr: ctx.Err(), deadline: time.Until(deadline),
	})
	controller.mu.Unlock()
	if controller.setupHandler != nil {
		return modem.Response{}, controller.setupHandler(ctx, id)
	}
	return modem.Response{}, nil
}

func (controller *smsSyncTestController) ListSMS(ctx context.Context, id string) (device.SMSListing, error) {
	controller.mu.Lock()
	controller.calls = append(controller.calls, smsSyncTestCall{kind: "list", physicalID: id, ctxErr: ctx.Err()})
	controller.active++
	controller.maxActive = max(controller.maxActive, controller.active)
	controller.mu.Unlock()
	defer func() {
		controller.mu.Lock()
		controller.active--
		controller.mu.Unlock()
	}()
	if controller.started != nil {
		select {
		case controller.started <- id:
		case <-ctx.Done():
			return device.SMSListing{}, ctx.Err()
		}
	}
	if controller.listHandler != nil {
		return controller.listHandler(ctx, id)
	}
	return controller.listing, nil
}

func (controller *smsSyncTestController) recordedCalls() ([]smsSyncTestCall, int) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return append([]smsSyncTestCall(nil), controller.calls...), controller.maxActive
}

type smsEventTestController struct {
	*smsSyncTestController
	events       <-chan device.SMSReceivedEvent
	subscribeErr error
	subscribed   chan struct{}
}

func (controller *smsEventTestController) SubscribeSMSReceivedEvents(context.Context) (<-chan device.SMSReceivedEvent, error) {
	if controller.subscribed != nil {
		close(controller.subscribed)
	}
	return controller.events, controller.subscribeErr
}

func smsSyncTestPhysical(id, imei string) device.Device {
	return device.Device{
		ID: id, Discovered: true,
		Snapshot: &device.Snapshot{DeviceID: id, IMEI: imei, ICCID: "iccid", IMSI: "imsi"},
	}
}

func newSMSSyncTestServer(t *testing.T, configs ...store.Device) (*Server, *smsSyncTestController) {
	t.Helper()
	database, err := store.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	controller := &smsSyncTestController{started: make(chan string, 128)}
	for _, config := range configs {
		if config.Name == "" {
			config.Name = config.ID
		}
		if err := database.UpsertDevice(context.Background(), config); err != nil {
			t.Fatal(err)
		}
		controller.entries = append(controller.entries, smsSyncTestPhysical(config.ID, config.ModemIMEI))
	}
	return &Server{store: database, devices: controller, logger: regionTestLogger()}, controller
}

func receiveSMSValue[T any](t *testing.T, values <-chan T) T {
	t.Helper()
	select {
	case value := <-values:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for SMS test barrier")
		var zero T
		return zero
	}
}

func sendSMSValue[T any](t *testing.T, values chan<- T, value T) {
	t.Helper()
	select {
	case values <- value:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out sending SMS test barrier")
	}
}

func startSMSSyncTestLoop(t *testing.T, server *Server, ticks <-chan time.Time) (context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.runSMSSyncLoop(ctx, ticks)
	}()
	t.Cleanup(func() {
		cancel()
		receiveSMSValue(t, done)
	})
	return cancel, done
}

// An unbuffered invalid indication acknowledges that all preceding indications
// have been added by the collector, without introducing another scan.
func flushSMSCollector(t *testing.T, events chan<- device.SMSReceivedEvent) {
	t.Helper()
	sendSMSValue(t, events, device.SMSReceivedEvent{})
}

func blockSMSListing(release <-chan struct{}) func(context.Context, string) (device.SMSListing, error) {
	return func(ctx context.Context, _ string) (device.SMSListing, error) {
		select {
		case <-release:
			return device.SMSListing{}, nil
		case <-ctx.Done():
			return device.SMSListing{}, ctx.Err()
		}
	}
}

func TestSMSSyncPendingCoalescesAndBoundsDirtyIDs(t *testing.T) {
	pending := newSMSSyncPending()
	if pending.add("", false) {
		t.Fatal("empty ID queued a scan")
	}
	for index := range smsSyncDirtyLimit {
		id := fmt.Sprintf("physical-%d", index)
		if !pending.add(id, false) || pending.add(id, false) {
			t.Fatalf("failed to coalesce %q", id)
		}
	}
	if len(pending.wake) != 1 || cap(pending.wake) != 1 {
		t.Fatalf("wake length/capacity = %d/%d", len(pending.wake), cap(pending.wake))
	}
	<-pending.wake
	batch := pending.take()
	if batch.fullScan || len(batch.physicalIDs) != smsSyncDirtyLimit {
		t.Fatalf("bounded batch = %+v", batch)
	}
	pending.add("during-scan", false)
	if _, changed := batch.physicalIDs["during-scan"]; changed {
		t.Fatal("new indication mutated the claimed batch")
	}
	for index := range smsSyncDirtyLimit {
		pending.add(fmt.Sprintf("overflow-%d", index), false)
	}
	batch = pending.take()
	if !batch.fullScan || len(batch.physicalIDs) != 0 {
		t.Fatalf("overflow did not promote to full scan: %+v", batch)
	}
	pending.add("after-full-claim", false)
	batch = pending.take()
	if batch.fullScan || len(batch.physicalIDs) != 1 {
		t.Fatalf("indication during full scan was lost: %+v", batch)
	}
	pending.add("before-full", false)
	pending.add("", true)
	pending.add("covered-by-pending-full", false)
	batch = pending.take()
	if !batch.fullScan || len(batch.physicalIDs) != 0 {
		t.Fatalf("full scan did not cover older dirty IDs: %+v", batch)
	}
	if batch = pending.take(); batch.fullScan || len(batch.physicalIDs) != 0 {
		t.Fatalf("claim did not clear pending work: %+v", batch)
	}
}

func TestSMSCollectorIgnoresInvalidIDsAndHonorsExplicitFullScan(t *testing.T) {
	server, controller := newSMSSyncTestServer(t, store.Device{ID: "physical"})
	controller.entries = append(controller.entries, device.Device{ID: "gone"})
	events := make(chan device.SMSReceivedEvent)
	ticks := make(chan time.Time)
	pending := newSMSSyncPending()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.collectSMSSyncTriggers(ctx, ticks, events, pending)
	}()
	defer func() { cancel(); receiveSMSValue(t, done) }()
	for _, id := range []string{"", "  ", "gone", "unknown", "all"} {
		sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: id})
	}
	flushSMSCollector(t, events)
	if batch := pending.take(); batch.fullScan || len(batch.physicalIDs) != 0 {
		t.Fatalf("invalid IDs queued work: %+v", batch)
	}
	sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: " physical "})
	flushSMSCollector(t, events)
	if batch := pending.take(); batch.fullScan || len(batch.physicalIDs) != 1 {
		t.Fatalf("valid ID was ignored: %+v", batch)
	}
	sendSMSValue(t, events, device.SMSReceivedEvent{RescanAll: true})
	flushSMSCollector(t, events)
	if batch := pending.take(); !batch.fullScan {
		t.Fatal("explicit RescanAll was ignored")
	}
	close(events)
	sendSMSValue(t, ticks, time.Now())
	// A second tick is a barrier for the first tick's pending update.
	sendSMSValue(t, ticks, time.Now())
	if batch := pending.take(); !batch.fullScan {
		t.Fatal("closed event channel disabled periodic scans")
	}
}

func TestSMSCollectorPrioritizesDueTickerUnderEventFlood(t *testing.T) {
	server, _ := newSMSSyncTestServer(t, store.Device{ID: "physical"})
	events := make(chan device.SMSReceivedEvent, 256)
	for range cap(events) {
		events <- device.SMSReceivedEvent{DeviceID: "physical"}
	}
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	pending := newSMSSyncPending()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.collectSMSSyncTriggers(ctx, ticks, events, pending)
	}()
	defer func() { cancel(); receiveSMSValue(t, done) }()
	receiveSMSValue(t, pending.wake)
	if batch := pending.take(); !batch.fullScan {
		t.Fatalf("due ticker lost priority to indications: %+v", batch)
	}
}

func TestStartSMSSyncLoopEventReadsBeforeLongTicker(t *testing.T) {
	server, controller := newSMSSyncTestServer(t, store.Device{ID: "configured", ModemIMEI: "imei"})
	controller.entries = []device.Device{smsSyncTestPhysical("physical", "imei")}
	controller.listing = device.SMSListing{Messages: []device.SMSMessage{{
		Index: 4, Storage: "SM", StorageStatus: device.SMSStatusReceivedUnread,
		Direction: device.SMSDirectionReceived, From: "+447700900123", Text: "hello", RawPDU: "pdu",
	}}}
	scans := 0
	controller.listHandler = func(ctx context.Context, _ string) (device.SMSListing, error) {
		scans++
		if scans > 1 {
			<-ctx.Done()
			return device.SMSListing{}, ctx.Err()
		}
		return controller.listing, nil
	}
	events := make(chan device.SMSReceivedEvent)
	server.devices = &smsEventTestController{smsSyncTestController: controller, events: events}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.StartSMSSyncLoop(ctx, time.Hour)
	}()
	defer func() { cancel(); receiveSMSValue(t, done) }()
	sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "physical"})
	if id := receiveSMSValue(t, controller.started); id != "physical" {
		t.Fatalf("scanned %q, want physical", id)
	}
	// The next scan cannot start until the first one has persisted its result.
	sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "physical"})
	receiveSMSValue(t, controller.started)
	cancel()
	receiveSMSValue(t, done)
	messages, err := server.store.ListSMSMessages(context.Background(), store.SMSFilter{})
	if err != nil || len(messages) != 1 || messages[0].DeviceID != "configured" || messages[0].Body != "hello" {
		t.Fatalf("event persistence = %#v, %v", messages, err)
	}
	calls, maxActive := controller.recordedCalls()
	if len(calls) != 4 || maxActive != 1 {
		t.Fatalf("calls = %+v, max concurrent = %d", calls, maxActive)
	}
}

func TestSMSSyncLoopRetainsAndCoalescesEventsDuringScan(t *testing.T) {
	server, controller := newSMSSyncTestServer(t, store.Device{ID: "physical"})
	release := make(chan struct{})
	controller.listHandler = blockSMSListing(release)
	events := make(chan device.SMSReceivedEvent)
	server.devices = &smsEventTestController{smsSyncTestController: controller, events: events}
	cancel, done := startSMSSyncTestLoop(t, server, nil)
	sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "physical"})
	receiveSMSValue(t, controller.started)
	for range 100 {
		sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "physical"})
	}
	flushSMSCollector(t, events)
	sendSMSValue(t, release, struct{}{})
	receiveSMSValue(t, controller.started)
	cancel()
	receiveSMSValue(t, done)
	calls, maxActive := controller.recordedCalls()
	if len(calls) != 4 || maxActive != 1 {
		t.Fatalf("calls = %+v, max concurrent = %d", calls, maxActive)
	}
}

func TestSMSSyncLoopFullScanDoesNotEraseEventsDuringScan(t *testing.T) {
	server, controller := newSMSSyncTestServer(t, store.Device{ID: "a"}, store.Device{ID: "b"})
	release := make(chan struct{})
	controller.listHandler = blockSMSListing(release)
	events := make(chan device.SMSReceivedEvent)
	ticks := make(chan time.Time)
	server.devices = &smsEventTestController{smsSyncTestController: controller, events: events}
	cancel, done := startSMSSyncTestLoop(t, server, ticks)
	sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "a"})
	if got := receiveSMSValue(t, controller.started); got != "a" {
		t.Fatalf("first scan = %q", got)
	}
	sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "b"})
	sendSMSValue(t, ticks, time.Now()) // Supersedes the pending b-only scan.
	flushSMSCollector(t, events)
	sendSMSValue(t, release, struct{}{})
	if got := receiveSMSValue(t, controller.started); got != "a" {
		t.Fatalf("full scan first device = %q", got)
	}
	sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "b"})
	flushSMSCollector(t, events)
	sendSMSValue(t, release, struct{}{})
	if got := receiveSMSValue(t, controller.started); got != "b" {
		t.Fatalf("full scan second device = %q", got)
	}
	sendSMSValue(t, release, struct{}{})
	if got := receiveSMSValue(t, controller.started); got != "b" {
		t.Fatalf("post-full-scan indication = %q", got)
	}
	cancel()
	receiveSMSValue(t, done)
	calls, maxActive := controller.recordedCalls()
	if len(calls) != 8 || maxActive != 1 {
		t.Fatalf("calls = %+v, max concurrent = %d", calls, maxActive)
	}
}

func TestSMSSyncLoopPeriodicFallback(t *testing.T) {
	for _, mode := range []string{"no-interface", "subscription-error", "closed", "silent"} {
		t.Run(mode, func(t *testing.T) {
			server, controller := newSMSSyncTestServer(t, store.Device{ID: "physical"})
			events := make(chan device.SMSReceivedEvent, 1)
			eventController := &smsEventTestController{smsSyncTestController: controller, events: events}
			switch mode {
			case "subscription-error":
				eventController.subscribeErr = errors.New("unsupported")
				events <- device.SMSReceivedEvent{DeviceID: "physical"}
			case "closed":
				close(events)
			}
			if mode != "no-interface" {
				server.devices = eventController
			}
			ticks := make(chan time.Time)
			cancel, done := startSMSSyncTestLoop(t, server, ticks)
			for range 2 {
				sendSMSValue(t, ticks, time.Now())
				receiveSMSValue(t, controller.started)
			}
			cancel()
			receiveSMSValue(t, done)
			calls, maxActive := controller.recordedCalls()
			if len(calls) != 4 || maxActive != 1 {
				t.Fatalf("calls = %+v, max concurrent = %d", calls, maxActive)
			}
			if mode == "subscription-error" && len(events) != 1 {
				t.Fatal("consumed events from a failed subscription")
			}
		})
	}
}

func TestSMSSyncLoopCancellationDiscardsQueuedWork(t *testing.T) {
	for _, blocked := range []string{"sync-lock", "listing"} {
		t.Run(blocked, func(t *testing.T) {
			server, controller := newSMSSyncTestServer(t, store.Device{ID: "physical"})
			controller.listHandler = blockSMSListing(nil)
			events := make(chan device.SMSReceivedEvent)
			server.devices = &smsEventTestController{smsSyncTestController: controller, events: events}
			if blocked == "sync-lock" {
				server.smsSyncMu.Lock()
			}
			cancel, done := startSMSSyncTestLoop(t, server, nil)
			sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "physical"})
			if blocked == "listing" {
				receiveSMSValue(t, controller.started)
			}
			for range 10 {
				sendSMSValue(t, events, device.SMSReceivedEvent{DeviceID: "physical"})
			}
			flushSMSCollector(t, events)
			cancel()
			if blocked == "sync-lock" {
				server.smsSyncMu.Unlock()
			}
			receiveSMSValue(t, done)
			calls, _ := controller.recordedCalls()
			want := 0
			if blocked == "listing" {
				want = 2
			}
			if len(calls) != want {
				t.Fatalf("calls after cancellation = %+v, want %d", calls, want)
			}
			for _, call := range calls {
				if call.ctxErr != nil {
					t.Fatalf("started modem operation with canceled context: %+v", call)
				}
			}
		})
	}
}

func TestSMSSyncLoopAlreadyCanceledDoesNotSubscribe(t *testing.T) {
	server, controller := newSMSSyncTestServer(t, store.Device{ID: "physical"})
	subscribed := make(chan struct{})
	server.devices = &smsEventTestController{smsSyncTestController: controller, subscribed: subscribed}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.StartSMSSyncLoop(ctx, time.Hour)
	select {
	case <-subscribed:
		t.Fatal("subscribed after cancellation")
	default:
	}
}

func TestSyncModemSMSPhysicalMappingPreservesConfigOrderAndOwnership(t *testing.T) {
	server, controller := newSMSSyncTestServer(t,
		store.Device{ID: "alias-last", Name: "C", ModemIMEI: "shared"},
		store.Device{ID: "other", Name: "B", ModemIMEI: "other"},
		store.Device{ID: "alias-first", Name: "A", ModemIMEI: "shared"},
	)
	controller.entries = []device.Device{smsSyncTestPhysical("physical", "shared"), smsSyncTestPhysical("other-physical", "other")}
	controller.listing = device.SMSListing{
		Storage: device.SMSStorageUsage{SM: device.SMSStorageArea{Total: 20}},
		Messages: []device.SMSMessage{{
			Index: 4, Storage: "SM", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: "+447700900123", Text: "shared message", RawPDU: "pdu",
		}},
	}
	ctx := context.Background()
	if err := developer.SetAutoClearModemStorage(ctx, server.store, false); err != nil {
		t.Fatal(err)
	}
	server.syncModemSMSPhysical(ctx, map[string]struct{}{"physical": {}})
	calls, _ := controller.recordedCalls()
	if len(calls) != 4 {
		t.Fatalf("shared physical scan did not process both configs: %+v", calls)
	}
	for _, call := range calls {
		if call.physicalID != "physical" {
			t.Fatalf("unrelated physical device was scanned: %+v", calls)
		}
	}
	for _, id := range []string{"alias-first", "alias-last"} {
		if _, ok := server.smsStorageUsage(id); !ok {
			t.Fatalf("shared config %q was not processed", id)
		}
	}
	if _, ok := server.smsStorageUsage("other"); ok {
		t.Fatal("physical filter processed unrelated config")
	}
	messages, err := server.store.ListSMSMessages(ctx, store.SMSFilter{})
	// With auto-clear disabled, the existing persistence path updates DeviceID
	// on each config's rescan, so the last alias retains ownership as before.
	if err != nil || len(messages) != 1 || messages[0].DeviceID != "alias-last" {
		t.Fatalf("existing last-config ownership changed: %+v, %v", messages, err)
	}
	server.syncModemSMS(ctx, "physical") // Existing API accepts config IDs, not physical IDs.
	after, _ := controller.recordedCalls()
	if len(after) != len(calls) {
		t.Fatal("configured ID entry point accepted a physical ID")
	}
	server.syncModemSMS(ctx, "alias-last")
	after, _ = controller.recordedCalls()
	if len(after) != len(calls)+2 {
		t.Fatalf("configured ID filter did not select one config: %+v", after)
	}
	controller.mu.Lock()
	controller.calls = nil
	controller.mu.Unlock()
	server.syncModemSMSPhysical(ctx, map[string]struct{}{"physical": {}, "other-physical": {}})
	after, _ = controller.recordedCalls()
	var listed []string
	for _, call := range after {
		if call.kind == "list" {
			listed = append(listed, call.physicalID)
		}
	}
	if !reflect.DeepEqual(listed, []string{"physical", "other-physical", "physical"}) {
		t.Fatalf("configuration order = %v", listed)
	}
}

func TestSyncModemSMSPhysicalResolvesMappingAfterLock(t *testing.T) {
	server, controller := newSMSSyncTestServer(t, store.Device{ID: "configured", ModemIMEI: "old-imei"})
	controller.entries = []device.Device{smsSyncTestPhysical("physical", "old-imei")}
	server.smsSyncMu.Lock()
	started := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(started)
		defer close(done)
		server.syncModemSMSPhysical(context.Background(), map[string]struct{}{"physical": {}})
	}()
	receiveSMSValue(t, started)
	// The pending event must not retain the mapping from before a profile/config
	// operation holding the same synchronization lock.
	err := server.store.UpsertDevice(context.Background(), store.Device{ID: "configured", Name: "configured", ModemIMEI: "new-imei"})
	server.smsSyncMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	receiveSMSValue(t, done)
	if calls, _ := controller.recordedCalls(); len(calls) != 0 {
		t.Fatalf("scanned stale physical mapping: %+v", calls)
	}
}

type smsSyncVoWiFiController struct {
	fakeVoWiFiController
	blocked bool
}

func (controller *smsSyncVoWiFiController) ModemSMSSyncBlocked(string) bool {
	return controller.blocked
}

func TestSyncModemSMSPhysicalKeepsGuardsAndRetriesCNMI(t *testing.T) {
	for _, test := range []struct {
		name       string
		deviceType string
		atPort     string
		state      vowifi.State
		blocked    bool
		missing    bool
		setupFails bool
		wantScan   bool
	}{
		{name: "ec20", deviceType: store.DeviceTypePCIeEC20EC25, wantScan: true},
		{name: "ml307", deviceType: store.DeviceTypeML307, wantScan: true},
		{name: "dji", deviceType: store.DeviceTypeDJI4G, wantScan: true},
		{name: "410-with-at", deviceType: store.DeviceTypeWiFi410, atPort: "/dev/ttyUSB0", wantScan: true},
		{name: "410-no-at", deviceType: store.DeviceTypeWiFi410},
		{name: "pcsc", deviceType: store.DeviceTypeUSBSIMReader},
		{name: "missing", missing: true},
		{name: "vowifi-setup", state: vowifi.State{Enabled: true, Phase: vowifi.PhaseSIMReady}},
		{name: "vowifi-failed-busy", state: vowifi.State{Enabled: true, Phase: vowifi.PhaseFailed}, blocked: true},
		{name: "vowifi-failed-idle", state: vowifi.State{Enabled: true, Phase: vowifi.PhaseFailed}, wantScan: true},
		{name: "vowifi-stable", state: vowifi.State{Enabled: true, Phase: vowifi.PhaseSMSReady}, wantScan: true},
		{name: "cnmi-error", setupFails: true, wantScan: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// SMSEnabled deliberately remains false: it was never a scan filter.
			server, controller := newSMSSyncTestServer(t, store.Device{ID: "physical", DeviceType: test.deviceType, ATPort: test.atPort})
			if test.missing {
				controller.entries = nil
			}
			server.vowifi = &smsSyncVoWiFiController{fakeVoWiFiController: fakeVoWiFiController{state: test.state}, blocked: test.blocked}
			if test.setupFails {
				controller.setupHandler = func(context.Context, string) error { return errors.New("CNMI unsupported") }
			}
			for range 2 {
				server.syncModemSMSPhysical(context.Background(), map[string]struct{}{"physical": {}})
			}
			calls, _ := controller.recordedCalls()
			want := 0
			if test.wantScan {
				want = 4
			}
			if len(calls) != want {
				t.Fatalf("calls = %+v, want %d", calls, want)
			}
			for index, call := range calls {
				if index%2 == 0 && (call.kind != "setup" || call.command != "AT+CNMI=2,1,0,0,0" || call.deadline <= 0 || call.deadline > 5*time.Second) {
					t.Fatalf("CNMI preparation = %+v", call)
				}
				if index%2 == 1 && call.kind != "list" {
					t.Fatalf("scan did not follow preparation: %+v", call)
				}
			}
		})
	}
}

func TestSyncModemSMSCanceledCNMISkipsListing(t *testing.T) {
	server, controller := newSMSSyncTestServer(t, store.Device{ID: "physical"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	controller.setupHandler = func(context.Context, string) error {
		cancel()
		return context.Canceled
	}
	server.syncModemSMSPhysical(ctx, map[string]struct{}{"physical": {}})
	if calls, _ := controller.recordedCalls(); len(calls) != 1 || calls[0].kind != "setup" {
		t.Fatalf("started listing after CNMI cancellation: %+v", calls)
	}
}
