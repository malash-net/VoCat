package server

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/device"
	"vocat/internal/modem"
	"vocat/internal/store"
)

// countingSMSController counts ListSMS calls to show a burst coalesces.
type countingSMSController struct {
	fakeDeviceController
	mu    sync.Mutex
	calls int
}

func (controller *countingSMSController) ListSMS(ctx context.Context, id string) (device.SMSListing, error) {
	controller.mu.Lock()
	controller.calls++
	controller.mu.Unlock()
	return controller.fakeDeviceController.ListSMS(ctx, id)
}

func (controller *countingSMSController) listCalls() int {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return controller.calls
}

func newSMSArrivalServer(t *testing.T, entry device.Device, controller DeviceController) *Server {
	t.Helper()
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.UpsertDevice(ctx, store.Device{
		ID: entry.ID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	server := &Server{store: database, logger: regionTestLogger()}
	server.devices = controller
	return server
}

func TestSMSArrivalEventTriggersTargetedSync(t *testing.T) {
	const deviceID = "ec20-1"
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	controller := &countingSMSController{fakeDeviceController: fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: "867394042309830", ICCID: "iccid-a", IMSI: "imsi-a",
		}},
		smsMessages: []device.SMSMessage{{
			Index: 7, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: "JETPAC", Text: "hello",
			ServiceCenterTimestamp: &receivedAt, RawPDU: "001122334455",
		}},
	}}
	server := newSMSArrivalServer(t, controller.entry, controller)
	server.syncModemSMSForPhysicalDevices(context.Background(), map[string]struct{}{deviceID: {}})

	messages, err := server.store.ListSMSMessages(context.Background(), store.SMSFilter{DeviceID: deviceID})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].Body != "hello" {
		t.Fatalf("stored messages = %#v", messages)
	}
}

// The indication carries a physical ID while syncModemSMS matches on the stored
// config ID, so the resolver has to bridge the two.
func TestSMSArrivalEventResolvesPhysicalIDToConfigID(t *testing.T) {
	const (
		configID   = "ec20"
		physicalID = "usb-1-2"
		imei       = "867394042309830"
	)
	ctx := context.Background()
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	controller := &countingSMSController{fakeDeviceController: fakeDeviceController{
		entry: device.Device{ID: physicalID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: physicalID, IMEI: imei, ICCID: "iccid-a", IMSI: "imsi-a",
		}},
		smsMessages: []device.SMSMessage{{
			Index: 7, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: "JETPAC", Text: "hello",
			ServiceCenterTimestamp: &receivedAt, RawPDU: "001122334455",
		}},
	}}
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	// The config ID and the physical ID differ; only the IMEI links them.
	if err := database.UpsertDevice(ctx, store.Device{
		ID: configID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	server := &Server{store: database, logger: regionTestLogger()}
	server.devices = controller

	server.syncModemSMSForPhysicalDevices(ctx, map[string]struct{}{physicalID: {}})

	messages, err := database.ListSMSMessages(ctx, store.SMSFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("stored messages = %#v", messages)
	}
	if messages[0].DeviceID != configID {
		t.Fatalf("message device ID = %q, want the config ID %q", messages[0].DeviceID, configID)
	}
}

func TestSMSArrivalEventUnresolvedFallsBackToFullScan(t *testing.T) {
	const deviceID = "ec20-1"
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	controller := &countingSMSController{fakeDeviceController: fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: "867394042309830", ICCID: "iccid-a", IMSI: "imsi-a",
		}},
		smsMessages: []device.SMSMessage{{
			Index: 7, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: "JETPAC", Text: "hello",
			ServiceCenterTimestamp: &receivedAt, RawPDU: "001122334455",
		}},
	}}
	server := newSMSArrivalServer(t, controller.entry, controller)
	server.syncModemSMSForPhysicalDevices(context.Background(), map[string]struct{}{"unknown-physical": {}})

	if calls := controller.listCalls(); calls == 0 {
		t.Fatal("unresolved indication did not trigger a fallback scan")
	}
	messages, err := server.store.ListSMSMessages(context.Background(), store.SMSFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 {
		t.Fatalf("stored messages = %#v", messages)
	}
}

// A burst must collapse into one scan pass instead of stacking six AT commands
// per indication behind the global SMS lock.
func TestSMSArrivalEventBurstCoalesces(t *testing.T) {
	const deviceID = "ec20-1"
	controller := &countingSMSController{fakeDeviceController: fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: "867394042309830", ICCID: "iccid-a", IMSI: "imsi-a",
		}},
	}}
	server := newSMSArrivalServer(t, controller.entry, controller)
	events := make(chan string, 1)
	controller.smsEvents = events
	// Point the reconciler at the same controller so the debounce path is
	// exercised end to end.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.runSMSArrivalReconciler(ctx)

	for index := 0; index < 20; index++ {
		events <- deviceID
	}
	// The debounce window plus one scan must fit well inside this budget.
	waitForCondition(t, func() bool { return controller.listCalls() > 0 })
	time.Sleep(4 * smsArrivalDebounce)
	if calls := controller.listCalls(); calls > 1 {
		t.Fatalf("burst produced %d scans, want 1", calls)
	}
}

func waitForCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before the deadline")
}

// capabilityLessController implements DeviceController but not the optional
// arrival-event interface; embedding the interface withholds the method.
type capabilityLessController struct {
	DeviceController
}

func TestSMSArrivalReconcilerWithoutCapability(t *testing.T) {
	server := &Server{logger: regionTestLogger()}
	server.devices = capabilityLessController{DeviceController: fakeDeviceController{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.runSMSArrivalReconciler(ctx)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciler did not return for a backend without the capability")
	}
}

func TestSMSArrivalReconcilerReturnsOnNilChannel(t *testing.T) {
	server := &Server{logger: regionTestLogger()}
	server.devices = fakeDeviceController{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.runSMSArrivalReconciler(ctx)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciler blocked on a nil event channel")
	}
}

// A modem that already defaults to 2,1,0,0,0 must not be rewritten: the write
// would be a no-op, and the scan stays clear of any URC routing change. Only
// the OpenStick 410, whose firmware resets CNMI, is reasserted.
func TestSyncModemSMSLeavesDefaultSMSIndicationAlone(t *testing.T) {
	const deviceID = "ec20-1"
	var commands []string
	controller := &countingSMSController{fakeDeviceController: fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: "867394042309830", ICCID: "iccid-a", IMSI: "imsi-a",
		}},
		atHandler: func(command string) (modem.Response, error) {
			commands = append(commands, command)
			return modem.Response{Final: "OK"}, nil
		},
	}}
	server := newSMSArrivalServer(t, controller.entry, controller)
	server.syncModemSMS(context.Background(), deviceID)

	for _, command := range commands {
		if strings.HasPrefix(strings.ToUpper(command), "AT+CNMI") {
			t.Fatalf("CNMI was rewritten on a modem that already defaults to it; commands = %#v", commands)
		}
	}
}
