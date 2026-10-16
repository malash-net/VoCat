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

// newSMSArrivalServer builds a server with one configured device backed by the
// given physical entry.
func newSMSArrivalServer(t *testing.T, entry device.Device, controller DeviceController) *Server {
	t.Helper()
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	server := &Server{store: database, logger: regionTestLogger()}
	server.devices = controller
	return server
}

func upsertArrivalDevice(t *testing.T, server *Server, config store.Device) {
	t.Helper()
	config.DeviceType = store.DeviceTypePCIeEC20EC25
	config.SMSEnabled = true
	if err := server.store.UpsertDevice(context.Background(), config); err != nil {
		t.Fatal(err)
	}
}

func arrivalMessage(receivedAt time.Time) device.SMSMessage {
	return device.SMSMessage{
		Index: 7, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
		Direction: device.SMSDirectionReceived, From: "JETPAC", Text: "hello",
		ServiceCenterTimestamp: &receivedAt, RawPDU: "001122334455",
	}
}

// The indication carries a physical device ID, while syncModemSMS matches on
// the stored config ID; the resolver must bridge the two.
func TestSMSArrivalEventScansMatchingDevice(t *testing.T) {
	const (
		configID   = "ultramobile"
		physicalID = "usb-1-2"
		imei       = "867394042309830"
	)
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	controller := &countingSMSController{fakeDeviceController: fakeDeviceController{
		entry: device.Device{ID: physicalID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: physicalID, IMEI: imei, ICCID: "iccid-a", IMSI: "imsi-a",
		}},
		smsMessages: []device.SMSMessage{arrivalMessage(receivedAt)},
	}}
	server := newSMSArrivalServer(t, controller.entry, controller)
	upsertArrivalDevice(t, server, store.Device{ID: configID, Name: "EC20", ModemIMEI: imei})

	server.syncModemSMSForPhysicalDevices(context.Background(), map[string]struct{}{physicalID: {}})

	messages, err := server.store.ListSMSMessages(context.Background(), store.SMSFilter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].DeviceID != configID {
		t.Fatalf("stored messages = %#v", messages)
	}
}

// A burst must collapse into one scan pass rather than one per indication.
func TestSMSArrivalEventBurstCoalesces(t *testing.T) {
	const deviceID = "ultramobile"
	controller := &countingSMSController{fakeDeviceController: fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: "867394042309830", ICCID: "iccid-a", IMSI: "imsi-a",
		}},
	}}
	server := newSMSArrivalServer(t, controller.entry, controller)
	upsertArrivalDevice(t, server, store.Device{ID: deviceID, Name: "EC20"})
	events := make(chan string, 1)
	controller.smsEvents = events

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go server.runSMSArrivalReconciler(ctx)

	for index := 0; index < 20; index++ {
		events <- deviceID
	}
	deadline := time.Now().Add(2 * time.Second)
	for controller.listCalls() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(4 * smsArrivalThrottle)
	if calls := controller.listCalls(); calls != 1 {
		t.Fatalf("burst produced %d scans, want 1", calls)
	}
}

// The reconciler must return rather than block when the backend cannot supply
// an event channel.
func TestSMSArrivalReconcilerReturnsWithoutEvents(t *testing.T) {
	server := &Server{logger: regionTestLogger()}
	server.devices = fakeDeviceController{}
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.runSMSArrivalReconciler(context.Background())
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("reconciler blocked on a nil event channel")
	}
}

// A modem already defaulting to 2,1,0,0,0 must not be rewritten: only the
// OpenStick 410, whose firmware resets CNMI, is reasserted.
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
	upsertArrivalDevice(t, server, store.Device{ID: deviceID, Name: "EC20"})
	server.syncModemSMS(context.Background(), deviceID)

	for _, command := range commands {
		if strings.HasPrefix(strings.ToUpper(command), "AT+CNMI") {
			t.Fatalf("CNMI was rewritten; commands = %#v", commands)
		}
	}
}
