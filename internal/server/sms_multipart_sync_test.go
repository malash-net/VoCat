package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"vocat/internal/developer"
	"vocat/internal/device"
	"vocat/internal/store"
)

type multipartSMSEventController struct {
	*smsDeletionController
	events  chan device.SMSReceivedEvent
	scanned chan struct{}
}

func (controller *multipartSMSEventController) SubscribeSMSReceivedEvents(context.Context) (<-chan device.SMSReceivedEvent, error) {
	return controller.events, nil
}

func (controller *multipartSMSEventController) ListSMS(ctx context.Context, id string) (device.SMSListing, error) {
	listing, err := controller.smsDeletionController.ListSMS(ctx, id)
	select {
	case controller.scanned <- struct{}{}:
	case <-ctx.Done():
		return device.SMSListing{}, ctx.Err()
	}
	return listing, err
}

func TestSyncModemSMSClearsReadyMessagesAlongsideIncompleteMultipart(t *testing.T) {
	const deviceID = "physical"
	ctx := context.Background()
	server, _ := newSMSSyncTestServer(t, store.Device{ID: deviceID, ModemIMEI: "imei"})
	part := func(index, reference, sequence int, body string) device.SMSMessage {
		return device.SMSMessage{
			Index: index, Storage: "SM", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: "+447700900123", Text: body, RawPDU: body,
			Concat: &device.SMSConcatInfo{Reference: reference, Total: 2, Sequence: sequence},
		}
	}
	plain := part(2, 0, 0, "plain")
	plain.Concat = nil
	controller := &smsDeletionController{
		fakeDeviceController: fakeDeviceController{entry: smsSyncTestPhysical(deviceID, "imei")},
		storedMessages: []device.SMSMessage{
			part(1, 7, 1, "pending "), plain,
			part(3, 8, 1, "complete "), part(4, 8, 2, "long"),
		},
	}
	server.devices = controller
	server.syncModemSMS(ctx, deviceID)
	controller.mu.Lock()
	remaining := append([]device.SMSMessage(nil), controller.storedMessages...)
	deleted := append([]string(nil), controller.deleted...)
	controller.mu.Unlock()
	if len(remaining) != 1 || remaining[0].Index != 1 || len(deleted) != 3 {
		t.Fatalf("cleanup retained the wrong slots: remaining=%#v, deleted=%v", remaining, deleted)
	}
	messages, err := server.store.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(messages) != 3 {
		t.Fatalf("stored messages = %#v, %v", messages, err)
	}
	ready := 0
	for _, message := range messages {
		if store.ConcatSMSReadyToNotify(message.MessageID, message.Extra) {
			ready++
		}
	}
	if ready != 2 {
		t.Fatalf("ready messages = %d, want plain and completed multipart", ready)
	}
	cursor, err := server.store.LatestSMSMessageID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server.syncModemSMS(ctx, deviceID)
	fresh, err := server.store.ListInboundSMSAfterID(ctx, cursor, 10)
	if err != nil || len(fresh) != 0 {
		t.Fatalf("incomplete rescan changed the notification cursor: %#v, %v", fresh, err)
	}
	controller.mu.Lock()
	controller.storedMessages = append(controller.storedMessages, part(5, 7, 2, "done"))
	controller.mu.Unlock()
	server.syncModemSMS(ctx, deviceID)
	fresh, err = server.store.ListInboundSMSAfterID(ctx, cursor, 10)
	if err != nil || len(fresh) != 1 || fresh[0].Body != "pending done" || !store.ConcatSMSReadyToNotify(fresh[0].MessageID, fresh[0].Extra) {
		t.Fatalf("completed notification = %#v, %v", fresh, err)
	}
	controller.mu.Lock()
	remainingCount, deletedCount := len(controller.storedMessages), len(controller.deleted)
	controller.mu.Unlock()
	if remainingCount != 0 || deletedCount != 5 {
		t.Fatalf("completed cleanup: remaining=%d, deleted=%d", remainingCount, deletedCount)
	}
}

func TestSMSEventsPreserveMultipartStorageUntilComplete(t *testing.T) {
	for _, test := range []struct {
		name        string
		batch       bool
		restart     bool
		keepSlots   bool
		dualStorage bool
	}{
		{name: "single scan", batch: true},
		{name: "separate events"},
		{name: "restart between segments", restart: true},
		{name: "automatic cleanup disabled", keepSlots: true},
		{name: "duplicate SM and ME copies", dualStorage: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			path := filepath.Join(t.TempDir(), "vocat.db")
			database, err := store.Open(ctx, path)
			if err != nil {
				t.Fatal(err)
			}
			const deviceID = "physical"
			if err := database.UpsertDevice(ctx, store.Device{ID: deviceID, Name: "modem", ModemIMEI: "imei", SMSEnabled: true}); err != nil {
				t.Fatal(err)
			}
			if test.keepSlots {
				if err := developer.SetAutoClearModemStorage(ctx, database, false); err != nil {
					t.Fatal(err)
				}
			}
			controller := &multipartSMSEventController{
				smsDeletionController: &smsDeletionController{fakeDeviceController: fakeDeviceController{
					entry: smsSyncTestPhysical(deviceID, "imei"),
				}},
				events: make(chan device.SMSReceivedEvent), scanned: make(chan struct{}),
			}
			server := &Server{store: database, devices: controller, logger: regionTestLogger()}
			t.Cleanup(func() { _ = server.store.Close() })
			cancel, done := startSMSSyncTestLoop(t, server, nil)
			scan := func() {
				t.Helper()
				sendSMSValue(t, controller.events, device.SMSReceivedEvent{DeviceID: deviceID})
				receiveSMSValue(t, controller.scanned)
				// ListSMS is called under smsSyncMu; acquiring it after the listing
				// signal waits for both persistence and actual modem slot deletion.
				server.smsSyncMu.Lock()
				server.smsSyncMu.Unlock()
			}
			receivedAt := time.Unix(1_700_000_000, 0).UTC()
			parts := make([]device.SMSMessage, 0, 3)
			for index, body := range []string{"first ", "middle ", "last"} {
				parts = append(parts, device.SMSMessage{
					Index: 1 + index*4, Storage: "SM", StorageStatus: device.SMSStatusReceivedUnread,
					Direction: device.SMSDirectionReceived, From: "+447700900123", Text: body,
					Encoding: device.SMSEncodingGSM7PDU, RawPDU: body, ServiceCenterTimestamp: &receivedAt,
					Concat: &device.SMSConcatInfo{Reference: 23, Total: 3, Sequence: index + 1},
				})
			}
			var cursor int64
			var messageID string
			check := func(count int) {
				t.Helper()
				messages, err := server.store.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
				if err != nil || len(messages) != 1 {
					t.Fatalf("after %d segments: messages = %#v, %v", count, messages, err)
				}
				message := messages[0]
				if messageID != "" && message.MessageID != messageID {
					t.Fatalf("multipart identity changed: %q -> %q", messageID, message.MessageID)
				}
				messageID = message.MessageID
				complete := count == len(parts)
				if ready := store.ConcatSMSReadyToNotify(message.MessageID, message.Extra); ready != complete {
					t.Fatalf("after %d segments: ready = %t, want %t", count, ready, complete)
				}
				if complete && message.Body != "first middle last" {
					t.Fatalf("assembled body = %q", message.Body)
				}
				fresh, err := server.store.ListInboundSMSAfterID(ctx, cursor, 10)
				if err != nil || len(fresh) != 1 || fresh[0].ID != message.ID {
					t.Fatalf("notification cursor did not advance: %#v, %v", fresh, err)
				}
				cursor = message.ID
				controller.mu.Lock()
				remaining, deleted := len(controller.storedMessages), len(controller.deleted)
				controller.mu.Unlock()
				wantRemaining, wantDeleted := count, 0
				if test.dualStorage {
					wantRemaining *= 2
				}
				if complete && !test.keepSlots {
					wantRemaining, wantDeleted = 0, wantRemaining
				}
				if remaining != wantRemaining || deleted != wantDeleted {
					t.Fatalf("after %d segments: remaining=%d deleted=%d, want %d/%d", count, remaining, deleted, wantRemaining, wantDeleted)
				}
			}
			if test.batch {
				controller.mu.Lock()
				controller.storedMessages = append(controller.storedMessages, parts...)
				controller.mu.Unlock()
				scan()
				check(len(parts))
			} else {
				for index, part := range parts {
					controller.mu.Lock()
					controller.storedMessages = append(controller.storedMessages, part)
					if test.dualStorage {
						duplicate := part
						duplicate.Storage = "ME"
						duplicate.RawPDU = "ME-" + part.RawPDU
						controller.storedMessages = append(controller.storedMessages, duplicate)
					}
					controller.mu.Unlock()
					scan()
					check(index + 1)
					if test.restart && index == 0 {
						cancel()
						receiveSMSValue(t, done)
						if err := server.store.Close(); err != nil {
							t.Fatal(err)
						}
						database, err = store.Open(ctx, path)
						if err != nil {
							t.Fatal(err)
						}
						server.store = database
						cancel, done = startSMSSyncTestLoop(t, server, nil)
					}
				}
			}
			scan()
			cancel()
			receiveSMSValue(t, done)
			fresh, err := server.store.ListInboundSMSAfterID(ctx, cursor, 10)
			if err != nil || len(fresh) != 0 {
				t.Fatalf("rescan repeated the completed notification: %#v, %v", fresh, err)
			}
		})
	}
}
