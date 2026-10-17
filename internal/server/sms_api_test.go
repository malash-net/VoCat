package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/developer"
	"vocat/internal/device"
	"vocat/internal/loghub"
	"vocat/internal/store"
)

type smsDeletionController struct {
	fakeDeviceController
	mu             sync.Mutex
	storedMessages []device.SMSMessage
	deleted        []string
}

func (controller *smsDeletionController) ListSMS(context.Context, string) (device.SMSListing, error) {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	return device.SMSListing{Messages: append([]device.SMSMessage(nil), controller.storedMessages...)}, nil
}

func (controller *smsDeletionController) DeleteSMSFromStorage(
	_ context.Context,
	_ string,
	storageName string,
	index int,
) error {
	controller.mu.Lock()
	defer controller.mu.Unlock()
	key := storageName + ":" + strconv.Itoa(index)
	controller.deleted = append(controller.deleted, key)
	remaining := controller.storedMessages[:0]
	for _, message := range controller.storedMessages {
		if message.Storage == storageName && message.Index == index {
			continue
		}
		remaining = append(remaining, message)
	}
	controller.storedMessages = remaining
	return nil
}

func TestSMSThreadAllDevicesUsesIMSIFilter(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	for index, imsi := range []string{"imsi-a", "imsi-b"} {
		if _, err := database.SaveSMSMessage(ctx, store.SMSMessage{
			MessageID: "message-" + imsi,
			DeviceID:  "ec20",
			IMSI:      imsi,
			Peer:      "VOXI",
			Direction: "inbound",
			Body:      imsi,
			Timestamp: time.Unix(1_700_000_000+int64(index), 0),
		}); err != nil {
			t.Fatal(err)
		}
	}

	server := &Server{store: database}
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/sms/thread?device_id=all&imsi=imsi-a&peer=VOXI",
		nil,
	)
	response := httptest.NewRecorder()
	server.handleSMSThread(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Data) != 1 || envelope.Data[0]["imsi"] != "imsi-a" {
		t.Fatalf("thread data = %#v", envelope.Data)
	}
}

func TestNative410DoesNotUseModemSMSStorage(t *testing.T) {
	if supportsModemSMSStorage(store.Device{DeviceType: store.DeviceTypeWiFi410}) {
		t.Fatal("native OpenStick 410 unexpectedly enabled modem SMS storage polling")
	}
	if !supportsModemSMSStorage(store.Device{DeviceType: store.DeviceTypePCIeEC20EC25}) {
		t.Fatal("EC20 modem SMS storage polling was disabled")
	}
}

func TestSyncModemSMSDoesNotRelabelStoredMessageAfterProfileSwitch(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	storedMessage := device.SMSMessage{
		Index: 7, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
		Direction: device.SMSDirectionReceived, From: "JETPAC", Text: "hello",
		ServiceCenterTimestamp: &receivedAt, RawPDU: "001122334455",
	}
	server := &Server{store: database, logger: regionTestLogger()}
	server.devices = fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: imei, ICCID: "iccid-a", IMSI: "imsi-a",
			Phone: device.PhoneNumber{Number: "+441111"},
		}},
		smsMessages: []device.SMSMessage{storedMessage},
	}
	server.syncModemSMS(ctx, deviceID)

	server.devices = fakeDeviceController{
		entry: device.Device{ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{
			DeviceID: deviceID, IMEI: imei, ICCID: "iccid-b", IMSI: "imsi-b",
			Phone: device.PhoneNumber{Number: "+442222"},
		}},
		smsMessages: []device.SMSMessage{storedMessage},
	}
	server.syncModemSMS(ctx, deviceID)

	messages, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil {
		t.Fatal(err)
	}
	if len(messages) != 1 || messages[0].ICCID != "iccid-a" ||
		messages[0].IMSI != "imsi-a" || messages[0].LocalPhone != "+441111" {
		t.Fatalf("message identity after profile B rescan = %#v", messages)
	}
}

func TestSMSThreadConfiguredDeviceUsesStableIMEI(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const imei = "867394042309830"
	if err := database.UpsertDevice(ctx, store.Device{
		ID: "ec20_2", Name: "EC20 renamed", ModemIMEI: imei,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SaveSMSMessage(ctx, store.SMSMessage{
		MessageID: "before-rename", DeviceID: "ec20_1", ModemIMEI: imei,
		IMSI: "imsi-a", Peer: "VOXI", Direction: "inbound", Body: "history",
	}); err != nil {
		t.Fatal(err)
	}

	server := &Server{store: database}
	request := httptest.NewRequest(
		http.MethodGet,
		"/api/sms/thread?device_id=ec20_2&imsi=imsi-a&peer=VOXI",
		nil,
	)
	response := httptest.NewRecorder()
	server.handleSMSThread(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	var envelope struct {
		Data []map[string]any `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if len(envelope.Data) != 1 || envelope.Data[0]["modem_imei"] != imei {
		t.Fatalf("thread data = %#v", envelope.Data)
	}
}

func TestNormalizeSMSDeviceFilter(t *testing.T) {
	if got := normalizeSMSDeviceFilter(" ALL "); got != "" {
		t.Fatalf("all filter = %q", got)
	}
	if got := normalizeSMSDeviceFilter("EC20"); got != "EC20" {
		t.Fatalf("device filter = %q", got)
	}
}

func TestSupportsModemSMSStorageRejectsUSBReader(t *testing.T) {
	if supportsModemSMSStorage(store.Device{DeviceType: store.DeviceTypeUSBSIMReader}) {
		t.Fatal("USB SIM reader must not be polled with modem SMS AT commands")
	}
	if !supportsModemSMSStorage(store.Device{DeviceType: store.DeviceTypePCIeEC20EC25}) {
		t.Fatal("cellular modem should retain modem SMS storage synchronization")
	}
}

func TestSyncModemSMSLogsMultipartMessageOnlyOnceAcrossStoragesAndPolls(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	messages := make([]device.SMSMessage, 0, 6)
	for _, storageName := range []string{"SM", "ME"} {
		for sequence, body := range []string{"part one ", "part two ", "part three"} {
			messages = append(messages, device.SMSMessage{
				Index:                  sequence + 1,
				Storage:                storageName,
				StorageStatus:          device.SMSStatusReceivedUnread,
				Direction:              device.SMSDirectionReceived,
				From:                   "+447700900123",
				Text:                   body,
				Encoding:               device.SMSEncodingGSM7PDU,
				ServiceCenterTimestamp: &receivedAt,
				Concat: &device.SMSConcatInfo{
					Reference: 23,
					Total:     3,
					Sequence:  sequence + 1,
				},
				RawPDU: storageName + body,
			})
		}
	}
	hub := loghub.New(slog.NewTextHandler(io.Discard, nil), 100)
	server := &Server{
		store:  database,
		logger: slog.New(hub),
		devices: fakeDeviceController{
			entry: device.Device{
				ID: deviceID, Discovered: true,
				Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: "23433"},
			},
			smsMessages: messages,
		},
	}

	server.syncModemSMS(ctx, deviceID)
	server.syncModemSMS(ctx, deviceID)

	receivedLogs := hub.History(100, slog.LevelInfo, "cellular SMS received")
	if len(receivedLogs) != 1 {
		t.Fatalf("sms.received logs = %d, want 1: %#v", len(receivedLogs), receivedLogs)
	}
	parts := receivedLogs[0].Fields["parts"]
	if receivedLogs[0].Fields["event"] != "sms.received" || (parts != int64(3) && parts != 3) {
		t.Fatalf("received log fields = %#v", receivedLogs[0].Fields)
	}
	stored, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil {
		t.Fatal(err)
	}
	if len(stored) != 1 || stored[0].Body != "part one part two part three" {
		t.Fatalf("stored messages = %#v", stored)
	}
}

func TestSyncModemSMSSeparatesReusedConcatReferencesWithoutCursorChurn(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
		peer     = "+447700900123"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	part := func(index, sequence int, body string) device.SMSMessage {
		return device.SMSMessage{
			Index: index, Storage: "SM", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: peer, Text: body,
			Encoding: device.SMSEncodingGSM7PDU, ServiceCenterTimestamp: &receivedAt,
			Concat: &device.SMSConcatInfo{Reference: 7, Total: 2, Sequence: sequence},
			RawPDU: body,
		}
	}
	messages := []device.SMSMessage{
		// Storage slot 21 and 43 contain unrelated messages that the modem did
		// not return in this filtered view. Multipart segments still belong to
		// the first segment's actual slot, rather than an inferred consecutive
		// slot number.
		part(20, 1, "old-a "), part(25, 2, "old-b"),
		part(42, 1, "new-a "), part(47, 2, "new-b"),
	}
	server := &Server{
		store: database, logger: regionTestLogger(),
		devices: fakeDeviceController{
			entry: device.Device{ID: deviceID, Discovered: true,
				Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: "23433"}},
			smsMessages: messages,
		},
	}

	server.syncModemSMS(ctx, deviceID)
	first, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(first) != 2 {
		t.Fatalf("first sync messages = %#v, %v", first, err)
	}
	latest, err := database.LatestSMSMessageID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	server.syncModemSMS(ctx, deviceID)
	second, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(second) != 2 {
		t.Fatalf("second sync messages = %#v, %v", second, err)
	}
	fresh, err := database.ListInboundSMSAfterID(ctx, latest, 10)
	if err != nil || len(fresh) != 0 {
		t.Fatalf("notification rows after repeated scan = %#v, %v", fresh, err)
	}
}

func TestDeleteSMSRemovesModemCopyBeforeDatabaseRow(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	modemMessages := make([]device.SMSMessage, 0, 6)
	for _, storageName := range []string{"SM", "ME"} {
		for sequence, body := range []string{"cloud offer ", "claim link ", "reply R"} {
			modemMessages = append(modemMessages, device.SMSMessage{
				Index: sequence + 1, Storage: storageName,
				StorageStatus: device.SMSStatusReceivedUnread,
				Direction:     device.SMSDirectionReceived,
				From:          "+447700900123", Text: body,
				Encoding:               device.SMSEncodingGSM7PDU,
				ServiceCenterTimestamp: &receivedAt,
				Concat: &device.SMSConcatInfo{
					Reference: 23, Total: 3, Sequence: sequence + 1,
				},
				RawPDU: storageName + body,
			})
		}
	}
	controller := &smsDeletionController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: deviceID, Discovered: true,
			Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: "23433"},
		}},
		storedMessages: modemMessages,
	}
	hub := loghub.New(slog.NewTextHandler(io.Discard, nil), 100)
	server := &Server{store: database, logger: slog.New(hub), devices: controller}
	server.syncModemSMS(ctx, deviceID)
	stored, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(stored) != 1 {
		t.Fatalf("initial stored messages = %#v, %v", stored, err)
	}
	deletedID := stored[0].ID
	controller.mu.Lock()
	syncDeleted := append([]string(nil), controller.deleted...)
	remainingAfterSync := len(controller.storedMessages)
	controller.mu.Unlock()
	sort.Strings(syncDeleted)
	wantDeleted := []string{"ME:1", "ME:2", "ME:3", "SM:1", "SM:2", "SM:3"}
	if strings.Join(syncDeleted, ",") != strings.Join(wantDeleted, ",") || remainingAfterSync != 0 {
		t.Fatalf("sync modem deletion = %v, remaining = %d", syncDeleted, remainingAfterSync)
	}

	request := httptest.NewRequest(http.MethodDelete, "/api/sms/messages/"+strconv.FormatInt(deletedID, 10), nil)
	response := httptest.NewRecorder()
	server.handleSMSMessage(response, request, strconv.FormatInt(deletedID, 10))
	if response.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body = %s", response.Code, response.Body.String())
	}

	server.syncModemSMS(ctx, deviceID)
	stored, err = database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(stored) != 0 {
		t.Fatalf("messages after delete and resync = %#v, %v", stored, err)
	}
	fresh, err := database.ListInboundSMSAfterID(ctx, deletedID, 10)
	if err != nil || len(fresh) != 0 {
		t.Fatalf("notification rows after delete and resync = %#v, %v", fresh, err)
	}
	if logs := hub.History(100, slog.LevelInfo, "cellular SMS received"); len(logs) != 1 {
		t.Fatalf("sms.received logs after delete and resync = %d, want 1", len(logs))
	}
}

func TestDeleteSMSWithoutDeviceConfig(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	message, err := database.SaveSMSMessage(ctx, store.SMSMessage{
		DeviceID: "removed-device", ModemIMEI: "867394042309830",
		MessageID: "modem:history", Source: "cellular_at",
		Peer: "VOXI", Direction: "inbound", Body: "history",
	})
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{store: database, devices: fakeDeviceController{}}
	if err := server.deleteSMSMessages(ctx, []store.SMSMessage{message}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SMSMessage(ctx, message.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("SMS after deletion: %v, want store.ErrNotFound", err)
	}
}

func TestSyncModemSMSClearsPersistedModemSlots(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	controller := &smsDeletionController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: deviceID, Discovered: true,
			Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: "23433"},
		}},
		storedMessages: []device.SMSMessage{{
			Index: 4, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: "+447700900123", Text: "hello",
			Encoding: device.SMSEncodingGSM7PDU, ServiceCenterTimestamp: &receivedAt, RawPDU: "ME-hello",
		}},
	}
	server := &Server{store: database, logger: regionTestLogger(), devices: controller}
	server.syncModemSMS(ctx, deviceID)
	controller.mu.Lock()
	remaining := len(controller.storedMessages)
	deleted := append([]string(nil), controller.deleted...)
	controller.mu.Unlock()
	if remaining != 0 || strings.Join(deleted, ",") != "ME:4" {
		t.Fatalf("remaining = %d, deleted = %v", remaining, deleted)
	}
	stored, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(stored) != 1 || stored[0].Body != "hello" {
		t.Fatalf("stored = %#v, %v", stored, err)
	}
}

func TestSyncModemSMSKeepsModemSlotsWhenAutoClearDisabled(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := developer.SetAutoClearModemStorage(ctx, database, false); err != nil {
		t.Fatal(err)
	}
	const deviceID = "ec20-1"
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: "867394042309830", SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	receivedAt := time.Unix(1_700_000_000, 0).UTC()
	controller := &smsDeletionController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: deviceID, Discovered: true,
			Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: "867394042309830", IMSI: "23433"},
		}},
		storedMessages: []device.SMSMessage{{
			Index: 4, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, From: "+447700900123", Text: "hello",
			Encoding: device.SMSEncodingGSM7PDU, ServiceCenterTimestamp: &receivedAt, RawPDU: "ME-hello",
		}},
	}
	server := &Server{store: database, logger: regionTestLogger(), devices: controller}
	server.syncModemSMS(ctx, deviceID)
	controller.mu.Lock()
	remaining := len(controller.storedMessages)
	deleted := len(controller.deleted)
	controller.mu.Unlock()
	if remaining != 1 || deleted != 0 {
		t.Fatalf("remaining = %d, deleted = %d", remaining, deleted)
	}
}

func TestSyncModemSMSDoesNotClearUnpersistedMessages(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const deviceID = "ec20-1"
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	controller := &smsDeletionController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: deviceID, Discovered: true,
			Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: "867394042309830"},
		}},
		storedMessages: []device.SMSMessage{{
			Index: 9, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionReceived, Text: "no peer",
			Encoding: device.SMSEncodingGSM7PDU, RawPDU: "orphan",
		}},
	}
	server := &Server{store: database, logger: regionTestLogger(), devices: controller}
	server.syncModemSMS(ctx, deviceID)
	controller.mu.Lock()
	remaining := len(controller.storedMessages)
	deleted := len(controller.deleted)
	controller.mu.Unlock()
	if remaining != 1 || deleted != 0 {
		t.Fatalf("remaining = %d, deleted = %d", remaining, deleted)
	}
	stored, err := database.ListSMSMessages(ctx, store.SMSFilter{DeviceID: deviceID})
	if err != nil || len(stored) != 0 {
		t.Fatalf("stored = %#v, %v", stored, err)
	}
}

func TestSyncModemSMSKeepsUnmatchedDeliveryReports(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	reference := 42
	status := 0
	controller := &smsDeletionController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: deviceID, Discovered: true,
			Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: "23433"},
		}},
		storedMessages: []device.SMSMessage{{
			Index: 8, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionStatusReport, To: "+447700900123",
			MessageReference: &reference, StatusCode: &status, DeliveryStatus: "delivered",
			Encoding: device.SMSEncodingGSM7PDU, RawPDU: "report-unmatched",
		}},
	}
	server := &Server{store: database, logger: regionTestLogger(), devices: controller}
	server.syncModemSMS(ctx, deviceID)
	controller.mu.Lock()
	remaining := len(controller.storedMessages)
	deleted := len(controller.deleted)
	controller.mu.Unlock()
	if remaining != 1 || deleted != 0 {
		t.Fatalf("remaining = %d, deleted = %d", remaining, deleted)
	}
}

func TestSyncModemSMSClearsMatchedDeliveryReports(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const (
		deviceID = "ec20-1"
		imei     = "867394042309830"
		imsi     = "23433"
		peer     = "+447700900123"
	)
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: imei, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := database.SaveSMSMessage(ctx, store.SMSMessage{
		MessageID: "outbound-1", DeviceID: deviceID, ModemIMEI: imei, IMSI: imsi,
		Peer: peer, Direction: "outbound", Body: "ping", Source: "cellular_at",
		Timestamp: time.Unix(1_700_000_000, 0).UTC(), Extra: json.RawMessage(`{"message_reference":42}`),
	}); err != nil {
		t.Fatal(err)
	}
	reference := 42
	status := 0
	controller := &smsDeletionController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: deviceID, Discovered: true,
			Snapshot: &device.Snapshot{DeviceID: deviceID, IMEI: imei, IMSI: imsi},
		}},
		storedMessages: []device.SMSMessage{{
			Index: 8, Storage: "ME", StorageStatus: device.SMSStatusReceivedUnread,
			Direction: device.SMSDirectionStatusReport, To: peer,
			MessageReference: &reference, StatusCode: &status, DeliveryStatus: "delivered",
			Encoding: device.SMSEncodingGSM7PDU, RawPDU: "report-matched",
		}},
	}
	server := &Server{store: database, logger: regionTestLogger(), devices: controller}
	server.syncModemSMS(ctx, deviceID)
	controller.mu.Lock()
	remaining := len(controller.storedMessages)
	deleted := append([]string(nil), controller.deleted...)
	controller.mu.Unlock()
	if remaining != 0 || strings.Join(deleted, ",") != "ME:8" {
		t.Fatalf("remaining = %d, deleted = %v", remaining, deleted)
	}
}

func TestSMSSettingsAPITogglesAutoClear(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	server := &Server{store: database, logger: regionTestLogger(), maxRequestBodyBytes: 4096}
	get := httptest.NewRecorder()
	server.handleSMSSettings(get, httptest.NewRequest(http.MethodGet, "/api/settings/sms", nil))
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"auto_clear_modem_storage":true`) {
		t.Fatalf("default GET = %d %s", get.Code, get.Body.String())
	}
	put := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPut, "/api/settings/sms", strings.NewReader(`{"auto_clear_modem_storage":false}`))
	request.Header.Set("Content-Type", "application/json")
	server.handleSMSSettings(put, request)
	if put.Code != http.StatusOK {
		t.Fatalf("PUT status = %d, body=%s", put.Code, put.Body.String())
	}
	if developer.AutoClearModemStorage(ctx, database) {
		t.Fatal("auto-clear should be disabled after PUT")
	}
}

func TestSMSSendOutcome(t *testing.T) {
	tests := []struct {
		name      string
		all       bool
		accepted  int
		total     int
		delivered bool
		want      string
	}{
		{name: "delivered", all: true, accepted: 1, total: 1, delivered: true, want: "delivered"},
		{name: "accepted but unconfirmed", all: true, accepted: 2, total: 2, want: "accepted_unconfirmed"},
		{name: "partial", accepted: 1, total: 2, want: "partial"},
		{name: "failed", total: 1, want: "failed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := smsSendOutcome(test.all, test.accepted, test.total, test.delivered); got != test.want {
				t.Fatalf("smsSendOutcome() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestBlockedSMSDestination(t *testing.T) {
	t.Skip("SMS destination restriction disabled")
	tests := []struct {
		name  string
		phone string
		block bool
	}{
		{"e164 china", "+8613800138000", true},
		{"no plus china", "8613800138000", true},
		{"international prefix china", "008613800138000", true},
		{"spaced china", "+86 138 0013 8000", true},
		{"dashed china", "+86-138-0013-8000", true},
		{"us e164", "+12025550177", false},
		{"us no plus", "12025550177", false},
		{"uk e164", "+447700900123", false},
		{"italy", "+393331234567", false},
		{"russia", "+79161234567", false},
		{"japan", "+819012345678", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			blocked, _ := blockedSMSDestination(test.phone)
			if blocked != test.block {
				t.Fatalf("blockedSMSDestination(%q) blocked = %v, want %v", test.phone, blocked, test.block)
			}
		})
	}
}

func TestHandleSMSSendEnforcesGlobalHourlyLimit(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := developer.SetSMSHourlyLimit(ctx, database, 1); err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertDevice(ctx, store.Device{ID: "ec20_1", Name: "EC20"}); err != nil {
		t.Fatal(err)
	}
	if reservation, err := database.ReserveSMSSend(ctx, "another-device", 1, time.Now().UTC()); err != nil || !reservation.Allowed {
		t.Fatalf("seed global SMS reservation = %+v, %v", reservation, err)
	}
	server := &Server{
		store:               database,
		logger:              regionTestLogger(),
		maxRequestBodyBytes: 4096,
		devices: fakeDeviceController{entry: device.Device{
			ID:         "ec20_1",
			Discovered: true,
			Snapshot:   &device.Snapshot{DeviceID: "ec20_1"},
		}},
	}
	request := httptest.NewRequest(
		http.MethodPost,
		"/api/sms/send",
		strings.NewReader(`{"device_id":"ec20_1","phone":"+447700900123","message":"hello"}`),
	)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.handleSMSSend(response, request)
	if response.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429; body=%s", response.Code, response.Body.String())
	}
	if response.Header().Get("Retry-After") == "" {
		t.Fatal("Retry-After header is missing")
	}
	var envelope errorEnvelope
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code != "sms_rate_limited" {
		t.Fatalf("error code = %q, want sms_rate_limited", envelope.Error.Code)
	}
}

type smsArrivalController struct {
	fakeDeviceController
	arrivals *device.SMSEvents
	listed   chan string
}

func (controller smsArrivalController) SubscribeSMSEvents(context.Context) (*device.SMSEvents, error) {
	return controller.arrivals, nil
}

func (controller smsArrivalController) ListSMS(_ context.Context, physicalID string) (device.SMSListing, error) {
	controller.listed <- physicalID
	return device.SMSListing{}, nil
}

func TestSMSSyncLoopScansModemOnArrivalEvent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	const deviceID = "ec20-1"
	if err := database.UpsertDevice(ctx, store.Device{
		ID: deviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	controller := smsArrivalController{
		fakeDeviceController: fakeDeviceController{entry: device.Device{
			ID: deviceID, Discovered: true, Snapshot: &device.Snapshot{DeviceID: deviceID},
		}},
		arrivals: device.NewSMSEvents(),
		listed:   make(chan string, 4),
	}
	server := &Server{store: database, logger: regionTestLogger(), devices: controller}
	done := make(chan struct{})
	go func() {
		defer close(done)
		server.StartSMSSyncLoop(ctx, time.Hour)
	}()

	// An arrival on a modem that is not configured must not scan the others,
	// and a burst from one modem collapses into a single scan.
	controller.arrivals.Add("other-modem")
	for range 16 {
		controller.arrivals.Add(deviceID)
	}
	select {
	case physicalID := <-controller.listed:
		if physicalID != deviceID {
			t.Fatalf("scanned %q, want %q", physicalID, deviceID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("SMS arrival event did not trigger a modem scan")
	}
	select {
	case physicalID := <-controller.listed:
		t.Fatalf("unexpected extra scan of %q", physicalID)
	case <-time.After(100 * time.Millisecond):
	}
	cancel()
	<-done
}

type multipartSyncFixture struct {
	t          *testing.T
	ctx        context.Context
	database   *store.Store
	controller *smsDeletionController
	server     *Server
}

const (
	multipartDeviceID = "ec20-1"
	multipartIMEI     = "867394042309830"
	multipartPeer     = "+447700900123"
)

func newMultipartSyncFixture(t *testing.T) *multipartSyncFixture {
	t.Helper()
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if err := database.UpsertDevice(ctx, store.Device{
		ID: multipartDeviceID, Name: "EC20", DeviceType: store.DeviceTypePCIeEC20EC25,
		ModemIMEI: multipartIMEI, SMSEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	controller := &smsDeletionController{fakeDeviceController: fakeDeviceController{entry: device.Device{
		ID: multipartDeviceID, Discovered: true,
		Snapshot: &device.Snapshot{DeviceID: multipartDeviceID, IMEI: multipartIMEI, IMSI: "23433"},
	}}}
	return &multipartSyncFixture{
		t: t, ctx: ctx, database: database, controller: controller,
		server: &Server{store: database, logger: regionTestLogger(), devices: controller},
	}
}

func multipartSegment(index, reference, total, sequence int, body string, at time.Time) device.SMSMessage {
	return device.SMSMessage{
		Index: index, Storage: "SM", StorageStatus: device.SMSStatusReceivedUnread,
		Direction: device.SMSDirectionReceived, From: multipartPeer, Text: body,
		Encoding: device.SMSEncodingGSM7PDU, ServiceCenterTimestamp: &at,
		Concat: &device.SMSConcatInfo{Reference: reference, Total: total, Sequence: sequence},
		RawPDU: fmt.Sprintf("SM-%d-%d-%s", reference, sequence, body),
	}
}

// deliver puts messages into modem storage, runs one scan and returns the
// slots the scan deleted.
func (fixture *multipartSyncFixture) deliver(messages ...device.SMSMessage) []string {
	fixture.t.Helper()
	fixture.controller.mu.Lock()
	fixture.controller.storedMessages = append(fixture.controller.storedMessages, messages...)
	before := len(fixture.controller.deleted)
	fixture.controller.mu.Unlock()
	fixture.server.syncModemSMS(fixture.ctx, multipartDeviceID)
	fixture.controller.mu.Lock()
	defer fixture.controller.mu.Unlock()
	deleted := append([]string(nil), fixture.controller.deleted[before:]...)
	sort.Strings(deleted)
	return deleted
}

func (fixture *multipartSyncFixture) stored() []store.SMSMessage {
	fixture.t.Helper()
	stored, err := fixture.database.ListSMSMessages(fixture.ctx, store.SMSFilter{DeviceID: multipartDeviceID})
	if err != nil {
		fixture.t.Fatal(err)
	}
	return stored
}

func TestSyncModemSMSMergesSegmentThatReusesFreedSlot(t *testing.T) {
	fixture := newMultipartSyncFixture(t)
	at := time.Unix(1_700_000_000, 0).UTC()
	plain := device.SMSMessage{
		Index: 1, Storage: "SM", StorageStatus: device.SMSStatusReceivedUnread,
		Direction: device.SMSDirectionReceived, From: "+447700900999", Text: "plain",
		Encoding: device.SMSEncodingGSM7PDU, ServiceCenterTimestamp: &at, RawPDU: "plain",
	}
	// The first segment is saved and released right away; the second segment
	// then lands in the lower slot the plain SMS freed.
	if deleted := fixture.deliver(plain, multipartSegment(2, 41, 2, 1, "first ", at)); strings.Join(deleted, ",") != "SM:1,SM:2" {
		t.Fatalf("first scan deleted %v", deleted)
	}
	if deleted := fixture.deliver(multipartSegment(1, 41, 2, 2, "second", at.Add(2*time.Second))); strings.Join(deleted, ",") != "SM:1" {
		t.Fatalf("second scan deleted %v", deleted)
	}
	var multipart []store.SMSMessage
	for _, message := range fixture.stored() {
		if message.Peer == multipartPeer {
			multipart = append(multipart, message)
		}
	}
	if len(multipart) != 1 || multipart[0].Body != "first second" ||
		!store.ConcatSMSReadyToNotify(multipart[0].MessageID, multipart[0].Extra) {
		t.Fatalf("multipart rows = %#v", multipart)
	}
}

func TestSyncModemSMSMergesSegmentsArrivingInReverseOrder(t *testing.T) {
	fixture := newMultipartSyncFixture(t)
	at := time.Unix(1_700_000_000, 0).UTC()
	fixture.deliver(multipartSegment(1, 9, 2, 2, "tail", at.Add(time.Second)))
	fixture.deliver(multipartSegment(1, 9, 2, 1, "head ", at))
	stored := fixture.stored()
	if len(stored) != 1 || stored[0].Body != "head tail" ||
		!store.ConcatSMSReadyToNotify(stored[0].MessageID, stored[0].Extra) {
		t.Fatalf("stored = %#v", stored)
	}
}

func TestSyncModemSMSReleasesSegmentsOfIncompleteMessages(t *testing.T) {
	fixture := newMultipartSyncFixture(t)
	old := time.Now().Add(-48 * time.Hour).UTC()
	var segments []device.SMSMessage
	for index := 1; index <= 40; index++ {
		segments = append(segments, multipartSegment(index, index, 2, 1, fmt.Sprintf("orphan %d ", index), old))
	}
	if deleted := fixture.deliver(segments...); len(deleted) != 40 {
		t.Fatalf("deleted %d of 40 persisted segments", len(deleted))
	}
	if stored := fixture.stored(); len(stored) != 40 {
		t.Fatalf("persisted %d of 40 partial messages", len(stored))
	}
}

func TestSyncModemSMSDoesNotTakeReadStateFromModemRescan(t *testing.T) {
	fixture := newMultipartSyncFixture(t)
	if err := developer.SetAutoClearModemStorage(fixture.ctx, fixture.database, false); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1_700_000_000, 0).UTC()
	fixture.deliver(multipartSegment(1, 5, 2, 1, "first ", at))
	// AT+CMGL=4 marked the first segment read on the modem.
	fixture.controller.mu.Lock()
	fixture.controller.storedMessages[0].StorageStatus = device.SMSStatusReceivedRead
	fixture.controller.mu.Unlock()
	fixture.deliver(multipartSegment(2, 5, 2, 2, "second", at))
	stored := fixture.stored()
	if len(stored) != 1 || stored[0].Body != "first second" || stored[0].Read {
		t.Fatalf("stored = %#v", stored)
	}
}

func TestSyncModemSMSSeparatesReusedReferenceWithSameText(t *testing.T) {
	fixture := newMultipartSyncFixture(t)
	at := time.Unix(1_700_000_000, 0).UTC()
	fixture.deliver(multipartSegment(1, 3, 2, 1, "same ", at), multipartSegment(2, 3, 2, 2, "text", at))
	later := at.Add(time.Hour)
	fixture.deliver(multipartSegment(1, 3, 2, 1, "same ", later), multipartSegment(2, 3, 2, 2, "text", later))
	stored := fixture.stored()
	if len(stored) != 2 {
		t.Fatalf("stored = %#v", stored)
	}
	for _, message := range stored {
		if message.Body != "same text" || !store.ConcatSMSReadyToNotify(message.MessageID, message.Extra) {
			t.Fatalf("stored = %#v", stored)
		}
	}
}

func TestSyncModemSMSRescanMatchesLegacyMultipartRow(t *testing.T) {
	fixture := newMultipartSyncFixture(t)
	if err := developer.SetAutoClearModemStorage(fixture.ctx, fixture.database, false); err != nil {
		t.Fatal(err)
	}
	at := time.Unix(1_700_000_000, 0).UTC()
	segments := []device.SMSMessage{multipartSegment(20, 7, 2, 1, "legacy ", at), multipartSegment(21, 7, 2, 2, "row", at)}
	// A row written before part keys existed, under the old slot-based ID.
	legacyID := store.StableConcatMessageID("cellular_at:20", multipartIMEI, multipartDeviceID, multipartPeer, 7, 2)
	for _, segment := range segments {
		extra, _ := json.Marshal(map[string]any{"concat": segment.Concat, "keep_durable_id_on_rescan": true})
		if _, err := fixture.database.SaveSMSMessage(fixture.ctx, store.SMSMessage{
			MessageID: legacyID, DeviceID: multipartDeviceID, ModemIMEI: multipartIMEI,
			Peer: multipartPeer, Direction: "inbound", Body: segment.Text, Timestamp: at,
			Source: "cellular_at", PartsTotal: 2, Extra: extra,
		}); err != nil {
			t.Fatal(err)
		}
	}
	latest, err := fixture.database.LatestSMSMessageID(fixture.ctx)
	if err != nil {
		t.Fatal(err)
	}
	fixture.deliver(segments...)
	fixture.deliver()
	stored := fixture.stored()
	if len(stored) != 1 || stored[0].MessageID != legacyID || stored[0].Body != "legacy row" {
		t.Fatalf("stored = %#v", stored)
	}
	if fresh, err := fixture.database.ListInboundSMSAfterID(fixture.ctx, latest, 10); err != nil || len(fresh) != 0 {
		t.Fatalf("rescan surfaced rows to notifiers: %#v, %v", fresh, err)
	}
}
