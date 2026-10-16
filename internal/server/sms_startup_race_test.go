package server

import (
	"context"
	"testing"

	"vocat/internal/store"
)

// The notification cursor starts from the history boundary captured at Open, so
// a message arriving in the startup window is delivered and pre-existing
// history is not replayed on restart.
func TestSMSNotificationStartupWindow(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/vocat.db"

	// History written before this process opens.
	first, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.UpsertDevice(ctx, store.Device{ID: "device-1", Name: "EC20"}); err != nil {
		t.Fatal(err)
	}
	if err := first.UpsertNotificationSetting(ctx, store.NotificationSetting{
		Channel: "webhook", Enabled: true, Config: []byte(`{"urls":["https://example.com/hook"]}`),
	}); err != nil {
		t.Fatal(err)
	}
	save := func(database *store.Store, body string) {
		t.Helper()
		if _, err := database.SaveSMSMessage(ctx, store.SMSMessage{
			DeviceID: "device-1", Peer: "SmartApp", Direction: "inbound", Body: body,
		}); err != nil {
			t.Fatal(err)
		}
	}
	save(first, "old message")
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	server := &Server{store: reopened, logger: regionTestLogger()}

	// Arrives after Open but before the first notification tick.
	save(reopened, "startup message")

	state := &smsNotificationCursor{}
	delivered := make([]string, 0, 2)
	sender := func(_ context.Context, _ string, _ map[string]any, message smsNotification) error {
		delivered = append(delivered, message.Content)
		return nil
	}
	server.deliverSMSNotificationsOnce(ctx, "webhook", state, sender)

	if len(delivered) != 1 || delivered[0] != "startup message" {
		t.Fatalf("delivered = %#v, want only the startup message", delivered)
	}
}
