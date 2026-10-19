package server

import (
	"context"
	"testing"

	"vocat/internal/store"
)

// A message persisted but not yet notified must still be delivered after a
// restart, and one already notified must not repeat. Without a persisted cursor
// the restarted process resumes past the pending message and drops it.
func TestSMSNotificationResumesAfterRestart(t *testing.T) {
	ctx := context.Background()
	path := t.TempDir() + "/vocat.db"
	database, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertDevice(ctx, store.Device{ID: "device-1", Name: "EC20"}); err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertNotificationSetting(ctx, store.NotificationSetting{
		Channel: "webhook", Enabled: true, Config: []byte(`{"urls":["https://example.com/hook"]}`),
	}); err != nil {
		t.Fatal(err)
	}
	save := func(body string) int64 {
		t.Helper()
		saved, err := database.SaveSMSMessage(ctx, store.SMSMessage{
			DeviceID: "device-1", Peer: "SmartApp", Direction: "inbound", Body: body,
		})
		if err != nil {
			t.Fatal(err)
		}
		return saved.ID
	}
	deliver := func(server *Server, state *smsNotificationCursor, sent *[]string) {
		t.Helper()
		sender := func(_ context.Context, _ string, _ map[string]any, message smsNotification) error {
			*sent = append(*sent, message.Content)
			return nil
		}
		server.deliverSMSNotificationsOnce(ctx, "webhook", state, sender)
	}

	// Delivered, and its cursor recorded.
	first := save("delivered before restart")
	state := &smsNotificationCursor{}
	sent := make([]string, 0, 2)
	deliver(&Server{store: database, logger: regionTestLogger()}, state, &sent)
	if len(sent) != 1 {
		t.Fatalf("first delivery = %#v", sent)
	}

	// The process dies with a message stored but not yet notified.
	pending := save("stored but not notified")
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	server := &Server{store: reopened, logger: regionTestLogger()}
	sent = nil
	deliver(server, &smsNotificationCursor{}, &sent)

	if len(sent) != 1 || sent[0] != "stored but not notified" {
		t.Fatalf("after restart delivered = %#v, want only the pending message", sent)
	}
	if pending <= first {
		t.Fatalf("pending id %d should follow the delivered id %d", pending, first)
	}
}
