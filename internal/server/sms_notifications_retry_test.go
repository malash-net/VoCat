package server

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"

	"golang.org/x/crypto/bcrypt"

	"vocat/internal/auth"
	"vocat/internal/store"
)

func startupSMSNotificationStore(t *testing.T) (*store.Store, store.SMSMessage) {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vocat.db")
	archive, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = archive.Close() })
	message := store.SMSMessage{
		DeviceID: "device-1", Peer: "+12025550123", Direction: "inbound", Body: "history",
	}
	if _, err := archive.SaveSMSMessage(ctx, message); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
	database, err := store.Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	message.Body = "received during startup"
	message, err = database.SaveSMSMessage(ctx, message)
	if err != nil {
		t.Fatal(err)
	}
	return database, message
}

func TestSMSNotificationsIncludeStartupMessages(t *testing.T) {
	ctx := context.Background()
	database, message := startupSMSNotificationStore(t)
	server := &Server{store: database, logger: regionTestLogger()}
	for channel, config := range map[string]string{
		"bark":     `{"urls":["https://example.com/bark"]}`,
		"email":    `{"smtp_host":"smtp.example.com","from_address":"from@example.com","to_addresses":["to@example.com"]}`,
		"pushplus": `{"token":"test-token"}`,
		"webhook":  `{"urls":["https://example.com/hook"]}`,
		"wecom":    `{"urls":["https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=test"],"payload_template":"{\"msgtype\":\"text\",\"text\":{\"content\":{{message}}}}"}`,
		"lark":     `{"url":"https://open.larksuite.com/open-apis/bot/v2/hook/test","payload_template":"{\"msg_type\":\"text\",\"content\":{\"text\":{{message}}}}"}`,
		"meow":     `{"nickname":"test"}`,
	} {
		t.Run(channel, func(t *testing.T) {
			if err := database.UpsertNotificationSetting(ctx, store.NotificationSetting{
				Channel: channel, Enabled: true, Config: json.RawMessage(config),
			}); err != nil {
				t.Fatal(err)
			}
			var delivered []string
			sender := func(_ context.Context, _ string, _ map[string]any, notification smsNotification) error {
				delivered = append(delivered, notification.Content)
				return nil
			}
			state := &smsNotificationCursor{}
			for range 2 {
				if _, retry := server.deliverSMSNotificationsOnce(ctx, channel, state, sender); retry {
					t.Fatal("unexpected notification retry")
				}
			}
			if len(delivered) != 1 || delivered[0] != message.Body || state.id != message.ID {
				t.Fatalf("delivered=%v, cursor=%d; want only startup message %d", delivered, state.id, message.ID)
			}
		})
	}
}

// TestSMSNotificationFailureDoesNotStallLaterMessages verifies that a provider
// permanently rejecting one SMS does not stall every later notification for
// that channel and is not retried forever: the dispatcher retries with a
// bounded exponential backoff and then moves past the poisoned message so later
// ones are still delivered.
func TestSMSNotificationFailureDoesNotStallLaterMessages(t *testing.T) {
	ctx := context.Background()
	database, err := store.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	authService, err := auth.New(database, auth.Options{SessionTTL: time.Hour, BcryptCost: bcrypt.MinCost})
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(Options{
		Store:               database,
		Auth:                authService,
		Assets:              fstest.MapFS{"index.html": &fstest.MapFile{Data: []byte("<html></html>")}},
		MaxRequestBodyBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.UpsertNotificationSetting(ctx, store.NotificationSetting{
		Channel:         "webhook",
		Enabled:         true,
		Config:          []byte(`{"urls":["https://example.com/hook"]}`),
		SensitiveFields: store.DefaultNotificationSensitiveFields("webhook"),
	}); err != nil {
		t.Fatal(err)
	}

	firstAttempts := 0
	delivered := 0
	sender := func(_ context.Context, _ string, _ map[string]any, notification smsNotification) error {
		if notification.Content == "first" {
			firstAttempts++
			return errors.New("provider rejected the notification")
		}
		delivered++
		return nil
	}

	state := &smsNotificationCursor{}

	for _, body := range []string{"first", "second"} {
		if _, err := database.SaveSMSMessage(ctx, store.SMSMessage{
			DeviceID:  "device-1",
			Peer:      "+12025550123",
			Direction: "inbound",
			Body:      body,
			Timestamp: time.Now().UTC(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	for tick := 0; tick < 40 && delivered == 0; tick++ {
		server.deliverSMSNotificationsOnce(ctx, "webhook", state, sender)
	}

	if delivered == 0 {
		t.Fatalf("the later notification was never delivered; a single failing message stalled the channel (first message attempted %d times)", firstAttempts)
	}
	if firstAttempts > smsNotificationMaxAttempts+1 {
		t.Fatalf("the failing notification was retried %d times instead of backing off and giving up", firstAttempts)
	}
}
