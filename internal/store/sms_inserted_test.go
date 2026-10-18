package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func smsInsertedFired(inserted <-chan struct{}) bool {
	select {
	case <-inserted:
		return true
	default:
		return false
	}
}

func TestSMSInsertedSignalsOnlyNewInboundRows(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	inbound := SMSMessage{
		MessageID: "modem:SM:3:abcd", DeviceID: "device-1", Peer: "+12025550123",
		Direction: "inbound", Body: "hello", Timestamp: time.Unix(1_700_000_000, 0).UTC(),
		Source: "cellular_at",
	}

	inserted := database.SMSInserted()
	if _, err := database.SaveSMSMessage(ctx, SMSMessage{
		DeviceID: "device-1", Peer: "+12025550123", Direction: "outbound", Body: "sent",
	}); err != nil {
		t.Fatal(err)
	}
	if smsInsertedFired(inserted) {
		t.Fatal("outbound SMS must not wake inbound notification consumers")
	}

	if _, err := database.SaveSMSMessageWithResult(ctx, inbound); err != nil {
		t.Fatal(err)
	}
	if !smsInsertedFired(inserted) {
		t.Fatal("new inbound SMS did not wake notification consumers")
	}

	// A periodic modem rescan re-saves the same row; it must not wake anyone.
	inserted = database.SMSInserted()
	if _, err := database.SaveSMSMessageWithResult(ctx, inbound); err != nil {
		t.Fatal(err)
	}
	if smsInsertedFired(inserted) {
		t.Fatal("idempotent rescan woke notification consumers")
	}

	inbound.MessageID, inbound.Body = "modem:SM:4:ef01", "batch"
	if err := database.SaveSMSMessages(ctx, []SMSMessage{inbound}); err != nil {
		t.Fatal(err)
	}
	if !smsInsertedFired(inserted) {
		t.Fatal("batch-saved inbound SMS did not wake notification consumers")
	}
}

func TestStartupSMSMessageIDIsCapturedAtOpen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "vocat.db")
	database, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	if got := database.StartupSMSMessageID(); got != 0 {
		t.Fatalf("empty database StartupSMSMessageID = %d, want 0", got)
	}
	historic, err := database.SaveSMSMessage(ctx, SMSMessage{
		DeviceID: "device-1", Peer: "+12025550123", Direction: "inbound", Body: "old",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}

	database, err = Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.SaveSMSMessage(ctx, SMSMessage{
		DeviceID: "device-1", Peer: "+12025550123", Direction: "inbound", Body: "during startup",
	}); err != nil {
		t.Fatal(err)
	}
	if got := database.StartupSMSMessageID(); got != historic.ID {
		t.Fatalf("StartupSMSMessageID = %d, want %d (history only)", got, historic.ID)
	}
}
