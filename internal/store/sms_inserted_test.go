package store

import (
	"context"
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

func TestSMSInsertedSignalsOnlyNewRows(t *testing.T) {
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
	if _, err := database.SaveSMSMessageWithResult(ctx, inbound); err != nil {
		t.Fatal(err)
	}
	if !smsInsertedFired(inserted) {
		t.Fatal("new SMS did not wake notification consumers")
	}

	// A periodic modem rescan re-saves the same row; it must not wake anyone.
	inserted = database.SMSInserted()
	if _, err := database.SaveSMSMessageWithResult(ctx, inbound); err != nil {
		t.Fatal(err)
	}
	if smsInsertedFired(inserted) {
		t.Fatal("idempotent rescan woke notification consumers")
	}
}
