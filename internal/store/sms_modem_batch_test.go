package store

import (
	"context"
	"testing"
	"time"
)

func TestPrepareModemSMSBatchFreezesConflicts(t *testing.T) {
	for _, test := range []struct {
		name         string
		differentSIM bool
		unknownSIM   bool
		later        bool
		conflict     bool
	}{
		{name: "same scope and time", conflict: true},
		{name: "different subscription", differentSIM: true},
		{name: "unknown subscription", unknownSIM: true, conflict: true},
		{name: "different timestamp", later: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			database, err := Open(ctx, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			aTime := time.Unix(1_700_000_000, 0).UTC()
			bTime := aTime
			if test.later {
				bTime = bTime.Add(2 * time.Hour)
			}
			parts := []SMSMessage{
				modemTestPart(t, 2, "A2", "a2", &aTime),
				modemTestPart(t, 1, "B1", "b1", &bTime),
				modemTestPart(t, 2, "B2", "b2", &bTime),
				modemTestPart(t, 1, "A1", "a1", &aTime),
			}
			for _, index := range []int{1, 2} {
				if test.differentSIM {
					parts[index].ICCID, parts[index].IMSI = "iccid-2", "imsi-2"
				} else if test.unknownSIM {
					parts[index].ICCID, parts[index].IMSI = "", ""
				}
			}
			if err := database.PrepareModemSMSBatch(ctx, parts); err != nil {
				t.Fatal(err)
			}
			for _, part := range parts {
				if part.modemBatchConflict != test.conflict {
					t.Fatalf("batch conflict = %v, want %v", part.modemBatchConflict, test.conflict)
				}
				if _, err := database.SaveSMSMessage(ctx, part); err != nil {
					t.Fatal(err)
				}
			}
			messages, err := database.ListSMSMessages(ctx, SMSFilter{DeviceID: "modem"})
			if err != nil {
				t.Fatal(err)
			}
			for _, message := range messages {
				if ConcatSMSReadyToNotify(message.MessageID, message.Extra) == test.conflict {
					t.Fatalf("unexpected readiness for %q", message.Body)
				}
				if !test.conflict && message.Body != "A1A2" && message.Body != "B1B2" {
					t.Fatalf("crossed message %q", message.Body)
				}
			}
			if test.conflict {
				// A later scan cannot ignore already isolated evidence and guess
				// that a new, single remaining candidate is now unambiguous.
				next := []SMSMessage{modemTestPart(t, 2, "C2", "c2", &aTime)}
				if err := database.PrepareModemSMSBatch(ctx, next); err != nil || !next[0].modemBatchConflict {
					t.Fatalf("persisted conflict disappeared: %v", err)
				}
			}
		})
	}
}

func TestSaveSMSMessagesUsesBatchEvidence(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	parts := []SMSMessage{
		modemTestPart(t, 1, "A1", "a1"), modemTestPart(t, 2, "A2", "a2"),
		modemTestPart(t, 1, "B1", "b1"), modemTestPart(t, 2, "B2", "b2"),
	}
	if err := database.SaveSMSMessages(ctx, parts); err != nil {
		t.Fatal(err)
	}
	messages, err := database.ListSMSMessages(ctx, SMSFilter{DeviceID: "modem"})
	if err != nil || len(messages) != len(parts) {
		t.Fatalf("saved %d fragments, want %d: %v", len(messages), len(parts), err)
	}
	for _, message := range messages {
		if ConcatSMSReadyToNotify(message.MessageID, message.Extra) {
			t.Fatalf("ambiguous batch produced notification %q", message.Body)
		}
	}
}
