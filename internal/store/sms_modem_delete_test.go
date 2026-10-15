package store

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestPrepareModemSMSDeletionLegacy(t *testing.T) {
	stamp := time.Unix(1_700_000_000, 0).UTC()
	later, minute, tooLate := stamp.Add(time.Second), stamp.Add(time.Minute), stamp.Add(61*time.Second)
	first := modemTestPart(t, 1, "first ", "first-pdu", &stamp)
	second := modemTestPart(t, 2, "second", "second-pdu", &later)
	alias := second
	alias.Direction, alias.DeviceID = "received", "renamed-device"
	otherProtocol := second
	var extra modemSMSExtra
	if err := json.Unmarshal(second.Extra, &extra); err != nil {
		t.Fatal(err)
	}
	extra.AT.DCS = 8
	otherProtocol.Extra, _ = json.Marshal(extra)
	unrelated := []SMSMessage{first, first, first, first}
	unrelated[0].Peer = "+447700900999"
	unrelated[1].ModemIMEI = "another-modem"
	unrelated[2].Direction = "outbound"
	unrelated[3].Source = "vowifi"
	changedSIM := []SMSMessage{first, second}
	for index := range changedSIM {
		changedSIM[index].ICCID = "another-subscription"
	}

	for _, test := range []struct {
		name     string
		observed []SMSMessage
		wantErr  bool
		backfill bool
		stale    bool
	}{
		{"t and t+1 with direction and device aliases", []SMSMessage{alias, first}, false, true, false},
		{"sixty seconds", []SMSMessage{first, modemTestPart(t, 2, "second", "second-pdu", &minute)}, false, true, false},
		{"same hash copies", []SMSMessage{first, second, first, second}, false, true, false},
		{"missing segment", []SMSMessage{first}, true, false, false},
		{"missing SCTS", []SMSMessage{first, modemTestPart(t, 2, "second", "second-pdu", nil)}, true, false, false},
		{"competing hash", []SMSMessage{first, second, modemTestPart(t, 2, "second", "other-pdu", &later)}, true, false, false},
		{"different body", []SMSMessage{first, modemTestPart(t, 2, "other", "other-pdu", &later)}, true, false, false},
		{"different protocol", []SMSMessage{first, otherProtocol}, true, false, false},
		{"different minimum", []SMSMessage{modemTestPart(t, 1, "first ", "first-pdu", &later), second}, true, false, false},
		{"over sixty seconds", []SMSMessage{first, modemTestPart(t, 2, "second", "second-pdu", &tooLate)}, true, false, false},
		{"empty listing", nil, false, false, false},
		{"unrelated listing", unrelated, false, false, false},
		{"SIM changed with potential legacy copies", changedSIM, true, false, false},
		{"concurrent metadata change", []SMSMessage{first, second}, true, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			database, err := Open(ctx, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			var stored SMSMessage
			for _, part := range []SMSMessage{first, second} {
				document, err := decodeJSONObject(part.Extra)
				if err != nil {
					t.Fatal(err)
				}
				delete(document, "at_concat")
				document["unrelated_metadata"] = "preserve me"
				part.Extra, _ = json.Marshal(document)
				stored, err = database.SaveSMSMessage(ctx, part)
				if err != nil {
					t.Fatal(err)
				}
			}
			expected := stored
			if test.stale {
				document, _ := decodeJSONObject(stored.Extra)
				document["concurrent_change"] = true
				expected.Extra, _ = json.Marshal(document)
				if _, err := database.db.ExecContext(ctx, `UPDATE sms_messages SET extra_json = ? WHERE id = ?`, string(expected.Extra), stored.ID); err != nil {
					t.Fatal(err)
				}
			}
			notifications := database.SMSNotifications()
			prepared, err := database.PrepareModemSMSDeletion(ctx, stored, test.observed)
			if (err != nil) != test.wantErr {
				t.Fatalf("PrepareModemSMSDeletion = %#v, %v", prepared, err)
			}
			persisted, err := database.SMSMessage(ctx, stored.ID)
			if err != nil {
				t.Fatal(err)
			}
			if test.backfill {
				var pinned modemSMSExtra
				if err := json.Unmarshal(persisted.Extra, &pinned); err != nil || pinned.AT == nil || len(pinned.AT.Segments) != 2 {
					t.Fatalf("pinned metadata = %s, %v", persisted.Extra, err)
				}
				before, _ := decodeJSONObject(expected.Extra)
				after, _ := decodeJSONObject(persisted.Extra)
				delete(after, "at_concat")
				if !reflect.DeepEqual(before, after) || !reflect.DeepEqual(prepared, persisted) {
					t.Fatal("backfill changed non-fingerprint metadata or was not persisted")
				}
				for _, part := range []SMSMessage{first, second} {
					if !ModemSMSMatches(prepared, part) {
						t.Fatal("prepared segment does not match for physical deletion")
					}
				}
				// After one physical deletion fails, retry only the remaining hash;
				// the first slot may now contain a completely different delivery.
				reused := modemTestPart(t, 1, "replacement", "replacement-pdu", &later)
				remaining := second
				remaining.ICCID, remaining.IMSI = "current-card", "current-sim"
				retried, err := database.PrepareModemSMSDeletion(ctx, persisted, []SMSMessage{reused, remaining})
				if err != nil || !reflect.DeepEqual(retried, persisted) || ModemSMSMatches(retried, reused) || !ModemSMSMatches(retried, remaining) {
					t.Fatalf("retry = %#v, %v", retried, err)
				}
				expected.Extra = persisted.Extra
			}
			if !reflect.DeepEqual(persisted, expected) {
				t.Fatalf("non-metadata state changed: got %#v, want %#v", persisted, expected)
			}
			cursor, err := database.LatestSMSMessageID(ctx)
			if err != nil || cursor != stored.ID {
				t.Fatalf("notification cursor = %d, %v", cursor, err)
			}
			select {
			case <-notifications:
				t.Fatal("deletion preparation emitted a notification")
			default:
			}
		})
	}
}

func TestPrepareModemSMSDeletionFingerprintCoverage(t *testing.T) {
	for _, complete := range []bool{false, true} {
		name := "partial new-format group"
		if complete {
			name = "legacy group with partial fingerprints"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			database, err := Open(ctx, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			first := modemTestPart(t, 1, "first ", "first-pdu")
			stored, err := database.SaveSMSMessage(ctx, first)
			if err != nil {
				t.Fatal(err)
			}
			if complete {
				document, _ := decodeJSONObject(stored.Extra)
				document["concat_parts"].(map[string]any)["2"] = "second"
				document["concat_complete"] = true
				stored.Extra, _ = json.Marshal(document)
				stored.MessageID = first.MessageID
				if _, err := database.db.ExecContext(ctx, `UPDATE sms_messages SET message_id = ?, extra_json = ? WHERE id = ?`, stored.MessageID, string(stored.Extra), stored.ID); err != nil {
					t.Fatal(err)
				}
			}
			prepared, err := database.PrepareModemSMSDeletion(ctx, stored, []SMSMessage{first})
			if (err != nil) != complete || (!complete && !reflect.DeepEqual(prepared, stored)) {
				t.Fatalf("PrepareModemSMSDeletion = %#v, %v", prepared, err)
			}
		})
	}
}
