package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

func modemTestPart(t *testing.T, sequence int, body, hash string, overrideSCTS ...*time.Time) SMSMessage {
	t.Helper()
	timestamp := time.Unix(1_700_000_000, 0).UTC()
	scts := &timestamp
	if len(overrideSCTS) > 0 {
		scts = overrideSCTS[0]
		if scts != nil {
			timestamp = *scts
		} else {
			timestamp = time.Now().UTC()
		}
	}
	extra, err := json.Marshal(map[string]any{
		"concat":                    map[string]int{"reference": 7, "total": 2, "sequence": sequence},
		"keep_durable_id_on_rescan": true,
		"at_concat": ModemSMSConcat{Segments: map[string]ModemSMSSegment{
			fmt.Sprint(sequence): {Hash: hash, SCTS: scts},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return SMSMessage{
		MessageID: StableConcatMessageID("cellular_at:20", "867394042309830", "modem", "+447700900123", 7, 2),
		DeviceID:  "modem", ModemIMEI: "867394042309830", ICCID: "iccid-1", IMSI: "imsi-1",
		Peer: "+447700900123", Direction: "inbound", Source: "cellular_at",
		Body: body, Timestamp: timestamp, PartsTotal: 2, Extra: extra,
	}
}

func TestModemSMSDoesNotCrossServiceCenterTimes(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	aTime := time.Unix(1_700_000_000, 0).UTC()
	bTime := aTime.Add(2 * time.Hour)
	parts := []SMSMessage{
		modemTestPart(t, 2, "A2", "a2", &aTime),
		modemTestPart(t, 1, "B1", "b1", &bTime),
		modemTestPart(t, 2, "B2", "b2", &bTime),
		modemTestPart(t, 1, "A1", "a1", &aTime),
	}
	for _, part := range parts {
		if _, err := database.SaveSMSMessage(ctx, part); err != nil {
			t.Fatal(err)
		}
	}
	messages, err := database.ListSMSMessages(ctx, SMSFilter{DeviceID: "modem"})
	if err != nil || len(messages) != 2 {
		t.Fatalf("groups = %#v, %v", messages, err)
	}
	for _, message := range messages {
		if (message.Body != "A1A2" && message.Body != "B1B2") || !ConcatSMSReadyToNotify(message.MessageID, message.Extra) {
			t.Fatalf("crossed or incomplete message: %q", message.Body)
		}
	}
}

func TestModemSMSLegacyUncertaintyRespectsSubscription(t *testing.T) {
	for _, test := range []struct {
		name, iccid, imsi string
		unknownLegacy     bool
		isolated          bool
	}{
		{"different ICCID", "iccid-2", "imsi-1", false, false},
		{"different IMSI", "iccid-1", "imsi-2", false, false},
		{"same subscription", "iccid-1", "imsi-1", false, true},
		{"unknown current subscription", "", "", false, true},
		{"unknown legacy subscription", "iccid-2", "imsi-2", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			database, err := Open(ctx, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			for index, body := range []string{"first ", "old second"} {
				part := modemTestPart(t, index+1, body, "old")
				if test.unknownLegacy {
					part.ICCID, part.IMSI = "", ""
				}
				document, _ := decodeJSONObject(part.Extra)
				delete(document, "at_concat")
				part.Extra, _ = json.Marshal(document)
				if _, err := database.SaveSMSMessage(ctx, part); err != nil {
					t.Fatal(err)
				}
			}
			later := time.Unix(1_700_000_001, 0).UTC()
			first := modemTestPart(t, 1, "first ", "new-first", &later)
			first.ICCID, first.IMSI = test.iccid, test.imsi
			stored, err := database.SaveSMSMessage(ctx, first)
			if err != nil {
				t.Fatal(err)
			}
			var extra modemSMSExtra
			if err := json.Unmarshal(stored.Extra, &extra); err != nil || extra.AT == nil || extra.AT.Isolated != test.isolated {
				t.Fatalf("new subscription isolated=%v: %#v, %v", test.isolated, stored, err)
			}
			second := modemTestPart(t, 2, "new second", "new-second", &later)
			second.ICCID, second.IMSI = test.iccid, test.imsi
			finished, err := database.SaveSMSMessage(ctx, second)
			if err != nil || ConcatSMSReadyToNotify(finished.MessageID, finished.Extra) == test.isolated {
				t.Fatalf("new subscription completion = %#v, %v", finished, err)
			}
			if !test.isolated && (finished.MessageID != stored.MessageID || finished.Body != "first new second") {
				t.Fatalf("new SIM's segments did not join: %#v", finished)
			}
		})
	}
}

func TestModemSMSGroupScopeAndExpiry(t *testing.T) {
	for _, test := range []struct {
		name          string
		alias         bool
		newSIM        bool
		newDCS        bool
		expired       bool
		differentTime bool
		missingTime   bool
		joins         bool
	}{
		{name: "compatible", joins: true},
		{name: "direction alias", alias: true, joins: true},
		{name: "different subscription", newSIM: true},
		{name: "different coding", newDCS: true},
		{name: "expired group", expired: true},
		{name: "different SCTS", differentTime: true},
		{name: "missing SCTS", missingTime: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			database, err := Open(ctx, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			first := modemTestPart(t, 1, "first ", "first-pdu")
			if test.alias {
				first.Direction = "received"
			}
			if test.expired {
				first.CreatedAt = time.Now().UTC().Add(-25 * time.Hour)
			}
			stored, err := database.SaveSMSMessage(ctx, first)
			if err != nil {
				t.Fatal(err)
			}
			// Known PDU rescans retain the original subscription and deadline.
			first.ICCID, first.IMSI, first.CreatedAt = "another-active-card", "another-active-sim", time.Now().UTC()
			replayed, err := database.SaveSMSMessageWithResult(ctx, first)
			if err != nil || replayed.Inserted || replayed.Message.ID != stored.ID ||
				replayed.Message.ICCID != stored.ICCID || replayed.Message.IMSI != stored.IMSI || !replayed.Message.CreatedAt.Equal(stored.CreatedAt) {
				t.Fatalf("replayed segment = %#v, %v", replayed, err)
			}
			second := modemTestPart(t, 2, "second", "second-pdu")
			if test.differentTime {
				later := second.Timestamp.Add(time.Second)
				second = modemTestPart(t, 2, "second", "second-pdu", &later)
			} else if test.missingTime {
				second = modemTestPart(t, 2, "second", "second-pdu", nil)
			}
			if test.newSIM {
				second.ICCID, second.IMSI = "iccid-2", "imsi-2"
			}
			if test.newDCS {
				document, err := decodeJSONObject(second.Extra)
				if err != nil {
					t.Fatal(err)
				}
				document["at_concat"].(map[string]any)["dcs"] = 8
				second.Extra, _ = json.Marshal(document)
			}
			saved, err := database.SaveSMSMessage(ctx, second)
			if err != nil {
				t.Fatal(err)
			}
			if (saved.MessageID == stored.MessageID) != test.joins || ConcatSMSReadyToNotify(saved.MessageID, saved.Extra) != test.joins {
				t.Fatalf("second segment = %#v, joins=%v", saved, test.joins)
			}
		})
	}
}

func TestModemSMSAmbiguousGroupsAreIsolated(t *testing.T) {
	ctx := context.Background()
	database, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = database.Close() })
	for _, body := range []string{"one ", "two "} {
		if _, err := database.SaveSMSMessage(ctx, modemTestPart(t, 1, body, body)); err != nil {
			t.Fatal(err)
		}
	}
	part := modemTestPart(t, 2, "ambiguous", "ambiguous-pdu")
	saved, err := database.SaveSMSMessage(ctx, part)
	if err != nil {
		t.Fatal(err)
	}
	var extra modemSMSExtra
	if err := json.Unmarshal(saved.Extra, &extra); err != nil {
		t.Fatal(err)
	}
	if extra.AT == nil || !extra.AT.Isolated || ConcatSMSReadyToNotify(saved.MessageID, saved.Extra) {
		t.Fatalf("ambiguous segment was not isolated: %#v", saved)
	}
	replayed, err := database.SaveSMSMessageWithResult(ctx, part)
	if err != nil || replayed.Inserted || replayed.Message.ID != saved.ID {
		t.Fatalf("isolated segment replay = %#v, %v", replayed, err)
	}
}

func TestModemSMSUnverifiableLegacySegmentsStayIsolated(t *testing.T) {
	later := time.Unix(1_700_000_001, 0).UTC()
	for name, scts := range map[string]*time.Time{"missing SCTS": nil, "later segment SCTS": &later} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			database, err := Open(ctx, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			for index, body := range []string{"first ", "second"} {
				part := modemTestPart(t, index+1, body, "old-pdu")
				part.Timestamp = part.Timestamp.Add(time.Duration(index) * time.Second)
				document, _ := decodeJSONObject(part.Extra)
				delete(document, "at_concat")
				part.Extra, _ = json.Marshal(document)
				if _, err := database.SaveSMSMessage(ctx, part); err != nil {
					t.Fatal(err)
				}
			}
			observed := modemTestPart(t, 2, "second", "old-second-pdu", scts)
			observed.MessageID = StableConcatMessageID("cellular_at:pending:5", observed.ModemIMEI, observed.DeviceID, observed.Peer, 7, 2)
			isolated, err := database.SaveSMSMessage(ctx, observed)
			if err != nil {
				t.Fatal(err)
			}
			var extra modemSMSExtra
			if err := json.Unmarshal(isolated.Extra, &extra); err != nil || extra.AT == nil || !extra.AT.Isolated {
				t.Fatalf("unverifiable legacy segment = %#v, %v", isolated, err)
			}
			first, err := database.SaveSMSMessage(ctx, modemTestPart(t, 1, "new first ", "new-first-pdu"))
			if err != nil || first.MessageID == isolated.MessageID || ConcatSMSReadyToNotify(first.MessageID, first.Extra) {
				t.Fatalf("new first segment absorbed legacy tail: %#v, %v", first, err)
			}
			replayed, err := database.SaveSMSMessageWithResult(ctx, observed)
			if err != nil || replayed.Inserted || replayed.Message.ID != isolated.ID {
				t.Fatalf("isolated legacy replay = %#v, %v", replayed, err)
			}
		})
	}
}

func TestModemSMSLegacyAssociationKeepsDurableID(t *testing.T) {
	for _, complete := range []bool{false, true} {
		t.Run(fmt.Sprintf("complete=%v", complete), func(t *testing.T) {
			ctx := context.Background()
			database, err := Open(ctx, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = database.Close() })
			parts := []SMSMessage{modemTestPart(t, 1, "first ", "first-pdu"), modemTestPart(t, 2, "second", "second-pdu")}
			var legacy SMSMessage
			for index, part := range parts {
				if index == 1 && !complete {
					break
				}
				document, _ := decodeJSONObject(part.Extra)
				delete(document, "at_concat")
				part.Extra, _ = json.Marshal(document)
				legacy, err = database.SaveSMSMessage(ctx, part)
				if err != nil {
					t.Fatal(err)
				}
			}
			// The low-slot second part is processed before the retained first part.
			parts[1].MessageID = StableConcatMessageID("cellular_at:pending:5", legacy.ModemIMEI, legacy.DeviceID, legacy.Peer, 7, 2)
			second, err := database.SaveSMSMessageWithResult(ctx, parts[1])
			if err != nil || second.Message.MessageID != legacy.MessageID ||
				!ConcatSMSReadyToNotify(second.Message.MessageID, second.Message.Extra) || second.Inserted == complete {
				t.Fatalf("legacy second part = %#v, %v", second, err)
			}
			first, err := database.SaveSMSMessageWithResult(ctx, parts[0])
			if err != nil || first.Inserted || first.Message.ID != second.Message.ID || first.Message.Body != "first second" {
				t.Fatalf("metadata-only backfill = %#v, %v", first, err)
			}
			for _, part := range parts {
				if !ModemSMSMatches(first.Message, part) {
					t.Fatal("backfilled PDU does not match for deletion")
				}
				replayed, err := database.SaveSMSMessageWithResult(ctx, part)
				if err != nil || replayed.Inserted || replayed.Message.ID != first.Message.ID {
					t.Fatalf("legacy replay = %#v, %v", replayed, err)
				}
			}
			parts[0].ModemIMEI = "another-modem"
			if ModemSMSMatches(first.Message, parts[0]) {
				t.Fatal("PDU matched a different modem")
			}
			// Once the old hash is known, equal text at a new SCTS does not
			// quarantine different PDUs from a new generation.
			nextTime := parts[1].Timestamp.Add(time.Second)
			next, err := database.SaveSMSMessage(ctx, modemTestPart(t, 1, "first ", "new-first-pdu", &nextTime))
			if err != nil || next.MessageID == legacy.MessageID {
				t.Fatalf("new generation = %#v, %v", next, err)
			}
			finished, err := database.SaveSMSMessage(ctx, modemTestPart(t, 2, "new second", "new-second-pdu", &nextTime))
			if err != nil || finished.MessageID != next.MessageID || !ConcatSMSReadyToNotify(finished.MessageID, finished.Extra) {
				t.Fatalf("new generation did not complete: %#v, %v", finished, err)
			}
		})
	}
}
