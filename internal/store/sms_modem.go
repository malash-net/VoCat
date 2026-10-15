package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const modemSMSReassemblyWindow = 24 * time.Hour

// ModemSMSSegment identifies a physical delivery independently of its storage slot.
// SCTS is nil when the modem could not decode the service-centre timestamp.
type ModemSMSSegment struct {
	Hash string     `json:"hash"`
	SCTS *time.Time `json:"scts"`
}

type ModemSMSConcat struct {
	PID      int                        `json:"pid"`
	DCS      int                        `json:"dcs"`
	UDH      string                     `json:"udh"`
	Segments map[string]ModemSMSSegment `json:"segments"`
	Isolated bool                       `json:"isolated,omitempty"`
}

type modemSMSExtra struct {
	Concat struct {
		Reference int `json:"reference"`
		Total     int `json:"total"`
		Sequence  int `json:"sequence"`
	} `json:"concat"`
	Parts    map[string]string `json:"concat_parts"`
	Complete bool              `json:"concat_complete"`
	AT       *ModemSMSConcat   `json:"at_concat"`
}

type modemSMSCandidate struct {
	message SMSMessage
	extra   modemSMSExtra
}

func listModemSMSCandidates(ctx context.Context, executor contextQueryExecer, value SMSMessage, incoming modemSMSExtra) ([]modemSMSCandidate, error) {
	direction, alias := modemSMSDirection(value.Direction), "received"
	if direction == "outbound" {
		alias = "sent"
	}
	rows, err := executor.QueryContext(ctx, smsMessageSelect+` WHERE
		source = 'cellular_at' AND peer = ? AND direction IN (?, ?)
		AND (COALESCE(NULLIF(modem_imei, ''), 'device:' || device_id) = ?
			OR (modem_imei = '' AND device_id = ?))
		AND message_id LIKE 'concat:%'
		AND json_extract(extra_json, '$.concat.reference') = ?
		AND json_extract(extra_json, '$.concat.total') = ?`,
		value.Peer, direction, alias, smsHardwareKey(value.ModemIMEI, value.DeviceID), value.DeviceID,
		incoming.Concat.Reference, incoming.Concat.Total)
	if err != nil {
		return nil, fmt.Errorf("find modem SMS groups: %w", err)
	}
	defer rows.Close()
	var candidates []modemSMSCandidate
	for rows.Next() {
		message, err := scanSMSMessage(rows)
		if err != nil {
			return nil, err
		}
		candidate := modemSMSCandidate{message: message}
		if err := json.Unmarshal(message.Extra, &candidate.extra); err != nil {
			return nil, fmt.Errorf("decode stored modem SMS metadata: %w", err)
		}
		candidates = append(candidates, candidate)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate modem SMS groups: %w", err)
	}
	return candidates, nil
}

// resolveModemSMS runs inside the save transaction. Legacy IDs are retained only
// after matching stored evidence; new generations never depend on modem slots.
func resolveModemSMS(ctx context.Context, executor contextQueryExecer, value *SMSMessage, extra json.RawMessage, now time.Time) (json.RawMessage, bool, error) {
	if value.Source != "cellular_at" || !isConcatSMSMessageID(value.MessageID) {
		return extra, false, nil
	}
	var incoming modemSMSExtra
	if err := json.Unmarshal(extra, &incoming); err != nil {
		return nil, false, fmt.Errorf("decode modem SMS metadata: %w", err)
	}
	if incoming.AT == nil {
		return extra, false, nil
	}
	sequence := fmt.Sprint(incoming.Concat.Sequence)
	part, present := incoming.AT.Segments[sequence]
	if !present || len(incoming.AT.Segments) != 1 || part.Hash == "" || incoming.Concat.Total < 2 || incoming.Concat.Sequence < 1 || incoming.Concat.Sequence > incoming.Concat.Total {
		return nil, false, fmt.Errorf("invalid modem SMS segment metadata")
	}
	candidates, err := listModemSMSCandidates(ctx, executor, *value, incoming)
	if err != nil {
		return nil, false, err
	}
	var known, legacy, pending []modemSMSCandidate
	uncertain := false
	for _, candidate := range candidates {
		message, stored := candidate.message, candidate.extra
		body, occupied := stored.Parts[sequence]
		if stored.AT != nil && stored.AT.Segments[sequence].Hash == part.Hash {
			// A known physical delivery retains its first subscription identity,
			// even if the modem is being rescanned after a SIM switch.
			known = append(known, candidate)
			continue
		}
		if legacyModemSMSPart(message, stored, *value, incoming) {
			legacy = append(legacy, candidate)
			continue
		}
		// Legacy Timestamp is only the group's earliest SCTS. A later matching
		// segment is not proven new merely because its own SCTS differs.
		if isLegacyModemSMS(message.MessageID) && sameSMSSubscription(message, *value) && occupied && body == value.Body &&
			(stored.AT == nil || (stored.AT.Segments[sequence].Hash == "" && sameModemSMSProtocol(stored.AT, incoming.AT))) &&
			(message.Timestamp.Unix() <= 0 || compatibleModemSMSTimes(&message.Timestamp, part.SCTS)) {
			uncertain = true
		}
		if part.SCTS == nil || stored.Complete || len(stored.Parts) == 0 || !sameSMSSubscription(message, *value) || now.Sub(message.CreatedAt) > modemSMSReassemblyWindow {
			continue
		}
		if occupied {
			continue
		}
		if stored.AT != nil {
			if stored.AT.Isolated || !sameModemSMSProtocol(stored.AT, incoming.AT) || stored.AT.Segments[sequence].Hash != "" {
				continue
			}
			compatible := true
			for storedSequence := range stored.Parts {
				previous, present := stored.AT.Segments[storedSequence]
				if !present || previous.Hash == "" || !sameModemSMSTime(previous.SCTS, part.SCTS) {
					compatible = false
					break
				}
			}
			if !compatible {
				continue
			}
		} else if message.Timestamp.Unix() <= 0 || !sameModemSMSTime(&message.Timestamp, part.SCTS) {
			// A legacy timestamp is an anchor, not a tolerance window in which
			// an unknown fragment can be assumed to belong to the old generation.
			continue
		}
		pending = append(pending, candidate)
	}

	var selected *modemSMSCandidate
	switch {
	case len(known) == 1:
		selected = &known[0]
	case !value.modemBatchConflict && len(known) == 0 && len(legacy) == 1 && len(pending) == 0 && !uncertain:
		selected = &legacy[0]
	case !value.modemBatchConflict && len(known) == 0 && len(legacy) == 0 && len(pending) == 1 && !uncertain:
		selected = &pending[0]
	}
	metadata := incoming.AT
	var previous []byte
	if selected != nil {
		value.MessageID = selected.message.MessageID
		previous, _ = json.Marshal(selected.extra.AT)
		if selected.extra.AT != nil {
			metadata = selected.extra.AT
		}
	} else {
		value.MessageID = ConcatMessageIDPrefix + "cellular_at:" + rand.Text()
		metadata.Isolated = metadata.Isolated || value.modemBatchConflict || part.SCTS == nil || uncertain || len(known)+len(legacy)+len(pending) > 0
	}
	if metadata.Segments == nil {
		metadata.Segments = make(map[string]ModemSMSSegment)
	}
	if old := metadata.Segments[sequence]; old.Hash == part.Hash && part.SCTS == nil {
		part.SCTS = old.SCTS
	}
	metadata.Segments[sequence] = part
	current, _ := json.Marshal(metadata)
	document, err := decodeJSONObject(extra)
	if err != nil {
		return nil, false, err
	}
	document["at_concat"] = metadata
	encoded, err := json.Marshal(document)
	return encoded, !bytes.Equal(previous, current), err
}

func modemSMSDirection(direction string) string {
	switch direction {
	case "received":
		return "inbound"
	case "sent":
		return "outbound"
	default:
		return direction
	}
}

func sameModemSMSHardware(a, b SMSMessage) bool {
	if a.ModemIMEI != "" && b.ModemIMEI != "" {
		return a.ModemIMEI == b.ModemIMEI
	}
	return a.DeviceID == b.DeviceID
}

func sameSMSSubscription(a, b SMSMessage) bool {
	return (a.ICCID == "" || b.ICCID == "" || a.ICCID == b.ICCID) &&
		(a.IMSI == "" || b.IMSI == "" || a.IMSI == b.IMSI)
}

func sameModemSMSProtocol(a, b *ModemSMSConcat) bool {
	return a.PID == b.PID && a.DCS == b.DCS && a.UDH == b.UDH
}

func sameModemSMSTime(a, b *time.Time) bool {
	return a != nil && b != nil && a.Equal(*b)
}

func compatibleModemSMSTimes(a, b *time.Time) bool {
	return a == nil || b == nil || (a.Sub(*b) >= -modemSMSReassemblyWindow && a.Sub(*b) <= modemSMSReassemblyWindow)
}

func isLegacyModemSMS(messageID string) bool {
	return strings.HasPrefix(messageID, "concat:cellular_at:") && strings.Count(messageID, ":") > 2
}

func legacyModemSMSPart(stored SMSMessage, old modemSMSExtra, observed SMSMessage, incoming modemSMSExtra) bool {
	sequence := fmt.Sprint(incoming.Concat.Sequence)
	body, present := old.Parts[sequence]
	part := incoming.AT.Segments[sequence]
	return isLegacyModemSMS(stored.MessageID) && present && body == observed.Body &&
		sameSMSSubscription(stored, observed) && part.SCTS != nil && stored.Timestamp.Equal(*part.SCTS) &&
		(old.AT == nil || (old.AT.Segments[sequence].Hash == "" && sameModemSMSProtocol(old.AT, incoming.AT)))
}

// ModemSMSMatches checks a current PDU against persisted segment fingerprints.
// Legacy records must pass PrepareModemSMSDeletion before authorizing deletion.
func ModemSMSMatches(stored, observed SMSMessage) bool {
	if stored.Source != "cellular_at" || stored.Peer != observed.Peer || modemSMSDirection(stored.Direction) != modemSMSDirection(observed.Direction) {
		return false
	}
	if !sameModemSMSHardware(stored, observed) {
		return false
	}
	var old, incoming modemSMSExtra
	if json.Unmarshal(stored.Extra, &old) != nil || json.Unmarshal(observed.Extra, &incoming) != nil || incoming.AT == nil ||
		old.Concat.Reference != incoming.Concat.Reference || old.Concat.Total != incoming.Concat.Total {
		return false
	}
	sequence := fmt.Sprint(incoming.Concat.Sequence)
	part := incoming.AT.Segments[sequence]
	if part.Hash == "" {
		return false
	}
	return old.AT != nil && old.AT.Segments[sequence].Hash == part.Hash
}
