package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// PrepareModemSMSDeletion pins legacy segment fingerprints before any physical
// deletion. observed must come from one complete SM/ME listing, not a partial scan.
func (s *Store) PrepareModemSMSDeletion(ctx context.Context, stored SMSMessage, observed []SMSMessage) (SMSMessage, error) {
	if len(observed) == 0 || stored.Source != "cellular_at" || !isConcatSMSMessageID(stored.MessageID) {
		return stored, nil
	}
	var old modemSMSExtra
	if err := json.Unmarshal(stored.Extra, &old); err != nil {
		return SMSMessage{}, fmt.Errorf("decode modem SMS deletion metadata: %w", err)
	}
	fingerprinted := old.AT != nil && len(old.Parts) > 0
	for sequence := range old.Parts {
		number, err := strconv.Atoi(sequence)
		if err != nil || number < 1 || number > old.Concat.Total || strconv.Itoa(number) != sequence {
			return SMSMessage{}, fmt.Errorf("invalid stored modem SMS sequence %q", sequence)
		}
		if old.AT == nil || old.AT.Segments[sequence].Hash == "" {
			fingerprinted = false
		}
	}
	// Partial/isolated new-format messages also have reliable fingerprints for
	// every saved part; missing or reused slots must not prevent their deletion.
	if fingerprinted {
		return stored, nil
	}
	if !isLegacyModemSMS(stored.MessageID) || old.Concat.Total < 2 {
		return SMSMessage{}, fmt.Errorf("modem SMS deletion lacks segment evidence")
	}

	segments := make(map[string]ModemSMSSegment)
	protocol := old.AT
	var earliest, latest time.Time
	for _, current := range observed {
		if current.Source != stored.Source || !sameModemSMSHardware(stored, current) ||
			current.Peer != stored.Peer ||
			modemSMSDirection(current.Direction) != modemSMSDirection(stored.Direction) {
			continue
		}
		var incoming modemSMSExtra
		if err := json.Unmarshal(current.Extra, &incoming); err != nil {
			return SMSMessage{}, fmt.Errorf("decode observed modem SMS deletion metadata: %w", err)
		}
		if incoming.Concat.Reference != old.Concat.Reference || incoming.Concat.Total != old.Concat.Total {
			continue
		}
		// ME can retain the old SIM's PDUs. The current subscription cannot
		// prove that these potential legacy copies are absent or unrelated.
		if !sameSMSSubscription(stored, current) {
			return SMSMessage{}, fmt.Errorf("modem SMS deletion cannot verify legacy copies after a subscription change")
		}
		sequence := strconv.Itoa(incoming.Concat.Sequence)
		body, present := old.Parts[sequence]
		if incoming.Concat.Sequence < 1 || incoming.Concat.Sequence > old.Concat.Total || !present || body != current.Body || incoming.AT == nil {
			return SMSMessage{}, fmt.Errorf("modem SMS deletion has conflicting segment %s", sequence)
		}
		part := incoming.AT.Segments[sequence]
		if len(incoming.AT.Segments) != 1 || part.Hash == "" || part.SCTS == nil || part.SCTS.IsZero() {
			return SMSMessage{}, fmt.Errorf("modem SMS deletion lacks fingerprint or SCTS for segment %s", sequence)
		}
		if protocol != nil && !sameModemSMSProtocol(protocol, incoming.AT) {
			return SMSMessage{}, fmt.Errorf("modem SMS deletion has conflicting protocol")
		}
		protocol = incoming.AT
		if previous := segments[sequence]; previous.Hash != "" && previous.Hash != part.Hash {
			return SMSMessage{}, fmt.Errorf("modem SMS deletion has competing fingerprints for segment %s", sequence)
		}
		if old.AT != nil {
			if previous := old.AT.Segments[sequence]; previous.Hash != "" && previous.Hash != part.Hash {
				return SMSMessage{}, fmt.Errorf("modem SMS deletion changed fingerprint for segment %s", sequence)
			}
		}
		segments[sequence] = part
		if earliest.IsZero() || part.SCTS.Before(earliest) {
			earliest = *part.SCTS
		}
		if latest.IsZero() || part.SCTS.After(latest) {
			latest = *part.SCTS
		}
	}
	if len(segments) == 0 {
		// A complete listing with no related delivery only removes history.
		return stored, nil
	}
	if len(segments) != old.Concat.Total || !earliest.Equal(stored.Timestamp) || latest.Sub(earliest) > time.Minute {
		return SMSMessage{}, fmt.Errorf("modem SMS deletion cannot verify the complete legacy delivery")
	}
	metadata := *protocol
	metadata.Segments = segments
	if old.AT != nil {
		metadata.Isolated = old.AT.Isolated
	}
	document, err := decodeJSONObject(stored.Extra)
	if err != nil {
		return SMSMessage{}, err
	}
	document["at_concat"] = metadata
	extra, err := json.Marshal(document)
	if err != nil {
		return SMSMessage{}, fmt.Errorf("encode modem SMS deletion fingerprints: %w", err)
	}
	result, err := s.db.ExecContext(ctx, `UPDATE sms_messages SET extra_json = ? WHERE id = ? AND extra_json = ?`, string(extra), stored.ID, string(stored.Extra))
	if err != nil {
		return SMSMessage{}, fmt.Errorf("persist modem SMS deletion fingerprints: %w", err)
	}
	if err := requireAffected(result); err != nil {
		return SMSMessage{}, fmt.Errorf("modem SMS changed before deletion: %w", err)
	}
	stored.Extra = extra
	return stored, nil
}
