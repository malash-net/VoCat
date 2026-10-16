package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

type modemSMSBatchPart struct {
	message  SMSMessage
	extra    modemSMSExtra
	sequence string
	segment  ModemSMSSegment
	index    int
	known    bool
}

type modemSMSQueryKey struct {
	device, imei, peer, direction string
	reference, total              int
}

// PrepareModemSMSBatch freezes conflict evidence before any segment is saved.
// Callers must keep the prepared records together through the save loop; the
// server's SMS synchronization lock serializes that loop with other AT writers.
func (s *Store) PrepareModemSMSBatch(ctx context.Context, values []SMSMessage) error {
	var inputs []modemSMSBatchPart
	for index, value := range values {
		values[index].modemBatchConflict = false
		if value.Source != "cellular_at" || !isConcatSMSMessageID(value.MessageID) {
			continue
		}
		normalizeSMSMessageIdentity(&value)
		var extra modemSMSExtra
		if json.Unmarshal(value.Extra, &extra) != nil || extra.AT == nil {
			continue // The individual save still reports invalid input.
		}
		sequence := fmt.Sprint(extra.Concat.Sequence)
		part, present := extra.AT.Segments[sequence]
		if !present || part.Hash == "" || len(extra.AT.Segments) != 1 || extra.Concat.Total < 2 || extra.Concat.Sequence < 1 || extra.Concat.Sequence > extra.Concat.Total {
			continue
		}
		inputs = append(inputs, modemSMSBatchPart{message: value, extra: extra, sequence: sequence, segment: part, index: index})
	}
	if len(inputs) == 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return fmt.Errorf("begin modem SMS batch snapshot: %w", err)
	}
	defer tx.Rollback()
	cache := make(map[modemSMSQueryKey][]modemSMSCandidate)
	var evidence []modemSMSBatchPart
	now := time.Now().UTC()
	for index := range inputs {
		input := &inputs[index]
		key := modemSMSQueryKey{
			device: input.message.DeviceID, imei: input.message.ModemIMEI, peer: input.message.Peer,
			direction: modemSMSDirection(input.message.Direction),
			reference: input.extra.Concat.Reference, total: input.extra.Concat.Total,
		}
		candidates, loaded := cache[key]
		if !loaded {
			candidates, err = listModemSMSCandidates(ctx, tx, input.message, input.extra)
			if err != nil {
				return err
			}
			cache[key] = candidates
			for _, candidate := range candidates {
				if candidate.extra.AT == nil || candidate.extra.Complete || now.Sub(candidate.message.CreatedAt) > modemSMSReassemblyWindow {
					continue
				}
				// Isolated parts remain evidence, not candidates that disappear
				// and make another generation look unique on the next scan.
				for sequence, segment := range candidate.extra.AT.Segments {
					if segment.Hash != "" {
						evidence = append(evidence, modemSMSBatchPart{message: candidate.message, extra: candidate.extra, sequence: sequence, segment: segment})
					}
				}
			}
		}
		known := 0
		for _, candidate := range candidates {
			if candidate.extra.AT != nil && candidate.extra.AT.Segments[input.sequence].Hash == input.segment.Hash {
				known++
			}
		}
		input.known = known == 1
		if !input.known {
			evidence = append(evidence, *input)
		}
	}
	for _, input := range inputs {
		if input.known {
			continue
		}
		conflict := input.segment.SCTS == nil
		hashes := make(map[string]string)
		for _, other := range evidence {
			if !sameModemSMSBatchScope(input, other) {
				continue
			}
			if previous, exists := hashes[other.sequence]; exists && previous != other.segment.Hash {
				conflict = true
				break
			}
			hashes[other.sequence] = other.segment.Hash
		}
		values[input.index].modemBatchConflict = conflict
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("finish modem SMS batch snapshot: %w", err)
	}
	return nil
}

func sameModemSMSBatchScope(a, b modemSMSBatchPart) bool {
	return sameModemSMSHardware(a.message, b.message) && sameSMSSubscription(a.message, b.message) &&
		a.message.Peer == b.message.Peer && modemSMSDirection(a.message.Direction) == modemSMSDirection(b.message.Direction) &&
		a.extra.Concat.Reference == b.extra.Concat.Reference && a.extra.Concat.Total == b.extra.Concat.Total &&
		sameModemSMSProtocol(a.extra.AT, b.extra.AT) &&
		(a.segment.SCTS == nil || b.segment.SCTS == nil || sameModemSMSTime(a.segment.SCTS, b.segment.SCTS))
}
