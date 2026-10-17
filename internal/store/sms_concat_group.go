package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// concatGroupJoinWindow bounds how far apart two segments of one long SMS may
// be timestamped and still be merged. It keeps a reused UDH reference from
// joining a long-abandoned partial message.
const concatGroupJoinWindow = 24 * time.Hour

// concatGroupCandidateLimit bounds the rows inspected per resolution; only
// recent partial groups for one sender/reference/total can be candidates.
const concatGroupCandidateLimit = 200

// ConcatSegment describes one physical segment of a long SMS read from modem
// storage. PartKey must identify this delivery of the segment independent of
// its storage slot (for example a hash of the raw PDU, which includes the SMSC
// timestamp), so a rescan maps it back to the same group after other slots
// were deleted or reused.
type ConcatSegment struct {
	ModemIMEI string
	DeviceID  string
	Peer      string
	Reference int
	Total     int
	Sequence  int
	PartKey   string
	Text      string
	Timestamp time.Time
}

// ResolveConcatSMSGroup returns the message ID of the stored group a segment
// belongs to, using only persisted state so the answer does not depend on
// which other segments are still in modem storage or which slots they occupy:
//
//  1. a group that already folded this exact segment (PartKey), or a legacy
//     group without part keys that holds the same text at this sequence;
//  2. otherwise the incomplete group still missing this sequence whose
//     timestamp is closest to the segment, within concatGroupJoinWindow.
//
// found=false means the segment starts a new group.
func (s *Store) ResolveConcatSMSGroup(ctx context.Context, source string, segment ConcatSegment) (string, bool, error) {
	hardwareKey := smsHardwareKey(segment.ModemIMEI, segment.DeviceID)
	prefix := ConcatMessageIDPrefix + source + ":"
	suffix := ":" + hardwareKey + ":" + segment.Peer + ":" +
		strconv.Itoa(segment.Reference) + ":" + strconv.Itoa(segment.Total)
	rows, err := s.db.QueryContext(ctx, `
		SELECT message_id, message_time, extra_json FROM sms_messages
		WHERE COALESCE(NULLIF(modem_imei, ''), 'device:' || device_id) = ?
			AND peer = ? AND substr(message_id, 1, ?) = ?
		ORDER BY id DESC
		LIMIT ?`,
		hardwareKey, segment.Peer, len(prefix), prefix, concatGroupCandidateLimit,
	)
	if err != nil {
		return "", false, fmt.Errorf("list concatenated SMS groups: %w", err)
	}
	defer rows.Close()
	type candidate struct {
		messageID string
		distance  time.Duration
	}
	var join *candidate
	for rows.Next() {
		var messageID, extraText string
		var messageTime int64
		if err := rows.Scan(&messageID, &messageTime, &extraText); err != nil {
			return "", false, fmt.Errorf("scan concatenated SMS group: %w", err)
		}
		if !strings.HasSuffix(messageID, suffix) {
			continue
		}
		group := decodeConcatGroup(json.RawMessage(extraText))
		if group.holds(segment) {
			return messageID, true, nil
		}
		if group.complete {
			continue
		}
		if _, taken := group.parts[segment.Sequence]; taken {
			continue
		}
		distance := segment.Timestamp.Sub(time.Unix(messageTime, 0))
		if distance < 0 {
			distance = -distance
		}
		if distance > concatGroupJoinWindow {
			continue
		}
		// Rows are newest first, so a tie keeps the most recent group.
		if join == nil || distance < join.distance {
			join = &candidate{messageID: messageID, distance: distance}
		}
	}
	if err := rows.Err(); err != nil {
		return "", false, fmt.Errorf("iterate concatenated SMS groups: %w", err)
	}
	if join != nil {
		return join.messageID, true, nil
	}
	return "", false, nil
}

// ConcatSMSHoldsSegment reports whether a stored row already contains the
// given modem segment; it is the deletion-side counterpart of
// ResolveConcatSMSGroup's first rule.
func ConcatSMSHoldsSegment(message SMSMessage, segment ConcatSegment) bool {
	suffix := ":" + smsHardwareKey(segment.ModemIMEI, segment.DeviceID) + ":" + segment.Peer + ":" +
		strconv.Itoa(segment.Reference) + ":" + strconv.Itoa(segment.Total)
	if !isConcatSMSMessageID(message.MessageID) || !strings.HasSuffix(message.MessageID, suffix) {
		return false
	}
	return decodeConcatGroup(message.Extra).holds(segment)
}

type concatGroup struct {
	parts    map[int]string
	keys     map[int]string
	complete bool
}

func decodeConcatGroup(extra json.RawMessage) concatGroup {
	group := concatGroup{parts: map[int]string{}, keys: map[int]string{}}
	document, err := decodeJSONObject(extra)
	if err != nil {
		return group
	}
	group.complete, _ = document["concat_complete"].(bool)
	for field, target := range map[string]map[int]string{"concat_parts": group.parts, "concat_part_keys": group.keys} {
		values, _ := document[field].(map[string]any)
		for key, value := range values {
			sequence, err := strconv.Atoi(key)
			if text, ok := value.(string); err == nil && sequence > 0 && ok {
				target[sequence] = text
			}
		}
	}
	return group
}

func (group concatGroup) holds(segment ConcatSegment) bool {
	if key, ok := group.keys[segment.Sequence]; ok {
		return segment.PartKey != "" && key == segment.PartKey
	}
	// Parts folded before part keys existed can only be matched by content;
	// the merge backfills the key the first time such a part is rescanned.
	text, ok := group.parts[segment.Sequence]
	return ok && text == segment.Text
}
