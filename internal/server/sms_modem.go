package server

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"vocat/internal/device"
	"vocat/internal/store"
)

// modemSMSRecord is shared by ingestion and deletion so both compare the same
// delivery evidence. Storage coordinates never enter a multipart fingerprint.
func modemSMSRecord(message device.SMSMessage) (store.SMSMessage, error) {
	timestamp := time.Now().UTC()
	if message.ServiceCenterTimestamp != nil {
		timestamp = message.ServiceCenterTimestamp.UTC()
	} else if message.DischargeTimestamp != nil {
		timestamp = message.DischargeTimestamp.UTC()
	}
	direction := "inbound"
	if message.Direction == device.SMSDirectionSubmitted {
		direction = "outbound"
	}
	extra := map[string]any{
		"modem_index": message.Index, "storage": message.Storage,
		"storage_status": message.StorageStatus, "encoding": message.Encoding,
		"concat": message.Concat, "decode_error": message.DecodeError,
		"status_code": message.StatusCode, "message_reference": message.MessageReference,
		"delivery_status": message.DeliveryStatus, "data_coding_scheme": message.DataCodingScheme,
		"keep_durable_id_on_rescan": true,
	}
	if message.Concat != nil && message.Concat.Total > 1 {
		fingerprint, err := modemSMSFingerprint(message)
		if err != nil {
			return store.SMSMessage{}, err
		}
		var scts *time.Time
		if message.ServiceCenterTimestamp != nil {
			utc := message.ServiceCenterTimestamp.UTC()
			scts = &utc
		}
		extra["at_concat"] = store.ModemSMSConcat{
			PID: message.ProtocolID, DCS: message.DataCodingScheme, UDH: modemSMSUDH(message),
			Segments: map[string]store.ModemSMSSegment{
				strconv.Itoa(message.Concat.Sequence): {Hash: fingerprint, SCTS: scts},
			},
		}
	}
	encoded, err := json.Marshal(extra)
	if err != nil {
		return store.SMSMessage{}, err
	}
	return store.SMSMessage{
		Peer: firstNonEmpty(message.From, message.To), Direction: direction,
		Body: message.Text, Timestamp: timestamp, Status: string(message.StorageStatus),
		Source: "cellular_at", PartsTotal: concatTotal(message.Concat),
		DeliveryState: message.DeliveryStatus, Read: message.StorageStatus == device.SMSStatusReceivedRead,
		Extra: encoded,
	}, nil
}

func modemSMSFingerprint(message device.SMSMessage) (string, error) {
	raw := strings.TrimSpace(message.RawPDU)
	payload := []byte(raw)
	if decoded, err := hex.DecodeString(raw); err == nil && len(decoded) > 1 && int(decoded[0])+1 < len(decoded) {
		// AT PDU mode includes an SMSC prefix; the TPDU is the delivery itself.
		payload = decoded[1+int(decoded[0]):]
	} else if raw == "" {
		// Some adapters only supply decoded fields. Include all delivery fields,
		// not just text, while excluding mutable modem storage bookkeeping.
		message.Index, message.Storage, message.StorageStatus, message.ModemLength = 0, "", "", 0
		var err error
		payload, err = json.Marshal(message)
		if err != nil {
			return "", err
		}
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func modemSMSUDH(message device.SMSMessage) string {
	data, err := hex.DecodeString(message.RawUserData)
	if err != nil || len(data) == 0 || int(data[0])+1 > len(data) {
		return ""
	}
	header := data[1 : int(data[0])+1]
	for offset := 0; offset+2 <= len(header); {
		kind, length := header[offset], int(header[offset+1])
		end := offset + 2 + length
		if end > len(header) {
			break
		}
		if (kind == 0x00 && length == 3) || (kind == 0x08 && length == 4) {
			header[end-1] = 0 // Keep reference type/ports, omit only the sequence.
		}
		offset = end
	}
	return hex.EncodeToString(header)
}
