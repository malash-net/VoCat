package server

import (
	"testing"

	"vocat/internal/device"
)

func TestModemSMSFingerprintUsesTPDU(t *testing.T) {
	first, err := modemSMSFingerprint(device.SMSMessage{Storage: "SM", Index: 20, RawPDU: "00400102AA"})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		pdu  string
		same bool
	}{
		{"different SMSC and storage", "029100400102aa", true},
		{"reused slot with new PDU", "00400102AB", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := modemSMSFingerprint(device.SMSMessage{Storage: "ME", Index: 5, RawPDU: test.pdu})
			if err != nil || (got == first) != test.same {
				t.Fatalf("fingerprint = %q, %v; equal=%v", got, err, test.same)
			}
		})
	}
}

func TestModemSMSUDHPreservesProtocolIdentity(t *testing.T) {
	first := modemSMSUDH(device.SMSMessage{RawUserData: "0B000307020105040B8423F0"})
	for _, test := range []struct {
		name string
		data string
		same bool
	}{
		{"next segment", "0B000307020205040B8423F0", true},
		{"different port", "0B000307020205040B8523F0", false},
		{"16-bit reference", "0C08040007020105040B8423F0", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := modemSMSUDH(device.SMSMessage{RawUserData: test.data})
			if (got == first) != test.same {
				t.Fatalf("UDH signature = %q, want equal=%v", got, test.same)
			}
		})
	}
}
