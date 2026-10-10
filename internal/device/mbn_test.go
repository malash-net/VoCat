package device

import (
	"context"
	"errors"
	"strings"
	"testing"

	"vocat/internal/modem"
)

type fakeMBNExecutor struct {
	list    []string
	autosel string
	cmds    []string
}

func (f *fakeMBNExecutor) Execute(_ context.Context, command string) (modem.Response, error) {
	f.cmds = append(f.cmds, command)
	switch {
	case strings.Contains(command, `"List"`):
		return modem.Response{Lines: f.list, Final: "OK"}, nil
	case strings.Contains(command, `"AutoSel"`) && !strings.Contains(command, ","):
		return modem.Response{Lines: []string{`+QMBNCFG: "AutoSel",` + f.autosel}, Final: "OK"}, nil
	default:
		return modem.Response{Lines: []string{"OK"}, Final: "OK"}, nil
	}
}

func ceProfiles(selectedCMCC bool) []string {
	cmccSel, cmccAct := "0", "0"
	cuSel, cuAct := "0", "0"
	if selectedCMCC {
		cmccSel, cmccAct = "1", "1"
	} else {
		cuSel, cuAct = "1", "1"
	}
	return []string{
		`+QMBNCFG: "List",0,` + cmccSel + `,` + cmccAct + `,"Volte_OpenMkt-Commercial-CMCC",0x05012061,000000000`,
		`+QMBNCFG: "List",1,` + cuSel + `,` + cuAct + `,"OpenMkt-Commercial-CU",0x05011501,000000000`,
		`+QMBNCFG: "List",2,0,0,"OpenMkt-Commercial-CT",0x05011316,000000000`,
	}
}

func rowProfiles(selectedCMCC bool) []string {
	return append(ceProfiles(selectedCMCC),
		`+QMBNCFG: "List",3,0,0,"ROW_Generic_3GPP",0x05000000,000000000`,
	)
}

func TestIsEC20CandidateExcludesML307(t *testing.T) {
	if isEC20Candidate(modem.Candidate{VendorID: "2ecc", ProductID: "3012", Product: "ML307A"}) {
		t.Fatal("ML307 must not use Quectel MBN controls")
	}
}

func TestKnownMBNCarrierOpenMarketNames(t *testing.T) {
	carrier, country, known := knownMBNCarrier("OpenMkt-Commercial-CU")
	if !known || carrier != "China Unicom" || country != "CN" {
		t.Fatalf("CU: %q %q known=%v", carrier, country, known)
	}
	carrier, _, known = knownMBNCarrier("Volte_OpenMkt-Commercial-CMCC")
	if !known || carrier != "China Mobile" {
		t.Fatalf("CMCC: %q known=%v", carrier, known)
	}
	carrier, _, known = knownMBNCarrier("OpenMkt-Commercial-CT")
	if !known || carrier != "China Telecom" {
		t.Fatalf("CT: %q known=%v", carrier, known)
	}
}

func TestNormalizeCardMBNProfile(t *testing.T) {
	got, err := NormalizeCardMBNProfile("cu")
	if err != nil || got != MBNProfileCU {
		t.Fatalf("cu: %q %v", got, err)
	}
	got, err = NormalizeCardMBNProfile("Volte_OpenMkt-Commercial-CMCC")
	if err != nil || got != MBNProfileCMCC {
		t.Fatalf("cmcc: %q %v", got, err)
	}
	got, err = NormalizeCardMBNProfile("auto")
	if err != nil || got != "" {
		t.Fatalf("auto: %q %v", got, err)
	}
	if _, err = NormalizeCardMBNProfile("bad name"); err == nil {
		t.Fatal("expected invalid MBN token to fail")
	}
}

func TestReconcileMBNLeavesMatchingChinaMobileAlone(t *testing.T) {
	fake := &fakeMBNExecutor{list: ceProfiles(true), autosel: "1"}
	changed, _, selected, err := reconcileMBNSelection(context.Background(), fake, "46000", "")
	if err != nil {
		t.Fatal(err)
	}
	if changed || selected != MBNProfileCMCC {
		t.Fatalf("CMCC SIM should keep CMCC MBN, changed=%v selected=%q cmds=%v", changed, selected, fake.cmds)
	}
	for _, cmd := range fake.cmds {
		if strings.Contains(cmd, `"Select"`) || strings.Contains(cmd, `"AutoSel",0`) {
			t.Fatalf("unexpected mutating command %q", cmd)
		}
	}
}

func TestReconcileMBNWithoutOverrideNeedsROWGeneric(t *testing.T) {
	fake := &fakeMBNExecutor{list: ceProfiles(true), autosel: "1"}
	_, _, _, err := reconcileMBNSelection(context.Background(), fake, "45400", "")
	if err == nil || !strings.Contains(err.Error(), rowGeneric3GPPMBN) {
		t.Fatalf("CE image without ROW_Generic should keep the original heuristic error, err=%v", err)
	}
}

func TestReconcileMBNSelectsROWGenericWhenPresent(t *testing.T) {
	fake := &fakeMBNExecutor{list: rowProfiles(true), autosel: "1"}
	changed, previous, selected, err := reconcileMBNSelection(context.Background(), fake, "45400", "")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || previous != MBNProfileCMCC || selected != rowGeneric3GPPMBN {
		t.Fatalf("changed=%v previous=%q selected=%q cmds=%v", changed, previous, selected, fake.cmds)
	}
}

func TestReconcileMBNOverrideSelectsCUOnCEFirmware(t *testing.T) {
	fake := &fakeMBNExecutor{list: ceProfiles(true), autosel: "1"}
	changed, previous, selected, err := reconcileMBNSelection(context.Background(), fake, "45400", "CU")
	if err != nil {
		t.Fatal(err)
	}
	if !changed || previous != MBNProfileCMCC || selected != MBNProfileCU {
		t.Fatalf("changed=%v previous=%q selected=%q cmds=%v", changed, previous, selected, fake.cmds)
	}
	joined := strings.Join(fake.cmds, "\n")
	if !strings.Contains(joined, `"AutoSel",0`) || !strings.Contains(joined, `"Select","OpenMkt-Commercial-CU"`) {
		t.Fatalf("missing AT commands: %v", fake.cmds)
	}
}

func TestReconcileMBNOverrideKeepsCMCCAndDisablesAutoSel(t *testing.T) {
	fake := &fakeMBNExecutor{list: ceProfiles(true), autosel: "1"}
	changed, previous, selected, err := reconcileMBNSelection(context.Background(), fake, "50212", "CMCC")
	if err != nil {
		t.Fatal(err)
	}
	if changed || previous != MBNProfileCMCC || selected != MBNProfileCMCC {
		t.Fatalf("changed=%v previous=%q selected=%q", changed, previous, selected)
	}
	joined := strings.Join(fake.cmds, "\n")
	if !strings.Contains(joined, `"AutoSel",0`) {
		t.Fatalf("expected AutoSel disable, cmds=%v", fake.cmds)
	}
	if strings.Contains(joined, `"Select","`) {
		t.Fatalf("should not re-select MBN: %v", fake.cmds)
	}
}

func TestReconcileMBNOverrideMissingProfile(t *testing.T) {
	fake := &fakeMBNExecutor{list: ceProfiles(true), autosel: "1"}
	_, _, _, err := reconcileMBNSelection(context.Background(), fake, "45400", "ROW_Generic_3GPP")
	if err == nil {
		t.Fatal("expected missing ROW profile to fail")
	}
}
func TestResolveMBNOverrideRejectsSubstringCustomName(t *testing.T) {
	profiles := parseMBNProfiles(modem.Response{Lines: ceProfiles(true)})
	if _, err := resolveMBNOverride(profiles, "OpenMkt"); err == nil {
		t.Fatal("expected unmatched custom name to fail")
	}
	got, err := resolveMBNOverride(profiles, "CU")
	if err != nil || got != MBNProfileCU {
		t.Fatalf("CU alias = %q %v", got, err)
	}
}

func TestCardMBNOverridePropagatesLookupError(t *testing.T) {
	manager := &Manager{
		mbnProfileForICCID: func(context.Context, string) (string, error) {
			return "", errors.New("db down")
		},
	}
	if _, err := manager.cardMBNOverride(context.Background(), "8985200014631193805"); err == nil {
		t.Fatal("expected lookup error")
	}
}

func TestCardMBNOverridePropagatesInvalidProfile(t *testing.T) {
	manager := &Manager{
		mbnProfileForICCID: func(context.Context, string) (string, error) {
			return "not a profile", nil
		},
	}
	if _, err := manager.cardMBNOverride(context.Background(), "8985200014631193805"); err == nil {
		t.Fatal("expected invalid profile error")
	}
}

func TestMBNRestartRetainsUnsynchronizedOwner(t *testing.T) {
	const iccid = "8900000000000000001"
	client := &transcriptClient{steps: []clientStep{
		{command: "ATI", response: okResponse("Quectel", "EC20", "Revision: test")},
		{command: "AT+CPIN?", response: okResponse("+CPIN: READY")},
		{command: "AT+CCID", response: okResponse("+CCID: " + iccid)},
		{command: "AT+CIMI", response: okResponse("234150000000001")},
		{command: "AT+CRSM=176,28486,0,0,17", response: okResponse()},
		{command: "AT+CSQ", response: okResponse()},
		{command: `AT+QENG="servingcell"`, response: okResponse()},
		{command: "AT+COPS?", response: okResponse()},
		{command: "AT+CEREG?", response: okResponse("+CEREG: 0,1")},
		{command: "AT+CGSN", response: okResponse("867123456789012")},
		{command: "AT+CFUN?", response: okResponse("+CFUN: 1")},
		{command: "AT+CNUM", response: okResponse(`+CNUM: "","+447700900001",145`)},
		{command: "AT+CFUN?", response: okResponse("+CFUN: 1")},
		{command: `AT+QMBNCFG="List"`, response: okResponse(ceProfiles(true)...)},
		{command: `AT+QMBNCFG="AutoSel"`, response: okResponse(`+QMBNCFG: "AutoSel",1`)},
		{command: `AT+QMBNCFG="AutoSel",0`, response: okResponse()},
		{command: `AT+QMBNCFG="Select","OpenMkt-Commercial-CU"`, response: okResponse()},
		{command: "AT+CFUN=1,1", err: modem.ErrSessionUnsynchronized},
	}}
	manager, id := newStartedTestManager(t, client)
	manager.mbnProfileForICCID = func(context.Context, string) (string, error) { return "CU", nil }
	state, _ := manager.lookup(id)
	manager.setResult(id, state, &Snapshot{ICCID: iccid, IdentityFilesRead: true, Responsive: true}, nil)
	if err := manager.ReconcileEC20MBNAfterProfileSwitch(context.Background(), id, iccid); !errors.Is(err, modem.ErrSessionUnsynchronized) || client.closeCount != 0 || state.client != client {
		t.Fatalf("MBN restart = %v; close count = %d, owner retained = %t", err, client.closeCount, state.client == client)
	}
	if !state.opMu.TryLock() {
		t.Fatal("MBN restart retained the operation lock")
	}
	state.opMu.Unlock()
	client.assertDone(t)
}

func TestCardMBNOverrideEmptyWhenUnset(t *testing.T) {
	got, err := (*Manager)(nil).cardMBNOverride(context.Background(), "8985200014631193805")
	if err != nil || got != "" {
		t.Fatalf("nil manager = %q %v", got, err)
	}
	manager := &Manager{}
	got, err = manager.cardMBNOverride(context.Background(), "8985200014631193805")
	if err != nil || got != "" {
		t.Fatalf("unset callback = %q %v", got, err)
	}
}
