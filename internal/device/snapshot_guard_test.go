package device

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"vocat/internal/modem"
)

// lenientATClient answers every command with a bare CommandError and records
// the commands it saw. It lets snapshot tests exercise the full readSnapshot
// sequence without enumerating every step of the transcript.
type lenientATClient struct {
	mu        sync.Mutex
	commands  []string
	cgsnDelay time.Duration
	cgsnIMEI  string
}

func (c *lenientATClient) Execute(ctx context.Context, command string) (modem.Response, error) {
	c.mu.Lock()
	c.commands = append(c.commands, command)
	c.mu.Unlock()
	if command == "ATI" {
		return okResponse("Qualcomm", "PCIe/MHI WWAN modem", "Revision: native-410"), nil
	}
	if command == "AT+CGSN" && c.cgsnDelay > 0 {
		select {
		case <-time.After(c.cgsnDelay):
		case <-ctx.Done():
		}
	}
	if command == "AT+CGSN" && c.cgsnIMEI != "" {
		return okResponse("+CGSN: " + c.cgsnIMEI), nil
	}
	return modem.Response{}, &modem.CommandError{Command: command, Final: "ERROR"}
}

func (c *lenientATClient) WaitURC(context.Context, func(string) bool) (string, error) {
	return "", errors.New("no URC")
}

func (c *lenientATClient) Close() error { return nil }

func (c *lenientATClient) saw(command string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, seen := range c.commands {
		if seen == command {
			return true
		}
	}
	return false
}

func TestNativeSnapshotSkipsCGSN(t *testing.T) {
	const cachedIMEI = "867123456789012"
	candidate := modem.Candidate{ID: "mhi-wwan0", QMIControl: "/dev/wwan0qmi0"}
	for _, test := range []struct {
		name, backend string
		retry         bool
	}{
		{name: "AT keeps cache", backend: "at"},
		{name: "QMI", backend: "qmi"},
		{name: "QMI retries existing fallback", backend: "qmi", retry: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			client := &lenientATClient{cgsnIMEI: "866241014372802"}
			qmiCalls := 0
			manager := &Manager{commandTimeout: time.Second, qmiRadioOpener: func(context.Context, string) (qmiRadioSession, error) {
				qmiCalls++
				if test.retry && qmiCalls == 1 {
					return nil, errors.New("transient DMS failure")
				}
				return &fakeQMIRadioSession{imei: "861716070416510"}, nil
			}}
			snapshot, err := manager.readSnapshot(context.Background(), candidate.ID, candidate, test.backend, "", &Snapshot{IMEI: cachedIMEI}, client)
			if err != nil || client.saw("AT+CGSN") || client.saw("AT+CGSN=1") {
				t.Fatalf("native snapshot = %+v, %v; commands = %v", snapshot, err, client.commands)
			}
			wantIMEI, wantCalls := cachedIMEI, 0
			if test.backend == "qmi" {
				wantIMEI, wantCalls = "861716070416510", 1
				if test.retry {
					wantCalls++
				}
			}
			if snapshot.IMEI != wantIMEI || qmiCalls != wantCalls {
				t.Fatalf("IMEI = %q, QMI calls = %d; want %q, %d", snapshot.IMEI, qmiCalls, wantIMEI, wantCalls)
			}
		})
	}
}

func TestManagerRefreshBoundsCGSNTimeout(t *testing.T) {
	client := &lenientATClient{cgsnDelay: 5 * time.Second}
	manager, id := newStartedTestManager(t, client)

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	start := time.Now()
	snapshot, err := manager.Refresh(ctx, id)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// CGSN times out after CommandTimeout (1s in the test manager); the rest
	// of the snapshot is immediate. An un-bounded CGSN would wait for the
	// 4s outer deadline (or worse, a real 30s refresh deadline).
	if elapsed > 3*time.Second {
		t.Fatalf("Refresh took %s; CGSN was not bounded by CommandTimeout", elapsed)
	}
	if !client.saw("AT+CGSN") {
		t.Fatalf("CGSN was never sent; commands = %v", client.commands)
	}
	if snapshot.IMEI != "" {
		t.Fatalf("IMEI = %q, want empty after CGSN timeout", snapshot.IMEI)
	}
}

// A missing SIM must not fall back to the QMI UIM ICCID read: without a READY
// card that call blocks until its long timeout and starves the AT terminal
// behind the device lock.
func TestManagerRefreshSkipsQMIICCIDWithoutReadySIM(t *testing.T) {
	client := &lenientATClient{cgsnIMEI: "866241014372802"}
	manager, err := NewManager(Options{
		Discoverer: staticDiscoverer{candidates: []modem.Candidate{{
			ID:               "mhi-wwan0",
			Product:          "PCIe/MHI WWAN modem",
			QMIControl:       "/dev/wwan0qmi0",
			NetworkInterface: "wwan0",
			ATPort:           modem.Port{Path: "/dev/wwan0at0", Name: "wwan0at0", Role: modem.PortRoleAT},
		}}},
		Opener: &staticOpener{client: client},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = manager.Stop(context.Background()) })

	qmiCalls := 0
	manager.qmiRadioOpener = func(context.Context, string) (qmiRadioSession, error) {
		qmiCalls++
		return nil, errors.New("QMI should not be opened without a SIM")
	}
	if err := manager.SetBackend("mhi-wwan0", "qmi"); err != nil {
		t.Fatal(err)
	}

	snapshot, err := manager.Refresh(context.Background(), "mhi-wwan0")
	if err != nil {
		t.Fatalf("Refresh: %v", err)
	}
	// The initial DMS read and its existing fallback both fail; the UIM
	// ICCID fallback must still be skipped without a READY SIM.
	if qmiCalls != 2 {
		t.Fatalf("qmiRadioOpener called %d times, want 2 DMS attempts and no UIM ICCID read", qmiCalls)
	}
	if client.saw("AT+CGSN") || client.saw("AT+CGSN=1") || snapshot.IMEI != "" {
		t.Fatalf("native IMEI fell back to AT: %q, commands = %v", snapshot.IMEI, client.commands)
	}
	for _, warning := range snapshot.Warnings {
		if strings.Contains(warning, "QMI UIM") {
			t.Fatalf("unexpected QMI ICCID warning: %q", warning)
		}
	}
}
