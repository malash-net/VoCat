package device

import (
	"context"
	"strconv"
	"strings"

	"vocat/internal/modem"
)

// smsEventBuffer lets a burst of arrivals on several modems queue one wake-up
// per modem; further arrivals are coalesced because a sync reads all storage.
const smsEventBuffer = 16

// SubscribeSMSEvents subscribes to new-message indications (+CMTI, +CDSI and
// direct +CMT/+CDS) from every modem AT session. The channel carries the
// physical device ID and is only a wake-up signal; callers must list modem
// storage. The subscription ends and the channel closes when ctx is done.
func (manager *Manager) SubscribeSMSEvents(ctx context.Context) (<-chan string, error) {
	if manager == nil {
		return nil, ErrNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	events := make(chan string, smsEventBuffer)
	manager.smsEventsMu.Lock()
	if manager.smsEventSubscribers == nil {
		manager.smsEventSubscribers = make(map[chan string]struct{})
	}
	manager.smsEventSubscribers[events] = struct{}{}
	manager.smsEventsMu.Unlock()
	if done := ctx.Done(); done != nil {
		go func() {
			<-done
			manager.smsEventsMu.Lock()
			defer manager.smsEventsMu.Unlock()
			if _, ok := manager.smsEventSubscribers[events]; ok {
				delete(manager.smsEventSubscribers, events)
				close(events)
			}
		}()
	}
	return events, nil
}

// publishSMSEvent never blocks: it runs on the AT session's URC path.
func (manager *Manager) publishSMSEvent(deviceID string) {
	manager.smsEventsMu.Lock()
	defer manager.smsEventsMu.Unlock()
	for events := range manager.smsEventSubscribers {
		select {
		case events <- deviceID:
		default:
		}
	}
}

// watchSMSURCs routes SMS arrival URCs from a freshly opened client to the
// SMS event subscribers.
func (manager *Manager) watchSMSURCs(client modem.Client, deviceID string) {
	notifier, ok := client.(modem.URCNotifier)
	if !ok || strings.TrimSpace(deviceID) == "" {
		return
	}
	notifier.SetURCHandler(func(line string) {
		if isSMSArrivalURC(line) {
			manager.publishSMSEvent(deviceID)
		}
	})
}

func isSMSArrivalURC(line string) bool {
	upper := strings.ToUpper(strings.TrimSpace(line))
	for _, prefix := range []string{"+CMTI:", "+CMT:", "+CDSI:", "+CDS:"} {
		if strings.HasPrefix(upper, prefix) {
			return true
		}
	}
	return false
}

// ensureSMSIndications makes the modem store new SMS and announce them with
// +CMTI (AT+CNMI <mode>=2, <mt>=1). It is re-checked on every storage scan
// because firmware can reset CNMI after a reboot, CFUN cycle or profile
// switch. <bm>, <ds> and <bfr> are preserved so cell broadcast and delivery
// report routing stay as configured. Failures are not fatal: periodic storage
// scans still find the messages, just later.
func (manager *Manager) ensureSMSIndications(ctx context.Context, client modem.Client) {
	response, err := manager.command(ctx, client, "AT+CNMI?")
	if err != nil {
		return
	}
	values, ok := parseCNMI(response)
	if !ok {
		values = []int{0, 0, 0, 0, 0}
	}
	if values[0] != 0 && values[1] == 1 {
		return
	}
	values[0], values[1] = 2, 1
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.Itoa(value)
	}
	if _, err := manager.command(ctx, client, "AT+CNMI="+strings.Join(parts, ",")); err != nil {
		_, _ = manager.command(ctx, client, "AT+CNMI=2,1")
	}
}

// parseCNMI reads "+CNMI: <mode>,<mt>,<bm>,<ds>,<bfr>". At least mode and mt
// must be present; missing trailing values are returned as zero.
func parseCNMI(response modem.Response) ([]int, bool) {
	for _, line := range response.Lines {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(strings.ToUpper(line), "+CNMI:") {
			continue
		}
		fields := strings.Split(strings.TrimSpace(line[len("+CNMI:"):]), ",")
		if len(fields) < 2 || len(fields) > 5 {
			return nil, false
		}
		values := make([]int, 5)
		for index, field := range fields {
			value, err := strconv.Atoi(strings.TrimSpace(field))
			if err != nil || value < 0 {
				return nil, false
			}
			values[index] = value
		}
		return values, true
	}
	return nil, false
}
