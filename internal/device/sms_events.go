package device

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"vocat/internal/modem"
)

// smsIndicationRetryDelay keeps a modem that rejects or ignores AT+CNMI from
// stalling its AT port on every storage scan.
const smsIndicationRetryDelay = 10 * time.Minute

// SMSEvents is one subscription to modem SMS arrival indications. Arrivals
// are coalesced per physical device: any number of indications from one modem
// before the next Take collapse into a single pending entry, so a burst on one
// modem can never crowd out another modem's wake-up.
type SMSEvents struct {
	mu      sync.Mutex
	pending map[string]struct{}
	wake    chan struct{}
}

// NewSMSEvents returns an empty subscription. Manager creates its own through
// SubscribeSMSEvents; other event sources and tests can feed one with Add.
func NewSMSEvents() *SMSEvents {
	return &SMSEvents{pending: make(map[string]struct{}), wake: make(chan struct{}, 1)}
}

// Wake receives a value whenever at least one device became pending since the
// last Take. It is closed when the subscription ends.
func (events *SMSEvents) Wake() <-chan struct{} { return events.wake }

// Take returns and clears the devices with pending arrivals. An arrival that
// lands after Take is kept for the next Take and re-signals Wake.
func (events *SMSEvents) Take() []string {
	events.mu.Lock()
	defer events.mu.Unlock()
	devices := make([]string, 0, len(events.pending))
	for deviceID := range events.pending {
		devices = append(devices, deviceID)
	}
	clear(events.pending)
	sort.Strings(devices)
	return devices
}

// Add marks deviceID as having a pending arrival. It never blocks.
func (events *SMSEvents) Add(deviceID string) {
	events.mu.Lock()
	events.pending[deviceID] = struct{}{}
	events.mu.Unlock()
	select {
	case events.wake <- struct{}{}:
	default:
		// A wake-up is already pending; its Take will include this device.
	}
}

// SubscribeSMSEvents subscribes to new-message indications (+CMTI, +CDSI and
// direct +CMT/+CDS) from every modem AT session. Pending entries carry the
// physical device ID and are only wake-up signals; callers must list modem
// storage. The subscription ends and Wake closes when ctx is done.
func (manager *Manager) SubscribeSMSEvents(ctx context.Context) (*SMSEvents, error) {
	if manager == nil {
		return nil, ErrNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	events := NewSMSEvents()
	manager.smsEventsMu.Lock()
	if manager.smsEventSubscribers == nil {
		manager.smsEventSubscribers = make(map[*SMSEvents]struct{})
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
				close(events.wake)
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
		events.Add(deviceID)
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
// scans still find the messages, just later. Each command gets its own short
// budget instead of the caller's scan deadline, and a failure backs off so a
// timed-out command does not hold the AT session on every scan. The caller
// must hold state.opMu.
func (manager *Manager) ensureSMSIndications(ctx context.Context, state *managedDevice, client modem.Client) {
	now := time.Now()
	if now.Before(state.smsIndicationRetryAt) {
		return
	}
	command := func(text string) (modem.Response, error) {
		commandCtx, cancel := context.WithTimeout(ctx, manager.commandTimeout)
		defer cancel()
		return manager.command(commandCtx, client, text)
	}
	response, err := command("AT+CNMI?")
	if err != nil {
		state.smsIndicationRetryAt = now.Add(smsIndicationRetryDelay)
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
	if _, err := command("AT+CNMI=" + strings.Join(parts, ",")); err != nil {
		if errors.Is(err, modem.ErrCommandTimeout) || errors.Is(err, context.DeadlineExceeded) {
			state.smsIndicationRetryAt = now.Add(smsIndicationRetryDelay)
			return
		}
		if _, err := command("AT+CNMI=2,1"); err != nil {
			state.smsIndicationRetryAt = now.Add(smsIndicationRetryDelay)
		}
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
