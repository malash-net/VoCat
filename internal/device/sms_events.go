package device

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"time"

	"vocat/internal/modem"
)

// smsIndicationRetryDelay keeps a modem that rejects or ignores AT+CNMI from
// stalling its AT port on every storage scan.
const smsIndicationRetryDelay = 10 * time.Minute

// SMSArrivals receives a value when any modem announces a new SMS with
// +CMTI. It is only a wake-up signal: the receiver must list modem storage.
// The buffer holds one value, so a burst collapses into a single wake-up and
// an arrival during the receiver's scan wakes it once more afterwards.
func (manager *Manager) SMSArrivals() <-chan struct{} {
	return manager.smsArrivals
}

// watchSMSURCs signals SMSArrivals for +CMTI from a freshly opened client.
// The handler runs on the AT session's URC path, so it never blocks.
func (manager *Manager) watchSMSURCs(client modem.Client) {
	notifier, ok := client.(modem.URCNotifier)
	if !ok {
		return
	}
	notifier.SetURCHandler(func(line string) {
		if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(line)), "+CMTI:") {
			return
		}
		select {
		case manager.smsArrivals <- struct{}{}:
		default:
			// A wake-up is already pending; its scan will see this message.
		}
	})
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
