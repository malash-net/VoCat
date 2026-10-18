package device

import (
	"strings"

	"vocat/internal/modem"
)

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
