package device

import (
	"context"
	"strings"

	"vocat/internal/modem"
)

const smsReceivedEventBufferSize = 64

// SMSReceivedEvent is a wake-up signal, not a snapshot of a SIM or message.
// DeviceID identifies physical hardware; consumers must re-read its current
// state. RescanAll requests a scan of all current devices after queue overflow.
type SMSReceivedEvent struct {
	DeviceID  string
	RescanAll bool
}

// SubscribeSMSReceivedEvents subscribes globally to incoming SMS indications,
// including from clients opened after subscribing. Canceling ctx removes the
// subscription and closes its channel. Slow subscribers receive an explicit
// RescanAll event instead of silently losing pending device notifications.
func (manager *Manager) SubscribeSMSReceivedEvents(ctx context.Context) (<-chan SMSReceivedEvent, error) {
	if manager == nil {
		return nil, ErrNotStarted
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	events := make(chan SMSReceivedEvent, smsReceivedEventBufferSize)
	manager.smsEventsMu.Lock()
	if manager.smsEventSubscribers == nil {
		manager.smsEventSubscribers = make(map[chan SMSReceivedEvent]struct{})
	}
	manager.smsEventSubscribers[events] = struct{}{}
	manager.smsEventsMu.Unlock()
	if done := ctx.Done(); done != nil {
		go func() {
			<-done
			manager.removeSMSReceivedEventSubscriber(events)
		}()
	}
	return events, nil
}

func (manager *Manager) removeSMSReceivedEventSubscriber(events chan SMSReceivedEvent) {
	manager.smsEventsMu.Lock()
	defer manager.smsEventsMu.Unlock()
	if _, ok := manager.smsEventSubscribers[events]; !ok {
		return
	}
	delete(manager.smsEventSubscribers, events)
	close(events)
}

func (manager *Manager) publishSMSReceivedEvent(deviceID string) {
	deviceID = strings.TrimSpace(deviceID)
	if deviceID == "" {
		return
	}
	manager.smsEventsMu.Lock()
	defer manager.smsEventsMu.Unlock()
	for events := range manager.smsEventSubscribers {
		select {
		case events <- SMSReceivedEvent{DeviceID: deviceID}:
		default:
			// Replace the backlog with a full rescan, covering both the dropped
			// devices and this notification. Repeated overflows must preserve
			// that request even when a previous RescanAll is still queued.
			// Publishers and cancellation share smsEventsMu, so only consumers
			// can remove entries while we drain, and none can refill or close it.
			for range len(events) {
				select {
				case <-events:
				default:
				}
			}
			select {
			case events <- SMSReceivedEvent{RescanAll: true}:
			default:
			}
		}
	}
}

func (manager *Manager) bindSMSNotifications(client modem.Client, deviceID string) {
	if source, ok := client.(modem.SMSNotificationClient); ok {
		// Capture only the physical ID. The callback never takes manager.mu or
		// opMu, issues AT commands, or retains a possibly stale SIM snapshot.
		source.SetSMSNotificationHandler(func() {
			manager.publishSMSReceivedEvent(deviceID)
		})
	}
}
