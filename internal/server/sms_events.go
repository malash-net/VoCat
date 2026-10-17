package server

import (
	"context"
	"strings"
	"sync"
	"time"

	"vocat/internal/device"
)

// SMS events are an optional latency improvement; polling remains the fallback
// for controllers without this capability and for lost modem indications.
type smsReceivedEventController interface {
	SubscribeSMSReceivedEvents(context.Context) (<-chan device.SMSReceivedEvent, error)
}

const smsSyncDirtyLimit = 64

type smsSyncTask struct {
	fullScan    bool
	physicalIDs map[string]struct{}
}

// The collector never waits for modem I/O. Its bounded pending set is separate
// from the claimed task, so indications received during a scan cause a rescan.
type smsSyncPending struct {
	mu   sync.Mutex
	task smsSyncTask
	wake chan struct{}
}

func newSMSSyncPending() *smsSyncPending {
	return &smsSyncPending{wake: make(chan struct{}, 1)}
}

func (pending *smsSyncPending) add(physicalID string, fullScan bool) bool {
	pending.mu.Lock()
	defer pending.mu.Unlock()
	if pending.task.fullScan || (!fullScan && physicalID == "") {
		return false
	}
	if !fullScan {
		if _, exists := pending.task.physicalIDs[physicalID]; exists {
			return false
		}
		fullScan = len(pending.task.physicalIDs) >= smsSyncDirtyLimit
	}
	if fullScan {
		// This covers only work not yet claimed, never the next generation of
		// events that arrives while the executor is scanning.
		pending.task = smsSyncTask{fullScan: true}
	} else {
		if pending.task.physicalIDs == nil {
			pending.task.physicalIDs = make(map[string]struct{})
		}
		pending.task.physicalIDs[physicalID] = struct{}{}
	}
	select {
	case pending.wake <- struct{}{}:
	default:
	}
	return true
}

func (pending *smsSyncPending) take() smsSyncTask {
	pending.mu.Lock()
	defer pending.mu.Unlock()
	task := pending.task
	pending.task = smsSyncTask{}
	return task
}

// StartSMSSyncLoop persists inbound cellular SMS on indications and periodic
// catch-up scans. The first tick stays delayed to leave startup SIM/AKA alone;
// event-triggered scans use the same VoWiFi and modem-storage safety checks.
func (s *Server) StartSMSSyncLoop(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 15 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	s.runSMSSyncLoop(ctx, ticker.C)
}

func (s *Server) runSMSSyncLoop(ctx context.Context, ticks <-chan time.Time) {
	if ctx.Err() != nil {
		return
	}
	var events <-chan device.SMSReceivedEvent
	if controller, ok := s.devices.(smsReceivedEventController); ok {
		var err error
		events, err = controller.SubscribeSMSReceivedEvents(ctx)
		if err != nil {
			events = nil
			s.logger.Debug("cellular SMS event subscription unavailable", "error", err)
		}
	}
	pending := newSMSSyncPending()
	collectorDone := make(chan struct{})
	go func() {
		defer close(collectorDone)
		s.collectSMSSyncTriggers(ctx, ticks, events, pending)
	}()
	defer func() { <-collectorDone }()

	// This is the loop's sole executor. smsSyncMu also serializes it with
	// explicit syncs, deletion, and SIM/profile operations outside this loop.
	for {
		select {
		case <-ctx.Done():
			return
		case <-pending.wake:
			if ctx.Err() != nil {
				return
			}
			task := pending.take()
			if task.fullScan {
				s.syncModemSMS(ctx, "")
			} else if len(task.physicalIDs) > 0 {
				s.syncModemSMSPhysical(ctx, task.physicalIDs)
			}
		}
	}
}

func (s *Server) collectSMSSyncTriggers(
	ctx context.Context,
	ticks <-chan time.Time,
	events <-chan device.SMSReceivedEvent,
	pending *smsSyncPending,
) {
	for {
		if ctx.Err() != nil {
			return
		}
		// Prioritize an already-due tick, even under continuous indications.
		select {
		case _, open := <-ticks:
			if !open {
				ticks = nil
			} else {
				pending.add("", true)
			}
			continue
		default:
		}
		select {
		case <-ctx.Done():
			return
		case _, open := <-ticks:
			if !open {
				ticks = nil
			} else {
				pending.add("", true)
			}
		case event, open := <-events:
			if !open {
				events = nil // A closed subscription must not spin or disable polling.
				continue
			}
			physicalID := strings.TrimSpace(event.DeviceID)
			trigger := "sms_received"
			if event.RescanAll {
				physicalID = ""
				trigger = "rescan_all"
			} else {
				if physicalID == "" || s.devices == nil {
					continue
				}
				entry, err := s.devices.Get(physicalID)
				if err != nil || !entry.Discovered || entry.ID != physicalID {
					continue
				}
			}
			if pending.add(physicalID, event.RescanAll) {
				s.logger.Debug("cellular SMS synchronization triggered", "physical_id", physicalID, "trigger", trigger)
			}
		}
	}
}
