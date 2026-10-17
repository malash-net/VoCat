package modem

import "strings"

const smsIndicationLineLimit = 4096

func (session *Session) SetSMSNotificationHandler(handler func()) {
	session.smsHandlerMu.Lock()
	session.smsHandler = handler
	session.smsHandlerMu.Unlock()
}

func (session *Session) notifySMSReceived() {
	session.smsHandlerMu.RLock()
	handler := session.smsHandler
	session.smsHandlerMu.RUnlock()
	if handler != nil {
		handler()
	}
}

// Observe the reader's bytes without taking the transaction gate or consuming
// its input. A pending command, including a USSD wait, still owns every reply.
type smsIndicationObserver struct {
	line    []byte
	discard bool
}

func (observer *smsIndicationObserver) observe(data []byte, notify func()) {
	for _, character := range data {
		if character == '\r' || character == '\n' {
			if !observer.discard {
				line := strings.TrimSpace(strings.Trim(string(observer.line), "\x00"))
				if strings.HasPrefix(strings.ToUpper(line), "+CMTI:") {
					notify()
				}
			}
			observer.line = observer.line[:0]
			observer.discard = false
			continue
		}
		if observer.discard {
			continue
		}
		if len(observer.line) == smsIndicationLineLimit {
			observer.line = observer.line[:0]
			observer.discard = true
			continue
		}
		observer.line = append(observer.line, character)
	}
}
