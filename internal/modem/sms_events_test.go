package modem

import (
	"context"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

func TestSMSIndicationObserver(t *testing.T) {
	for _, test := range []struct {
		name   string
		chunks []string
		want   int
	}{
		{name: "split header and CRLF", chunks: []string{"\r\n+CM", "TI: \"SM\",7\r", "\n"}, want: 1},
		{name: "multiple storages", chunks: []string{"+CMTI: \"SM\",1\n+CMTI: \"ME\",2\r\n"}, want: 2},
		{name: "trimmed and lowercase", chunks: []string{"\x00 +cmti: \"SM\",1 \x00\r\n"}, want: 1},
		{name: "other input", chunks: []string{"AT+CNMI=2,1,0,0,0\r\nOK\r\n+CUSD: 0,\"text\",15\r\n+CMT: 23\r\n01234\r\n"}},
		{name: "incomplete indication", chunks: []string{"+CMTI: \"SM\",1"}},
		{name: "oversized line", chunks: []string{"+CMTI:" + strings.Repeat("x", smsIndicationLineLimit), "\r\n+CMTI: \"SM\",2\r\n"}, want: 1},
		{name: "oversized suffix is not a line", chunks: []string{strings.Repeat("x", smsIndicationLineLimit), "+CMTI: \"SM\",1\r\n"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var observer smsIndicationObserver
			count := 0
			for _, chunk := range test.chunks {
				observer.observe([]byte(chunk), func() { count++ })
				if len(observer.line) > smsIndicationLineLimit {
					t.Fatal("indication buffer exceeded its limit")
				}
			}
			if count != test.want {
				t.Fatalf("indications = %d, want %d", count, test.want)
			}
		})
	}
}

func TestSessionNotifiesSMSWhileIdleWithoutConsumingURC(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{}
		session := newTestSession(t, transport)
		events := make(chan struct{}, 1)
		session.SetSMSNotificationHandler(func() { events <- struct{}{} })
		transport.enqueue("\r\n+CM", "TI: \"SM\",7\r", "\n")
		synctest.Wait()
		if len(events) != 1 {
			t.Fatal("idle SMS did not trigger a notification")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		line, err := session.WaitURC(ctx, func(line string) bool { return strings.HasPrefix(line, "+CMTI:") })
		if err != nil || line != `+CMTI: "SM",7` {
			t.Fatalf("original SMS URC = %q, %v", line, err)
		}
		if len(transport.ops) != 0 {
			t.Fatalf("idle observation issued transport operations: %q", transport.ops)
		}
	})
}

func TestSessionIdleURCBurstDoesNotBlockSMSObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{}
		session := newTestSession(t, transport)
		events := make(chan struct{}, 1)
		session.SetSMSNotificationHandler(func() {
			select {
			case events <- struct{}{}:
			default:
			}
		})
		chunks := make([]string, 128)
		for index := range chunks {
			chunks[index] = "+CEREG: 1\r\n"
		}
		transport.enqueue(chunks...)
		transport.enqueue("+CMTI: \"SM\",1\r\n")
		synctest.Wait()
		if len(events) != 1 {
			t.Fatal("idle receive buffer blocked the SMS indication")
		}
		for index := range chunks {
			chunks[index] = "+CMTI: \"SM\",2\r\n"
		}
		transport.enqueue(chunks...)
		transport.enqueue("+CUSD: 0,\"done\",15\r\n")
		synctest.Wait()
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if _, err := session.WaitURC(ctx, func(line string) bool { return strings.HasPrefix(line, "+CUSD:") }); err != nil {
			t.Fatalf("slow notification consumer blocked later input: %v", err)
		}
	})
}

func TestSessionNotifiesSMSDuringCommand(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{steps: []transportStep{{write: "AT+CSQ\r"}}}
		session := newTestSession(t, transport)
		events := make(chan struct{}, 1)
		session.SetSMSNotificationHandler(func() { events <- struct{}{} })
		done := make(chan Response, 1)
		failures := make(chan error, 1)
		go func() {
			response, err := session.Execute(context.Background(), "AT+CSQ")
			done <- response
			failures <- err
		}()
		synctest.Wait()
		transport.enqueue("+CMTI: \"SM\",7\r\n")
		synctest.Wait()
		if len(events) != 1 || len(done) != 0 {
			t.Fatal("SMS observation waited for or completed the pending command")
		}
		transport.enqueue("+CSQ: 24,99\r\nOK\r\n")
		response := <-done
		if err := <-failures; err != nil || response.Text() != "+CSQ: 24,99" || len(response.URCs) != 1 || response.URCs[0] != `+CMTI: "SM",7` {
			t.Fatalf("command response changed: %#v, %v", response, err)
		}
	})
}

func TestSessionSMSObservationDoesNotStealUSSD(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{}
		session := newTestSession(t, transport)
		events := make(chan struct{}, 1)
		session.SetSMSNotificationHandler(func() { events <- struct{}{} })
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		done := make(chan string, 1)
		failures := make(chan error, 1)
		go func() {
			line, err := session.WaitURC(ctx, func(line string) bool { return strings.HasPrefix(line, "+CUSD:") })
			done <- line
			failures <- err
		}()
		synctest.Wait()
		transport.enqueue("+CMTI: \"SM\",7\r\n")
		synctest.Wait()
		if len(events) != 1 || len(done) != 0 {
			t.Fatal("SMS notification blocked on or stole the USSD wait")
		}
		transport.enqueue("+CUSD: 0,\"balance\",15\r\n")
		line := <-done
		if err := <-failures; err != nil || line != `+CUSD: 0,"balance",15` {
			t.Fatalf("USSD result = %q, %v", line, err)
		}
		if _, err := session.WaitURC(ctx, func(line string) bool { return strings.HasPrefix(line, "+CMTI:") }); err != nil {
			t.Fatalf("SMS URC was consumed by the observer: %v", err)
		}
	})
}

func TestSessionSMSObservationPreservesCanceledTransaction(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{steps: []transportStep{
			{write: "AT+CSQ\r"},
			{write: "AT+CSQ\r", chunks: []string{"+CSQ: 24,99\r\nOK\r\n"}},
		}}
		session := newTestSession(t, transport)
		events := make(chan struct{}, 1)
		session.SetSMSNotificationHandler(func() { events <- struct{}{} })
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { _, err := session.Execute(ctx, "AT+CSQ"); done <- err }()
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled command = %v", err)
		}
		transport.enqueue("+CMTI: \"SM\",7\r\n", "+CSQ: 1,99\r\nOK\r\n")
		synctest.Wait()
		if len(events) != 1 {
			t.Fatal("pending canceled transaction prevented SMS observation")
		}
		response, err := session.Execute(context.Background(), "AT+CSQ")
		if err != nil || response.Text() != "+CSQ: 24,99" {
			t.Fatalf("late reply leaked into next command: %#v, %v", response, err)
		}
	})
}

func TestSessionSMSObservationAfterCanceledReplyBurst(t *testing.T) {
	for _, prompt := range []bool{false, true} {
		name := "query"
		if prompt {
			name = "late SMS prompt"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				command := "AT+CSQ"
				if prompt {
					command = "AT+CMGS=5"
				}
				steps := []transportStep{{write: command + "\r"}}
				if prompt {
					steps = append(steps, transportStep{write: "\x1b", chunks: []string{"ERROR\r\n"}})
				}
				steps = append(steps, transportStep{write: "AT+CSQ\r", chunks: []string{"+CSQ: 24,99\r\nOK\r\n"}})
				transport := &transcriptTransport{steps: steps}
				session := newTestSession(t, transport)
				events := make(chan struct{}, 1)
				session.SetSMSNotificationHandler(func() { events <- struct{}{} })
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				done := make(chan error, 1)
				go func() {
					var err error
					if prompt {
						_, err = session.ExecutePrompt(ctx, command, []byte("HELLO"))
					} else {
						_, err = session.Execute(ctx, command)
					}
					done <- err
				}()
				synctest.Wait()
				cancel()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled command = %v", err)
				}
				chunks := []string{"+CSQ: 1,99\r\nOK\r\n"}
				if prompt {
					chunks[0] = "\r\n> "
				}
				for range 32 {
					chunks = append(chunks, "+CEREG: 1\r\n")
				}
				chunks = append(chunks, "+CMTI: \"SM\",7\r\n")
				transport.enqueue(chunks...)
				synctest.Wait()
				if len(events) != 1 {
					t.Fatal("abandoned reply and URC burst blocked the SMS indication")
				}
				response, err := session.Execute(context.Background(), "AT+CSQ")
				if err != nil || response.Text() != "+CSQ: 24,99" {
					t.Fatalf("late response leaked into next command: %#v, %v", response, err)
				}
			})
		})
	}
}

func TestSessionSMSHandlerRemovalAndClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transport := &transcriptTransport{}
		session := newTestSession(t, transport)
		events := make(chan struct{}, 4)
		handler := func() { events <- struct{}{} }
		session.SetSMSNotificationHandler(handler)
		transport.enqueue("+CMTI: \"SM\",1\r\n")
		synctest.Wait()
		session.SetSMSNotificationHandler(nil)
		transport.enqueue("+CMTI: \"SM\",2\r\n")
		synctest.Wait()
		if len(events) != 1 {
			t.Fatalf("events after removing handler = %d, want 1", len(events))
		}
		session.SetSMSNotificationHandler(handler)
		if err := session.Close(); err != nil {
			t.Fatal(err)
		}
		transport.enqueue("+CMTI: \"SM\",3\r\n")
		synctest.Wait()
		if len(events) != 1 {
			t.Fatal("closed session still called the handler")
		}
		select {
		case <-session.idleDone:
		default:
			t.Fatal("Close left the idle input worker running")
		}
	})
}
