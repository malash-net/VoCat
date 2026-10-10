package modem

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type atResult struct {
	response Response
	err      error
}

type atRequest struct {
	ctx     context.Context
	command string
	payload []byte
	done    chan struct{}

	mu        sync.Mutex
	response  Response
	result    atResult
	started   time.Time
	completed bool

	policy        atCommandPolicy
	awaitPrompt   bool
	abandoned     bool
	commandEcho   bool
	remainingEcho []string
}

func newATRequest(ctx context.Context, command string, payload []byte, interactive bool) *atRequest {
	return &atRequest{
		ctx: ctx, command: command, payload: payload, awaitPrompt: interactive,
		done: make(chan struct{}), response: Response{Command: command}, policy: commandPolicy(command),
	}
}

func (request *atRequest) appendLines(lines ...string) {
	request.mu.Lock()
	if !request.completed {
		request.response.Lines = append(request.response.Lines, lines...)
	}
	request.mu.Unlock()
}

func (request *atRequest) appendURC(lines []string) {
	request.mu.Lock()
	if !request.completed {
		request.response.URCs = append(request.response.URCs, strings.Join(lines, "\n"))
	}
	request.mu.Unlock()
}

func (request *atRequest) setFinal(line string) {
	request.mu.Lock()
	if !request.completed {
		request.response.Final = line
	}
	request.mu.Unlock()
}

func (request *atRequest) copyLines() []string {
	request.mu.Lock()
	defer request.mu.Unlock()
	return append([]string(nil), request.response.Lines...)
}

func (request *atRequest) snapshotLocked() Response {
	response := request.response
	response.Lines = append([]string(nil), response.Lines...)
	response.URCs = append([]string(nil), response.URCs...)
	if !request.started.IsZero() {
		response.Duration = time.Since(request.started)
	}
	return response
}

func (request *atRequest) canceledResultLocked(err error) atResult {
	if request.completed {
		return request.result
	}
	response := request.snapshotLocked()
	if !request.started.IsZero() {
		err = errors.Join(err, ErrSessionUnsynchronized)
	}
	return atResult{response: response, err: err}
}

func (request *atRequest) canceledResult(err error) atResult {
	request.mu.Lock()
	defer request.mu.Unlock()
	return request.canceledResultLocked(err)
}

func (request *atRequest) resultSnapshot(err error) atResult {
	request.mu.Lock()
	defer request.mu.Unlock()
	if request.completed {
		return request.result
	}
	return atResult{response: request.snapshotLocked(), err: err}
}

func (request *atRequest) complete(err error) {
	request.mu.Lock()
	defer request.mu.Unlock()
	if request.completed {
		return
	}
	request.result = atResult{response: request.snapshotLocked(), err: err}
	request.completed = true
	close(request.done)
}

func (request *atRequest) completeCanceled(err error) {
	request.mu.Lock()
	defer request.mu.Unlock()
	if request.completed {
		return
	}
	request.result = request.canceledResultLocked(err)
	request.completed = true
	close(request.done)
}

type atCommandPolicy struct {
	callFinal bool
	data      bool
	opaque    bool
}

func commandPolicy(command string) atCommandPolicy {
	var canonical strings.Builder
	quoted := false
	semicolons := 0
	lastSemicolon := -1
	for _, character := range strings.ToUpper(strings.TrimSpace(command))[2:] {
		switch character {
		case '"':
			quoted = !quoted
		case ' ', '\t':
			if !quoted {
				continue
			}
		case ';':
			if !quoted {
				semicolons++
				lastSemicolon = canonical.Len()
			}
		}
		canonical.WriteRune(character)
	}
	body := canonical.String()
	if quoted {
		return atCommandPolicy{opaque: true}
	}
	if strings.HasPrefix(body, "D") && len(body) > 1 {
		if semicolons == 0 {
			return atCommandPolicy{callFinal: true, data: true}
		}
		if semicolons == 1 && lastSemicolon == len(body)-1 {
			return atCommandPolicy{callFinal: true}
		}
		return atCommandPolicy{opaque: true}
	}
	if semicolons > 0 {
		return atCommandPolicy{opaque: true}
	}
	switch body {
	case "A", "A0", "H", "H0", "H1":
		return atCommandPolicy{callFinal: true}
	case "O", "O0", "O1":
		return atCommandPolicy{callFinal: true, data: true}
	}
	if (body == "+CGDATA" || body == "+CGANS" || strings.HasPrefix(body, "+CGDATA=") || strings.HasPrefix(body, "+CGANS=")) && !strings.HasSuffix(body, "=?") {
		return atCommandPolicy{callFinal: true, data: true}
	}
	return atCommandPolicy{}
}

type atController struct {
	session *Session
	framer  atFramer
	owner   *atRequest
	pending *atRequest
}

func (session *Session) runLoop() {
	defer session.workers.Done()
	controller := &atController{session: session}
	for !session.stopped() {
		controller.abandonExpired()
		if controller.pending != nil && controller.preflight(controller.pending) {
			controller.pending = nil
		}
		if controller.owner != nil && controller.owner.abandoned {
			for remaining := len(session.requests); remaining > 0; remaining-- {
				controller.preflight(<-session.requests)
			}
		}
		if !controller.receivePending() {
			return
		}
		controller.abandonExpired()
		if controller.dispatch() {
			continue
		}
		var canceled <-chan struct{}
		if controller.pending != nil {
			canceled = controller.pending.ctx.Done()
		} else if controller.owner != nil && !controller.owner.abandoned {
			canceled = controller.owner.ctx.Done()
		}
		incoming := session.requests
		if controller.pending != nil || controller.owner != nil && !controller.owner.abandoned || controller.owner == nil && controller.framer.pending() {
			incoming = nil
		}
		select {
		case <-session.stop:
			return
		case batch := <-session.received:
			if !controller.receive(batch) {
				return
			}
		case request := <-incoming:
			if !controller.preflight(request) {
				controller.pending = request
			}
		case <-canceled:
		}
	}
}

func (controller *atController) receivePending() bool {
	for !controller.session.stopped() {
		select {
		case batch := <-controller.session.received:
			if !controller.receive(batch) {
				return false
			}
		default:
			return true
		}
	}
	return false
}

func (controller *atController) preflight(request *atRequest) bool {
	if err := request.ctx.Err(); err != nil {
		request.completeCanceled(commandContextError(err))
	} else if err := controller.session.Err(); err != nil {
		request.complete(err)
	} else if controller.owner != nil && controller.owner.abandoned {
		request.complete(ErrSessionUnsynchronized)
	} else {
		return false
	}
	return true
}

func (controller *atController) dispatch() bool {
	if controller.owner != nil || controller.pending == nil || controller.framer.pending() {
		return false
	}
	request := controller.pending
	if controller.preflight(request) {
		controller.pending = nil
		return true
	}
	if err := drainTransport(request.ctx, controller.session.transport); err != nil {
		if request.ctx.Err() != nil {
			request.completeCanceled(commandContextError(request.ctx.Err()))
		} else {
			controller.failIO("drain serial output", 0, false, err)
		}
		controller.pending = nil
		return true
	}
	if !controller.receivePending() {
		return true
	}
	if controller.preflight(request) {
		controller.pending = nil
		return true
	}
	if controller.framer.pending() {
		return false
	}
	controller.pending = nil
	if err := controller.session.beginRequest(request); err != nil {
		request.complete(err)
		return true
	}
	controller.owner = request
	count, err := writeAll(controller.session.lifetime, controller.session.transport, []byte(request.command+"\r"))
	if err != nil {
		controller.failIO("write serial command", count, false, err)
	} else if request.awaitPrompt {
		if err := drainTransport(controller.session.lifetime, controller.session.transport); err != nil {
			controller.failIO("drain serial command", count, false, err)
		}
	}
	return true
}

func (controller *atController) abandonExpired() {
	request := controller.owner
	if request != nil && !request.abandoned {
		if err := request.ctx.Err(); err != nil {
			request.abandoned = true
			request.completeCanceled(commandContextError(err))
		}
	}
}

func (controller *atController) receive(batch atReadBatch) bool {
	controller.framer.feed(batch.data)
	for {
		controller.abandonExpired()
		if controller.session.stopped() {
			return false
		}
		frameContext := atFrameContext{}
		if request := controller.owner; request != nil {
			frameContext.prompt = request.awaitPrompt
			if len(request.remainingEcho) > 0 {
				frameContext.echo = &request.remainingEcho[0]
			}
		}
		frame, ok := controller.framer.next(frameContext)
		if !ok {
			break
		}
		controller.handleFrame(frame)
	}
	if batch.err != nil {
		controller.session.terminate(fmt.Errorf("read serial response: %w", batch.err), false)
		return false
	}
	return true
}

func (controller *atController) emitURC(lines []string) {
	controller.session.enqueueURC(lines)
	if controller.owner != nil {
		controller.owner.appendURC(lines)
	}
}

func (controller *atController) handleFrame(frame atFrame) {
	request := controller.owner
	if request != nil && frame.kind == atFrameLine && len(request.remainingEcho) > 0 && frame.lines[0] == request.remainingEcho[0] {
		request.remainingEcho = request.remainingEcho[1:]
		return
	}
	switch frame.kind {
	case atFrameURC:
		controller.emitURC(frame.lines)
		return
	case atFrameUnexpectedPrompt:
		controller.session.failProtocol("unexpected serial input prompt", nil)
		return
	case atFramePrompt:
		controller.handlePrompt()
		return
	case atFrameResponse:
		if request != nil {
			request.remainingEcho = nil
			request.appendLines(frame.lines...)
		}
		return
	}
	line := strings.TrimSpace(strings.Trim(frame.lines[0], "\x00"))
	if line == "" {
		return
	}
	if request == nil {
		if !isFinalResult(line) {
			controller.emitURC([]string{line})
		}
		return
	}
	if strings.EqualFold(line, request.command) {
		request.commandEcho = true
		return
	}
	if isCallResult(line) {
		controller.emitURC([]string{line})
		if request.policy.callFinal {
			controller.finish(line)
		}
		return
	}
	if isConnectResult(line) {
		controller.emitURC([]string{line})
		if !strings.EqualFold(line, "MO CONNECTED") && (request.policy.data || request.policy.opaque || !request.policy.callFinal) {
			controller.session.failProtocol("unexpected CONNECT result", ErrUnsupportedDataMode)
		}
		return
	}
	if isURC(line) && !strings.HasPrefix(strings.ToUpper(line), expectedResponsePrefix(request.command)) {
		controller.emitURC([]string{line})
		return
	}
	request.remainingEcho = nil
	if isFinalResult(line) {
		controller.finish(line)
	} else {
		request.appendLines(line)
	}
}

func isConnectResult(line string) bool {
	upper := strings.ToUpper(line)
	return upper == "CONNECT" || strings.HasPrefix(upper, "CONNECT ") || upper == "MO CONNECTED"
}

func (controller *atController) finish(final string) {
	if controller.session.stopped() {
		return
	}
	request := controller.owner
	controller.owner = nil
	request.setFinal(final)
	if err := request.ctx.Err(); err != nil {
		request.completeCanceled(commandContextError(err))
		return
	}
	var err error
	if strings.EqualFold(final, "OK") {
		if request.awaitPrompt {
			err = fmt.Errorf("%w: %s", ErrPromptNotReceived, request.command)
		}
	} else {
		err = &CommandError{Command: request.command, Final: final, Lines: request.copyLines()}
	}
	request.complete(err)
}

func (controller *atController) handlePrompt() {
	request := controller.owner
	if request == nil || !request.awaitPrompt {
		controller.session.failProtocol("unexpected serial input prompt", nil)
		return
	}
	controller.abandonExpired()
	if request.abandoned {
		controller.abortPrompt()
		return
	}
	if request.commandEcho {
		request.remainingEcho = strings.FieldsFunc(string(request.payload), func(character rune) bool {
			return character == '\r' || character == '\n'
		})
	}
	count, err := writeAll(controller.session.lifetime, controller.session.transport, request.payload)
	if err != nil {
		controller.failIO("write SMS payload", count, true, err)
		return
	}
	if request.ctx.Err() != nil {
		controller.abandonExpired()
		controller.abortPrompt()
		return
	}
	count, err = writeAll(controller.session.lifetime, controller.session.transport, []byte{0x1a})
	if err != nil {
		controller.failIO("terminate SMS payload", count, true, err)
		return
	}
	request.awaitPrompt = false
	if err := drainTransport(controller.session.lifetime, controller.session.transport); err != nil {
		controller.failIO("drain SMS payload", count, true, err)
	}
}

func (controller *atController) abortPrompt() {
	if controller.session.stopped() {
		return
	}
	controller.owner.awaitPrompt = false
	count, err := writeAll(controller.session.lifetime, controller.session.transport, []byte{0x1b})
	if err != nil {
		controller.failIO("abort SMS payload", count, true, err)
		return
	}
	if err := drainTransport(controller.session.lifetime, controller.session.transport); err != nil {
		controller.failIO("drain SMS abort", count, true, err)
	}
}

func (controller *atController) failIO(operation string, accepted int, interactive bool, err error) {
	if accepted > 0 || interactive {
		controller.session.failProtocol(fmt.Sprintf("%s incomplete (%d bytes accepted)", operation, accepted), err)
	} else {
		controller.session.terminate(fmt.Errorf("%s: %w", operation, err), false)
	}
}
