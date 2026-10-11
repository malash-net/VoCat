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
	err       error
	started   time.Time
	completed bool

	responsePrefix string
	awaitPrompt    bool
	abandoned      bool
	commandEcho    bool
	remainingEcho  []string
}

func newATRequest(ctx context.Context, command string, payload []byte, interactive bool) *atRequest {
	return &atRequest{
		ctx: ctx, command: command, payload: payload, awaitPrompt: interactive,
		done: make(chan struct{}), response: Response{Command: command}, responsePrefix: expectedResponsePrefix(command),
	}
}

func (request *atRequest) appendLines(lines ...string) {
	request.mu.Lock()
	if !request.completed {
		request.response.Lines = append(request.response.Lines, lines...)
	}
	request.mu.Unlock()
}

func (request *atRequest) appendURC(line string) {
	request.mu.Lock()
	if !request.completed {
		request.response.URCs = append(request.response.URCs, line)
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
	if !request.completed && !request.started.IsZero() {
		response.Duration = time.Since(request.started)
	}
	return response
}

func (request *atRequest) resultErrorLocked(err error) error {
	if request.completed {
		return request.err
	}
	if !request.started.IsZero() && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
		return errors.Join(err, ErrSessionUnsynchronized)
	}
	return err
}

func (request *atRequest) resultSnapshot(err error) atResult {
	request.mu.Lock()
	defer request.mu.Unlock()
	return atResult{response: request.snapshotLocked(), err: request.resultErrorLocked(err)}
}

func (request *atRequest) complete(err error) {
	request.mu.Lock()
	defer request.mu.Unlock()
	if request.completed {
		return
	}
	request.err = request.resultErrorLocked(err)
	if !request.started.IsZero() {
		request.response.Duration = time.Since(request.started)
	}
	request.completed = true
	close(request.done)
}

func isCallCommand(command string) bool {
	command = strings.ToUpper(strings.TrimSpace(command))
	switch command {
	case "ATA", "ATA0", "ATH", "ATH0", "ATH1":
		return true
	}
	return strings.HasPrefix(command, "ATD")
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
		request.complete(commandContextError(err))
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
			request.complete(commandContextError(request.ctx.Err()))
		} else {
			controller.failIO("drain serial output", err)
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
	err := writeAll(controller.session.lifetime, controller.session.transport, []byte(request.command+"\r"))
	if err != nil {
		controller.failIO("write serial command", err)
	} else if request.awaitPrompt {
		if err := drainTransport(controller.session.lifetime, controller.session.transport); err != nil {
			controller.failIO("drain serial command", err)
		}
	}
	return true
}

func (controller *atController) abandonExpired() {
	request := controller.owner
	if request != nil && !request.abandoned {
		if err := request.ctx.Err(); err != nil {
			request.abandoned = true
			request.complete(commandContextError(err))
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
		awaitPrompt := controller.owner != nil && controller.owner.awaitPrompt
		line, prompt, ok := controller.framer.next(awaitPrompt)
		if !ok {
			break
		}
		if prompt {
			controller.handlePrompt()
		} else {
			controller.handleLine(line)
		}
	}
	if batch.err != nil {
		controller.session.terminate(fmt.Errorf("read serial response: %w", batch.err), false)
		return false
	}
	return true
}

func (controller *atController) emitURC(line string) {
	controller.session.enqueueURC(line)
	if controller.owner != nil {
		controller.owner.appendURC(line)
	}
}

func (controller *atController) handleLine(line string) {
	request := controller.owner
	if request != nil && len(request.remainingEcho) > 0 && line == request.remainingEcho[0] {
		request.remainingEcho = request.remainingEcho[1:]
		return
	}
	line = strings.TrimSpace(strings.Trim(line, "\x00"))
	if line == "" {
		return
	}
	if request == nil {
		if !isFinalResult(line) {
			controller.emitURC(line)
		}
		return
	}
	if strings.EqualFold(line, request.command) {
		request.commandEcho = true
		return
	}
	if isCallResult(line) {
		controller.emitURC(line)
		if isCallCommand(request.command) {
			controller.finish(line)
		}
		return
	}
	if isConnectResult(line) {
		controller.emitURC(line)
		return
	}
	if isURC(line) && !strings.HasPrefix(strings.ToUpper(line), request.responsePrefix) {
		controller.emitURC(line)
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
		request.complete(commandContextError(err))
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
	err := writeAll(controller.session.lifetime, controller.session.transport, request.payload)
	if err != nil {
		controller.failIO("write SMS payload", err)
		return
	}
	if request.ctx.Err() != nil {
		controller.abandonExpired()
		controller.abortPrompt()
		return
	}
	err = writeAll(controller.session.lifetime, controller.session.transport, []byte{0x1a})
	if err != nil {
		controller.failIO("terminate SMS payload", err)
		return
	}
	request.awaitPrompt = false
	if err := drainTransport(controller.session.lifetime, controller.session.transport); err != nil {
		controller.failIO("drain SMS payload", err)
	}
}

func (controller *atController) abortPrompt() {
	if controller.session.stopped() {
		return
	}
	controller.owner.awaitPrompt = false
	err := writeAll(controller.session.lifetime, controller.session.transport, []byte{0x1b})
	if err != nil {
		controller.failIO("abort SMS payload", err)
		return
	}
	if err := drainTransport(controller.session.lifetime, controller.session.transport); err != nil {
		controller.failIO("drain SMS abort", err)
	}
}

func (controller *atController) failIO(operation string, err error) {
	controller.session.terminate(fmt.Errorf("%s: %w", operation, err), false)
}
