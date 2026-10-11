package modem

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"
)

type atRequest struct {
	ctx     context.Context
	payload []byte
	done    chan struct{}

	mu       sync.Mutex
	response Response
	err      error
	started  time.Time

	responsePrefix string
	awaitPrompt    bool
}

func newATRequest(ctx context.Context, command string, payload []byte, interactive bool) *atRequest {
	return &atRequest{
		ctx: ctx, payload: payload, awaitPrompt: interactive,
		done: make(chan struct{}), response: Response{Command: command}, responsePrefix: expectedResponsePrefix(command),
	}
}

func (request *atRequest) appendLines(lines ...string) {
	request.mu.Lock()
	if request.ctx.Err() == nil {
		request.response.Lines = append(request.response.Lines, lines...)
	}
	request.mu.Unlock()
}

func (request *atRequest) appendURC(line string) {
	request.mu.Lock()
	if request.ctx.Err() == nil {
		request.response.URCs = append(request.response.URCs, line)
	}
	request.mu.Unlock()
}

func (request *atRequest) resultSnapshot(err error) (Response, error) {
	request.mu.Lock()
	defer request.mu.Unlock()
	response := request.response
	response.Lines = append([]string(nil), response.Lines...)
	response.URCs = append([]string(nil), response.URCs...)
	select {
	case <-request.done:
		err = request.err
	default:
		if !request.started.IsZero() {
			response.Duration = time.Since(request.started)
		}
	}
	return response, err
}

func (request *atRequest) complete(final string, err error) {
	request.mu.Lock()
	defer request.mu.Unlock()
	request.response.Final = final
	if err == nil {
		if strings.EqualFold(final, "OK") {
			if request.awaitPrompt {
				err = fmt.Errorf("%w: %s", ErrPromptNotReceived, request.response.Command)
			}
		} else {
			err = &CommandError{Command: request.response.Command, Final: final, Lines: append([]string(nil), request.response.Lines...)}
		}
	}
	request.err = err
	if !request.started.IsZero() {
		request.response.Duration = time.Since(request.started)
	}
	close(request.done)
}

func isCallCommand(command string) bool {
	command = strings.ToUpper(strings.TrimSpace(command))
	switch command {
	case "ATA", "ATA0":
		return true
	}
	return strings.HasPrefix(command, "ATD")
}

type atController struct {
	session         *Session
	readBuf         []byte
	skipPromptSpace bool
	owner           *atRequest
}

func (session *Session) runLoop() {
	defer session.workers.Done()
	controller := &atController{session: session}
	for !session.stopped() {
		if !controller.receivePending() {
			return
		}
		incoming := session.requests
		// Cancellation releases the caller, not the wire. Queue new commands
		// until the old final and any buffered trailing input are consumed.
		if controller.owner != nil || len(controller.readBuf) > 0 {
			incoming = nil
		}
		select {
		case <-session.lifetime.Done():
			return
		case batch := <-session.received:
			if !controller.receive(batch) {
				return
			}
		case request := <-incoming:
			controller.start(request)
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

func (controller *atController) start(request *atRequest) {
	if err := request.ctx.Err(); err != nil {
		request.complete("", commandContextError(err))
		return
	}
	if err := drainTransport(request.ctx, controller.session.transport); err != nil {
		if request.ctx.Err() != nil {
			request.complete("", commandContextError(request.ctx.Err()))
		} else {
			controller.failIO("drain serial output", err)
		}
		return
	}
	// Finish input already received before assigning a new response owner.
	for {
		if !controller.receivePending() {
			return
		}
		if len(controller.readBuf) == 0 {
			break
		}
		select {
		case batch := <-controller.session.received:
			if !controller.receive(batch) {
				return
			}
		case <-request.ctx.Done():
			request.complete("", commandContextError(request.ctx.Err()))
			return
		case <-controller.session.lifetime.Done():
			return
		}
	}
	if err := controller.session.beginRequest(request); err != nil {
		request.complete("", err)
		return
	}
	controller.owner = request
	err := writeAll(controller.session.lifetime, controller.session.transport, []byte(request.response.Command+"\r"))
	if err != nil {
		controller.failIO("write serial command", err)
	} else if request.awaitPrompt {
		if err := drainTransport(controller.session.lifetime, controller.session.transport); err != nil {
			controller.failIO("drain serial command", err)
		}
	}
}

func (controller *atController) receive(batch atReadBatch) bool {
	controller.readBuf = append(controller.readBuf, batch.data...)
	for len(controller.readBuf) > 0 {
		if controller.session.stopped() {
			return false
		}
		if controller.skipPromptSpace {
			controller.skipPromptSpace = false
			if controller.readBuf[0] == ' ' {
				controller.readBuf = controller.readBuf[1:]
				continue
			}
		}
		if controller.owner != nil && controller.owner.awaitPrompt && controller.readBuf[0] == '>' {
			controller.readBuf = controller.readBuf[1:]
			controller.skipPromptSpace = true
			controller.handlePrompt()
			continue
		}
		line, ok := popLine(&controller.readBuf)
		if !ok {
			if controller.owner == nil && strings.Trim(string(controller.readBuf), "\x00 \t") == "" {
				controller.readBuf = nil
			}
			break
		}
		controller.handleLine(line)
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
	if strings.EqualFold(line, request.response.Command) ||
		(!request.awaitPrompt && len(request.payload) > 0 && line == string(request.payload)) {
		return
	}
	if isCallResult(line) {
		controller.emitURC(line)
		if isCallCommand(request.response.Command) {
			controller.finish(line)
		}
		return
	}
	if isURC(line) && !strings.HasPrefix(strings.ToUpper(line), request.responsePrefix) {
		controller.emitURC(line)
		return
	}
	if isFinalResult(line) {
		controller.finish(line)
	} else {
		request.appendLines(line)
	}
}

func (controller *atController) finish(final string) {
	if controller.session.stopped() {
		return
	}
	request := controller.owner
	controller.owner = nil
	request.complete(final, commandContextError(request.ctx.Err()))
}

func (controller *atController) handlePrompt() {
	request := controller.owner
	terminator := byte(0x1b) // ESC cancels a prompt whose caller has left.
	if request.ctx.Err() == nil {
		if err := writeAll(controller.session.lifetime, controller.session.transport, request.payload); err != nil {
			controller.failIO("write SMS payload", err)
			return
		}
		if request.ctx.Err() == nil {
			terminator = 0x1a // Ctrl-Z commits the SMS only while the caller is active.
		}
	}
	if err := writeAll(controller.session.lifetime, controller.session.transport, []byte{terminator}); err != nil {
		controller.failIO("terminate SMS payload", err)
		return
	}
	request.awaitPrompt = false
	if err := drainTransport(controller.session.lifetime, controller.session.transport); err != nil {
		controller.failIO("drain SMS payload", err)
	}
}

func (controller *atController) failIO(operation string, err error) {
	controller.session.terminate(fmt.Errorf("%s: %w", operation, err), false)
}
