package modem

import (
	"bytes"
	"strconv"
	"strings"
)

type atFrameKind uint8

const (
	atFrameLine atFrameKind = iota
	atFramePrompt
	atFrameURC
	atFrameResponse
	atFrameUnexpectedPrompt
)

type atFrame struct {
	kind  atFrameKind
	lines []string
}

type atFrameContext struct {
	prompt bool
	echo   *string
}

type atFramer struct {
	data      []byte
	skipLF    bool
	skipSpace bool
	header    string
	bodyKind  atFrameKind
}

func (f *atFramer) feed(data []byte) {
	f.data = append(f.data, data...)
}

func (f *atFramer) next(context atFrameContext) (atFrame, bool) {
	for len(f.data) > 0 {
		if f.skipLF {
			f.skipLF = false
			if f.data[0] == '\n' {
				f.data = f.data[1:]
				continue
			}
		}
		if f.skipSpace {
			ownedSpace := false
			if f.data[0] == ' ' && context.echo != nil && strings.HasPrefix(*context.echo, " ") {
				end := bytes.IndexAny(f.data, "\r\n")
				if end < 0 && bytes.HasPrefix([]byte(*context.echo), f.data) {
					return atFrame{}, false
				}
				ownedSpace = end >= 0 && string(f.data[:end]) == *context.echo
			}
			f.skipSpace = false
			if f.data[0] == ' ' && !ownedSpace {
				f.data = f.data[1:]
				continue
			}
		}
		end := bytes.IndexAny(f.data, "\r\n")
		isEcho := f.header == "" && end >= 0 && context.echo != nil && string(f.data[:end]) == *context.echo
		if f.header == "" && f.data[0] == '>' && !isEcho {
			if end < 0 && context.echo != nil && bytes.HasPrefix([]byte(*context.echo), f.data) {
				return atFrame{}, false
			}
			kind := atFrameUnexpectedPrompt
			if context.prompt {
				kind = atFramePrompt
			}
			f.data = f.data[1:]
			f.skipSpace = true
			return atFrame{kind: kind, lines: []string{">"}}, true
		}
		if end < 0 {
			return atFrame{}, false
		}
		line := string(f.data[:end])
		f.skipLF = f.data[end] == '\r'
		f.data = f.data[end+1:]
		if f.header != "" {
			frame := atFrame{kind: f.bodyKind, lines: []string{f.header, line}}
			f.header = ""
			return frame, true
		}
		if isEcho {
			return atFrame{kind: atFrameLine, lines: []string{line}}, true
		}
		normalized := strings.ToUpper(strings.TrimSpace(line))
		switch {
		case strings.HasPrefix(normalized, "+CMT:"):
			f.header, f.bodyKind = line, atFrameURC
		case strings.HasPrefix(normalized, "+CDS:"):
			if _, err := strconv.Atoi(strings.TrimSpace(normalized[len("+CDS:"):])); err == nil {
				f.header, f.bodyKind = line, atFrameURC
			}
		case atSMSRecordHeader(normalized):
			f.header, f.bodyKind = line, atFrameResponse
		}
		if f.header == "" {
			return atFrame{kind: atFrameLine, lines: []string{line}}, true
		}
	}
	return atFrame{}, false
}

func (f *atFramer) pending() bool {
	return len(f.data) > 0 || f.header != ""
}

func atSMSRecordHeader(line string) bool {
	fields, cmgr := strings.CutPrefix(line, "+CMGR:")
	if !cmgr {
		var cmgl bool
		fields, cmgl = strings.CutPrefix(line, "+CMGL:")
		if !cmgl {
			return false
		}
	}
	first, _, comma := strings.Cut(fields, ",")
	if !comma {
		return false
	}
	first = strings.TrimSpace(first)
	if _, err := strconv.Atoi(first); err == nil {
		return true
	}
	return cmgr && len(first) >= 2 && first[0] == '"' && first[len(first)-1] == '"'
}
