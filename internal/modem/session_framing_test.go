package modem

import (
	"reflect"
	"testing"
)

func TestATFramerStream(t *testing.T) {
	for _, test := range []struct {
		chunks []string
		frames []atFrame
	}{
		{chunks: []string{"\r", "\n>", " ", "\r\nOK\r", "\n"}, frames: []atFrame{{kind: atFrameLine, lines: []string{""}}, {kind: atFramePrompt, lines: []string{">"}}, {kind: atFrameLine, lines: []string{""}}, {kind: atFrameLine, lines: []string{"OK"}}}},
		{chunks: []string{"alpha\r\n\r\nbeta\npart", "ial\r\n"}, frames: []atFrame{{kind: atFrameLine, lines: []string{"alpha"}}, {kind: atFrameLine, lines: []string{""}}, {kind: atFrameLine, lines: []string{"beta"}}, {kind: atFrameLine, lines: []string{"partial"}}}},
		{chunks: []string{"echo > value\r\n >value\r\n"}, frames: []atFrame{{kind: atFrameLine, lines: []string{"echo > value"}}, {kind: atFrameLine, lines: []string{" >value"}}}},
		{chunks: []string{">"}, frames: []atFrame{{kind: atFramePrompt, lines: []string{">"}}}},
	} {
		var framer atFramer
		var frames []atFrame
		for _, chunk := range test.chunks {
			framer.feed([]byte(chunk))
			frames = append(frames, drainATFramer(&framer, atFrameContext{prompt: true})...)
		}
		if !reflect.DeepEqual(frames, test.frames) {
			t.Fatalf("chunks %q: frames = %#v, want %#v", test.chunks, frames, test.frames)
		}
	}
}

func TestATFramerOwnedBodies(t *testing.T) {
	for _, test := range []struct {
		header string
		body   string
		kind   atFrameKind
	}{
		{header: "+CMT: \"+123\",0", body: ">", kind: atFrameURC},
		{header: "+CDS: 24", body: "001122", kind: atFrameURC},
		{header: "+CMGR: 0,,24", body: "OK", kind: atFrameResponse},
	} {
		var framer atFramer
		framer.feed([]byte(test.header + "\r\n" + test.body))
		if got := drainATFramer(&framer, atFrameContext{}); len(got) != 0 {
			t.Fatalf("header produced frames: %#v", got)
		}
		framer.feed([]byte("\r\nOK\r\n"))
		want := []atFrame{{kind: test.kind, lines: []string{test.header, test.body}}, {kind: atFrameLine, lines: []string{"OK"}}}
		if got := drainATFramer(&framer, atFrameContext{}); !reflect.DeepEqual(got, want) {
			t.Fatalf("body %q: frames = %#v, want %#v", test.body, got, want)
		}
	}
}

func TestATFramerEchoAmbiguity(t *testing.T) {
	for _, test := range []struct {
		echo   string
		prefix string
		suffix string
		frames []atFrame
	}{
		{echo: ">", prefix: ">", suffix: "\r\n", frames: []atFrame{{kind: atFrameLine, lines: []string{">"}}}},
		{echo: ">expected", prefix: ">e", suffix: "lse\r\n", frames: []atFrame{{kind: atFrameUnexpectedPrompt, lines: []string{">"}}, {kind: atFrameLine, lines: []string{"else"}}}},
		{echo: "+CMT: hello", prefix: "+CMT: hello", suffix: "\r\n", frames: []atFrame{{kind: atFrameLine, lines: []string{"+CMT: hello"}}}},
	} {
		var framer atFramer
		context := atFrameContext{echo: &test.echo}
		framer.feed([]byte(test.prefix))
		if got := drainATFramer(&framer, context); len(got) != 0 {
			t.Fatalf("prefix %q produced frames: %#v", test.prefix, got)
		}
		framer.feed([]byte(test.suffix))
		if got := drainATFramer(&framer, context); !reflect.DeepEqual(got, test.frames) {
			t.Fatalf("echo %q: frames = %#v, want %#v", test.echo, got, test.frames)
		}
	}
}

func drainATFramer(framer *atFramer, context atFrameContext) []atFrame {
	var frames []atFrame
	for frame, ok := framer.next(context); ok; frame, ok = framer.next(context) {
		frames = append(frames, frame)
	}
	return frames
}
