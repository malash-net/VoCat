package modem

import (
	"reflect"
	"testing"
)

func TestATFramerStream(t *testing.T) {
	for _, test := range []struct {
		name        string
		chunks      []string
		awaitPrompt bool
		lines       []string
		prompts     int
	}{
		{name: "split prompt and CRLF", chunks: []string{"\r", "\n>", " ", "\r\nOK\r", "\n"}, awaitPrompt: true, lines: []string{"", ">", "", "OK"}, prompts: 1},
		{name: "partial line", chunks: []string{"alpha\r\n\r\nbeta\npart", "ial\r\n"}, lines: []string{"alpha", "", "beta", "partial"}},
		{name: "ordinary greater than", chunks: []string{">echo\r\necho > value\r\n"}, lines: []string{">echo", "echo > value"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			var framer atFramer
			var lines []string
			prompts := 0
			for _, chunk := range test.chunks {
				framer.feed([]byte(chunk))
				for line, prompt, ok := framer.next(test.awaitPrompt); ok; line, prompt, ok = framer.next(test.awaitPrompt) {
					lines = append(lines, line)
					if prompt {
						prompts++
					}
				}
			}
			if !reflect.DeepEqual(lines, test.lines) || prompts != test.prompts || framer.pending() {
				t.Fatalf("lines = %q, prompts = %d, pending = %t", lines, prompts, framer.pending())
			}
		})
	}
}
