package modem

import "bytes"

// atFramer retains incomplete input across reads and command boundaries.
type atFramer struct {
	data      []byte
	skipLF    bool
	skipSpace bool
}

func (f *atFramer) feed(data []byte) {
	f.data = append(f.data, data...)
}

func (f *atFramer) next(awaitPrompt bool) (line string, prompt, ok bool) {
	for len(f.data) > 0 {
		if f.skipLF {
			f.skipLF = false
			if f.data[0] == '\n' {
				f.data = f.data[1:]
				continue
			}
		}
		if f.skipSpace {
			f.skipSpace = false
			if f.data[0] == ' ' {
				f.data = f.data[1:]
				continue
			}
		}
		if awaitPrompt && f.data[0] == '>' {
			f.data = f.data[1:]
			f.skipSpace = true
			return ">", true, true
		}
		end := bytes.IndexAny(f.data, "\r\n")
		if end < 0 {
			break
		}
		line = string(f.data[:end])
		f.skipLF = f.data[end] == '\r'
		f.data = f.data[end+1:]
		return line, false, true
	}
	return "", false, false
}

func (f *atFramer) pending() bool {
	return len(f.data) > 0
}
