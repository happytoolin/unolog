package json

import (
	stdjson "encoding/json"
	"slices"
	"unicode/utf8"
)

// maxFastMapLen bounds the allocation-free map path: its keys are
// sorted in a stack array.
const maxFastMapLen = 8

// appendStringMap encodes a small map whose values are all plain
// strings and whose keys are ASCII — the shape of unolog's structured
// error and panic fields — byte-for-byte like jsonMarshal, without
// reflection. ok is false for any other map, and the caller falls back
// to jsonMarshal. ASCII keys make the key order unambiguous: byte order
// and UTF-16 order agree there.
func appendStringMap(dst []byte, m map[string]any) (out []byte, ok bool) {
	if m == nil || len(m) > maxFastMapLen {
		return dst, false
	}
	var keys [maxFastMapLen]string
	n := 0
	for k, v := range m {
		if _, isString := v.(string); !isString || !isASCII(k) || n == maxFastMapLen {
			return dst, false
		}
		keys[n] = k
		n++
	}
	sorted := keys[:n]
	slices.Sort(sorted)
	dst = append(dst, '{')
	for i, k := range sorted {
		if i > 0 {
			dst = append(dst, ',')
		}
		dst = appendStdString(dst, k)
		dst = append(dst, ':')
		v, _ := m[k].(string)
		dst = appendStdString(dst, v)
	}
	return append(dst, '}'), true
}

// invalidUTF8 is what jsonMarshal writes for one invalid UTF-8 byte in
// this build: the escape \ufffd in Go 1.25, the literal U+FFFD with the
// json/v2 engine. One probe at init keeps appendStdString byte-equal to
// the fallback on every toolchain.
var invalidUTF8 = func() string {
	b, err := stdjson.Marshal("\xff")
	if err != nil || len(b) < 2 {
		return `\ufffd`
	}
	return string(b[1 : len(b)-1])
}()

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] >= utf8.RuneSelf {
			return false
		}
	}
	return true
}

// appendStdString encodes s exactly like encoding/json with HTML
// escaping disabled, which differs from the zerolog table in
// AppendString: DEL stays literal, U+2028 and U+2029 are escaped, and
// an invalid byte becomes invalidUTF8.
func appendStdString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if b >= 0x20 && b != '"' && b != '\\' {
				i++
				continue
			}
			dst = append(dst, s[start:i]...)
			switch b {
			case '"', '\\':
				dst = append(dst, '\\', b)
			case '\b':
				dst = append(dst, '\\', 'b')
			case '\f':
				dst = append(dst, '\\', 'f')
			case '\n':
				dst = append(dst, '\\', 'n')
			case '\r':
				dst = append(dst, '\\', 'r')
			case '\t':
				dst = append(dst, '\\', 't')
			default:
				dst = append(dst, '\\', 'u', '0', '0', hex[b>>4], hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			dst = append(dst, s[start:i]...)
			dst = append(dst, invalidUTF8...)
			i += size
			start = i
			continue
		}
		if c == ' ' || c == ' ' {
			dst = append(dst, s[start:i]...)
			dst = append(dst, '\\', 'u', '2', '0', '2', hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	dst = append(dst, s[start:]...)
	return append(dst, '"')
}
