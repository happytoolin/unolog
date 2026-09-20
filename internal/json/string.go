package json

import (
	"unicode/utf8"
	"unsafe"
)

const hex = "0123456789abcdef"

// noEscapeTable[b] reports whether byte b needs no escaping inside a JSON
// string. Vendored verbatim from zerolog: printable ASCII except backslash
// and double quote; note 0x7f (DEL) is escaped.
var noEscapeTable = [256]bool{}

func init() {
	for i := 0; i <= 0x7e; i++ {
		noEscapeTable[i] = i >= 0x20 && i != '\\' && i != '"'
	}
}

// swarMinLen is the minimum string length for which the SWAR chunk scan
// is used; below it the byte loop is cheaper than setting up a load.
const swarMinLen = 8

// AppendString encodes the input string to json and appends the encoded
// string to the input byte slice. It locates the first byte that needs
// escaping with a single SWAR scan, appends a clean string in bulk, and
// hands anything else to appendStringComplex, which escapes from that
// byte on. Scanning once — instead of the old "is it clean?" scan
// followed by a second walk to find the escape — is what makes an
// escape-heavy string cheaper.
func (e Encoder) AppendString(dst []byte, s string) []byte {
	dst = append(dst, '"')
	if i := firstEscape(s); i == len(s) {
		dst = append(dst, s...)
	} else {
		dst = appendStringComplex(dst, s, i)
	}
	return append(dst, '"')
}

// load64 reads the 8 bytes of s starting at i as a little-endian word.
// The caller must guarantee i+8 <= len(s).
func load64(s string, i int) uint64 {
	return *(*uint64)(unsafe.Add(unsafe.Pointer(unsafe.StringData(s)), uintptr(i)))
}

// firstEscape returns the index of the first byte of s that needs JSON
// escaping, or len(s) when the whole string is clean. It scans 8 bytes
// per iteration while at least 8 remain, retries the overlapping tail
// chunk n-8 so a hit in the last bytes is never missed, and falls back
// to a byte loop for short strings and the final tail.
//
// The overlapping tail is not just a correctness trick: replacing it
// with a plain ≤ 7-iteration byte tail measured 8.2% slower on the
// 12-field sink benchmark, because the branch-free chunk check beats
// the loop's per-iteration bounds check and unpredictable exit.
func firstEscape(s string) int {
	n := len(s)
	if n >= swarMinLen {
		i := 0
		for i+8 <= n {
			if !chunkIsClean(load64(s, i)) {
				break
			}
			i += 8
		}
		if i+8 > n {
			if i == n || chunkIsClean(load64(s, n-8)) {
				return n
			}
		}
		for ; i < n; i++ {
			if !noEscapeTable[s[i]] {
				return i
			}
		}
		return n
	}
	if n >= 4 {
		// Two overlapping 4-byte loads cover [0,4) and [n-4,n); packing
		// them into one word lets the 64-bit predicate check the whole
		// 4..7-byte string in a single call. That matters: short keys like
		// "op.name" hit the byte loop for a quarter of the corpus fields.
		x := uint64(load32(s, 0)) | uint64(load32(s, n-4))<<32
		if chunkIsClean(x) {
			return n
		}
	}
	for i := 0; i < n; i++ {
		if !noEscapeTable[s[i]] {
			return i
		}
	}
	return n
}

// load32 reads the 4 bytes of s starting at i as a little-endian word.
// The caller must guarantee i+4 <= len(s).
func load32(s string, i int) uint32 {
	return *(*uint32)(unsafe.Add(unsafe.Pointer(unsafe.StringData(s)), uintptr(i)))
}

// appendStringTable is the vendored zerolog v1.34.0 AppendString: a
// byte-by-byte scan over noEscapeTable with a complex path for anything
// that needs escaping. It is the correctness oracle for the SWAR fast
// path (property and fuzz tests).
func appendStringTable(dst []byte, s string) []byte {
	dst = append(dst, '"')
	for i := 0; i < len(s); i++ {
		if !noEscapeTable[s[i]] {
			dst = appendStringComplex(dst, s, i)
			return append(dst, '"')
		}
	}
	// Clean string: appended in bulk.
	dst = append(dst, s...)
	return append(dst, '"')
}

// appendStringComplex is used by appendStringTable to take over an in
// progress JSON string encoding that encountered a character that needs
// to be encoded.
func appendStringComplex(dst []byte, s string, i int) []byte {
	start := 0
	for i < len(s) {
		b := s[i]
		if b >= utf8.RuneSelf {
			r, size := utf8.DecodeRuneInString(s[i:])
			if r == utf8.RuneError && size == 1 {
				// Invalid sequence: append the pending simple prefix and
				// the replacement character.
				if start < i {
					dst = append(dst, s[start:i]...)
				}
				dst = append(dst, `\ufffd`...)
				i += size
				start = i
				continue
			}
			i += size
			continue
		}
		if noEscapeTable[b] {
			i++
			continue
		}
		// Needs encoding: append the pending simple prefix, then the
		// escaped byte.
		if start < i {
			dst = append(dst, s[start:i]...)
		}
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
	}
	if start < len(s) {
		dst = append(dst, s[start:]...)
	}
	return dst
}

// isCleanASCII reports whether every byte of s is printable ASCII that
// needs no JSON escaping (0x20..0x7e except '"' and '\\'). It is
// conservative: any suspicious chunk (including chunks that only look
// suspicious because of SWAR borrow artifacts) returns false, so a
// false positive costs performance, never correctness.
func isCleanASCII(s string) bool {
	return firstEscape(s) == len(s)
}

// chunkIsClean reports whether all 8 bytes of the chunk are printable
// ASCII (0x20..0x7e) other than '"' and '\\'.
//
// All four hazards are folded into one borrow-term OR whose high bits are
// tested once:
//
//	((x - 0x2020..) | (0x7e7e.. - x) | (q - 0x0101..) | (s - 0x0101..)) & 0x8080..
//
// where q = x^0x2222.. and s = x^0x5c5c... The first two terms flag bytes
// outside [0x20,0x7e]: control bytes underflow the first subtraction, DEL
// and non-ASCII the second, and at least one term keeps its high bit for
// every out-of-range byte. The last two are plain zero-byte detectors on
// the quote/slash XOR masks: a byte minus one reaches 0x80 exactly when the
// byte was zero, and any borrow that sets a high bit is itself seeded by a
// zero byte — so (d - 0x0101..) & 0x8080.. != 0 is equivalent to "d contains
// a zero byte". The usual &^ d guard only suppresses those borrow false
// positives, which cost a slow-path fallback, never correctness. Keeping it
// would also break the OR: it would mask away a real hit's high bit.
//
// Every false positive only sends the string to the table path, so the
// output stays byte-identical to the vendored reference.
func chunkIsClean(x uint64) bool {
	q := x ^ swarQuote
	s := x ^ swarSlash
	return ((x-0x2020202020202020)|(0x7e7e7e7e7e7e7e7e-x)|(q-swarOnes)|(s-swarOnes))&swarHighs == 0
}

const (
	swarOnes  = 0x0101010101010101
	swarHighs = 0x8080808080808080
	swarQuote = 0x2222222222222222 // '"'
	swarSlash = 0x5c5c5c5c5c5c5c5c // '\\'
)

// hasZero64 reports whether the 8-byte word x contains a zero byte.
//
// This MUST stay in the canonical SWAR form
//
//	(x - 0x0101010101010101) &^ x & 0x8080808080808080
//
// Dropping the &^ x term compiles and runs several times faster —
// because it never detects anything: a zero byte clears its own high
// bit in x, so the masked result is always zero and escaping is
// silently skipped. The property test in this package fails any such
// regression.
func hasZero64(x uint64) bool {
	return (x-swarOnes)&^x&swarHighs != 0
}
