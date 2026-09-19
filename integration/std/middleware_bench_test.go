package std

import (
	"bytes"
	"io"
	"net/http"
	"testing"
)

type benchmarkReaderFromWriter struct{ header http.Header }

func (w benchmarkReaderFromWriter) Header() http.Header         { return w.header }
func (w benchmarkReaderFromWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w benchmarkReaderFromWriter) WriteHeader(int)             {}
func (w benchmarkReaderFromWriter) ReadFrom(r io.Reader) (int64, error) {
	return io.Copy(io.Discard, r)
}

func BenchmarkResponseWriterReadFrom(b *testing.B) {
	body := bytes.Repeat([]byte("x"), 4096)
	for _, committed := range []bool{false, true} {
		name := "uncommitted"
		if committed {
			name = "committed"
		}
		b.Run(name, func(b *testing.B) {
			rw := responseWriter{ResponseWriter: benchmarkReaderFromWriter{header: make(http.Header)}}
			var src bytes.Reader
			b.ReportAllocs()
			b.SetBytes(int64(len(body)))
			for b.Loop() {
				rw.statusCode = 0
				rw.wroteHeader = committed
				src.Reset(body)
				if _, err := rw.ReadFrom(&src); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
