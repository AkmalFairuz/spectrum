package protocol

import (
	"encoding/binary"
	"io"
)

// Writer is used for writing packets to an io.Writer.
type Writer struct {
	// w is the underlying io.Writer used for writing data.
	w io.Writer
	// p is a reusable byte slice used for writing the length of the packet.
	p []byte
	// b is a reusable byte slice used for writing the packet data.
	b []byte
}

// NewWriter creates a new Writer with the given io.Writer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w: w,
		p: make([]byte, 4),
		b: make([]byte, 32768),
	}
}

// Write writes a packet to the underlying io.Writer.
// It prefixes the packet data with its length as an uint32 in big-endian order,
// then writes the prefixed data to the underlying io.Writer.
func (w *Writer) Write(data []byte) (err error) {
	binary.BigEndian.PutUint32(w.p, uint32(len(data)))
	w.b = append(w.b[:0], w.p...)
	w.b = append(w.b, data...)
	if _, err := w.w.Write(w.b); err != nil {
		return err
	}
	return
}
