// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package codec

import (
	"encoding/binary"
	"math"
)

// Writer appends encoded values to a growing byte slice.
type Writer struct {
	p []byte
}

// NewWriter returns a new Writer with an empty internal buffer.
// Use NewWriterCap instead if the final size is known.
func NewWriter() *Writer {
	return &Writer{}
}

// NewWriterCap returns a Writer with its internal buffer pre-allocated to n
// bytes. Use this when the final size is known in advance to avoid
// reallocations.
func NewWriterCap(n int) *Writer {
	return &Writer{p: make([]byte, 0, n)}
}

func (w *Writer) U8(v uint8) {
	w.p = append(w.p, v)
}

func (w *Writer) U16(v uint16) {
	w.p = binary.LittleEndian.AppendUint16(w.p, v)
}

func (w *Writer) U32(v uint32) {
	w.p = binary.LittleEndian.AppendUint32(w.p, v)
}

func (w *Writer) U64(v uint64) {
	w.p = binary.LittleEndian.AppendUint64(w.p, v)
}

func (w *Writer) F32(v float32) {
	w.p = binary.LittleEndian.AppendUint32(w.p, math.Float32bits(v))
}

// StrN writes a string with no length prefix. Use U8LenStr or U32LenStr
// instead if the reader expects a length prefix.
func (w *Writer) StrN(v string) {
	w.p = append(w.p, v...)
}

// U32LenStr writes a length-prefixed string where the length is a 4-byte
// little-endian unsigned integer.
func (w *Writer) U32LenStr(v string) {
	w.p = binary.LittleEndian.AppendUint32(w.p, uint32(len(v)))
	w.p = append(w.p, v...)
}

// U8LenStr writes a length-prefixed string where the length is a single byte.
func (w *Writer) U8LenStr(v string) {
	w.p = append(w.p, uint8(len(v)))
	w.p = append(w.p, v...)
}

// Bytes returns the accumulated buffer directly. The caller must not retain
// the slice if further writes are expected.
func (w *Writer) Bytes() []byte {
	return w.p
}
