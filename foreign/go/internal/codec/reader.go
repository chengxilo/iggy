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
	"fmt"
	"math"
	"runtime"
)

// reader is a cursor over a byte slice. The first out-of-bounds read sets err;
// all subsequent reads are no-ops. Call Err() once after all reads to check.
type reader struct {
	p   []byte
	pos int
	err error
}

func newReader(p []byte) *reader {
	return &reader{p: p}
}

// overrun sets r.err to a descriptive error message including the caller's
// file and line number.
func (r *reader) overrun(need int) {
	_, file, line, _ := runtime.Caller(2)
	r.err = fmt.Errorf(
		"reader: need %d bytes at offset %d, only %d remaining (%s:%d)",
		need, r.pos, len(r.p)-r.pos, file, line)
}

func (r *reader) u8() uint8 {
	if r.err != nil {
		return 0
	}
	if r.pos+1 > len(r.p) {
		r.overrun(1)
		return 0
	}
	v := r.p[r.pos]
	r.pos++
	return v
}

func (r *reader) u32() uint32 {
	if r.err != nil {
		return 0
	}
	if r.pos+4 > len(r.p) {
		r.overrun(4)
		return 0
	}
	v := binary.LittleEndian.Uint32(r.p[r.pos : r.pos+4])
	r.pos += 4
	return v
}

func (r *reader) u64() uint64 {
	if r.err != nil {
		return 0
	}
	if r.pos+8 > len(r.p) {
		r.overrun(8)
		return 0
	}
	v := binary.LittleEndian.Uint64(r.p[r.pos : r.pos+8])
	r.pos += 8
	return v
}

func (r *reader) f32() float32 {
	if r.err != nil {
		return 0
	}
	if r.pos+4 > len(r.p) {
		r.overrun(4)
		return 0
	}
	v := math.Float32frombits(binary.LittleEndian.Uint32(r.p[r.pos : r.pos+4]))
	r.pos += 4
	return v
}

// strN reads exactly n bytes as a string, copying them so the caller's string
// Use u8LenStr or u32LenStr instead if the data is length-prefixed:
//
//	[length: 1 byte][data: N bytes]   → u8LenStr
//	[length: 4 bytes][data: N bytes]  → u32LenStr
func (r *reader) strN(n int) string {
	if r.err != nil {
		return ""
	}
	if r.pos+n > len(r.p) {
		r.overrun(n)
		return ""
	}
	v := string(r.p[r.pos : r.pos+n])
	r.pos += n
	return v
}

// u32LenStr reads a length-prefixed string where the length is a 4-byte
// little-endian unsigned integer.
func (r *reader) u32LenStr() string {
	if r.err != nil {
		return ""
	}
	if r.pos+4 > len(r.p) {
		r.overrun(4)
		return ""
	}
	n := int(binary.LittleEndian.Uint32(r.p[r.pos : r.pos+4]))
	r.pos += 4
	if r.pos+n > len(r.p) {
		r.overrun(n)
		return ""
	}
	v := string(r.p[r.pos : r.pos+n])
	r.pos += n
	return v
}

// u8LenStr reads a length-prefixed string where the length is a single byte.
func (r *reader) u8LenStr() string {
	if r.err != nil {
		return ""
	}
	if r.pos+1 > len(r.p) {
		r.overrun(1)
		return ""
	}
	n := int(r.p[r.pos])
	r.pos++
	if r.pos+n > len(r.p) {
		r.overrun(n)
		return ""
	}
	v := string(r.p[r.pos : r.pos+n])
	r.pos += n
	return v
}

// remaining returns the number of unread bytes
func (r *reader) remaining() int {
	return len(r.p) - r.pos
}

// Err returns the first error encountered during reading, or nil.
func (r *reader) Err() error { return r.err }
