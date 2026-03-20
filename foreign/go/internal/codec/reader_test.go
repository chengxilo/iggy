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
	"strings"
	"testing"
)

// --- byte-construction helpers ---

func u16le(v uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return b
}

func u32le(v uint32) []byte {
	b := make([]byte, 4)
	binary.LittleEndian.PutUint32(b, v)
	return b
}

func u64le(v uint64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, v)
	return b
}

func cat(slices ...[]byte) []byte {
	var out []byte
	for _, s := range slices {
		out = append(out, s...)
	}
	return out
}

// TestReader_reads exercises every read method in sequence.
func TestReader_reads(t *testing.T) {
	const wantU8 uint8 = math.MaxUint8
	const wantU16 uint16 = math.MaxUint16
	const wantU32 uint32 = math.MaxUint32
	const wantU64 uint64 = math.MaxUint64
	const wantF32 float32 = math.Pi
	const wantRem = 1

	wantStrN := "str"
	wantU32LenStr := "uint32"
	wantU8LenStr := "uint8"

	payload := cat(
		[]byte{wantU8},                                           // u8
		u16le(wantU16),                                           // u16
		u32le(wantU32),                                           // u32
		u64le(wantU64),                                           // u64
		u32le(math.Float32bits(wantF32)),                         // f32
		[]byte(wantStrN),                                         // strN(len(wantStrN))
		u32le(uint32(len(wantU32LenStr))), []byte(wantU32LenStr), // u32LenStr
		[]byte{uint8(len(wantU8LenStr))}, []byte(wantU8LenStr), // u8LenStr
		[]byte{0xFF}, // wantRem trailing bytes for remaining()
	)

	r := newReader(payload)
	u8 := r.u8()
	u16 := r.u16()
	u32 := r.u32()
	u64 := r.u64()
	f32 := r.f32()
	strN := r.strN(len(wantStrN))
	u32LenStr := r.u32LenStr()
	u8LenStr := r.u8LenStr()
	rem := r.remaining()
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if u8 != wantU8 {
		t.Errorf("u8: got %#x, want %#x", u8, wantU8)
	}
	if u16 != wantU16 {
		t.Errorf("u16: got %#x, want %#x", u16, wantU16)
	}
	if u32 != wantU32 {
		t.Errorf("u32: got %#x, want %#x", u32, wantU32)
	}
	if u64 != wantU64 {
		t.Errorf("u64: got %#x, want %#x", u64, wantU64)
	}
	if f32 != wantF32 {
		t.Errorf("f32: got %v, want %v", f32, wantF32)
	}
	if strN != wantStrN {
		t.Errorf("strN: got %q, want %q", strN, wantStrN)
	}
	if u32LenStr != wantU32LenStr {
		t.Errorf("u32LenStr: got %q, want %q", u32LenStr, wantU32LenStr)
	}
	if u8LenStr != wantU8LenStr {
		t.Errorf("u8LenStr: got %q, want %q", u8LenStr, wantU8LenStr)
	}
	if rem != wantRem {
		t.Errorf("remaining: got %d, want %d", rem, wantRem)
	}
}

// TestReader_str_copies verifies that strN, u8LenStr, and u32LenStr all copy
// the source buffer so the returned string is not affected by later mutations.
func TestReader_str_copies(t *testing.T) {
	const wantStr = "hello"

	cases := []struct {
		name    string
		payload func() []byte
		read    func(*reader) string
	}{
		{
			name:    "strN",
			payload: func() []byte { return []byte(wantStr) },
			read:    func(r *reader) string { return r.strN(len(wantStr)) },
		},
		{
			name: "u8LenStr",
			payload: func() []byte {
				return append([]byte{uint8(len(wantStr))}, []byte(wantStr)...)
			},
			read: func(r *reader) string { return r.u8LenStr() },
		},
		{
			name: "u32LenStr",
			payload: func() []byte {
				return append(u32le(uint32(len(wantStr))), []byte(wantStr)...)
			},
			read: func(r *reader) string { return r.u32LenStr() },
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := tc.payload()
			r := newReader(src)
			s := tc.read(r)
			if err := r.Err(); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			src[len(src)-1] = 'X' // mutate source buffer after read
			if s != wantStr {
				t.Errorf("got %q, want %q — string was not copied", s, wantStr)
			}
		})
	}
}

// TestReader_truncation verifies that every read method returns a descriptive error
// when the buffer is too short, including mid-sequence truncation.
func TestReader_truncation(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
		read    func(*reader)
	}{
		{"u8", []byte{}, func(r *reader) { r.u8() }},
		{"u16", []byte{0x01}, func(r *reader) { r.u16() }},                                           // 1 byte, need 2
		{"u32", []byte{0x01, 0x02, 0x03}, func(r *reader) { r.u32() }},                               // 3 bytes, need 4
		{"u64", []byte{0x01, 0x02, 0x03, 0x04}, func(r *reader) { r.u64() }},                         // 4 bytes, need 8
		{"f32", []byte{0x01, 0x02, 0x03}, func(r *reader) { r.f32() }},                               // 3 bytes, need 4
		{"strN", []byte("hi"), func(r *reader) { r.strN(5) }},                                        // claims 5, has 2
		{"u32LenStr/short-len-prefix", []byte{0x05, 0x00}, func(r *reader) { r.u32LenStr() }},        // len prefix needs 4 bytes, got 2
		{"u32LenStr/short-body", cat(u32le(10), []byte("short")), func(r *reader) { r.u32LenStr() }}, // claims 10, has 5
		{"u8LenStr/short-len-prefix", []byte{}, func(r *reader) { r.u8LenStr() }},                    // len prefix needs 1 byte, got 0
		{"u8LenStr/short-body", cat([]byte{10}, []byte("short")), func(r *reader) { r.u8LenStr() }},  // claims 10, has 5
		{"mid-sequence", cat(u32le(1), []byte{0xFF}), func(r *reader) { r.u32(); r.u32() }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReader(tc.payload)
			tc.read(r)
			err := r.Err()
			if err == nil || !strings.HasPrefix(err.Error(), "reader: need ") {
				t.Fatalf("got %v, want overrun error", err)
			}
		})
	}
}

// TestReader_overrun_error_location verifies that the error message contains
// the file and line of the call site that triggered the overrun, for every
// public read method.
func TestReader_overrun_error_location(t *testing.T) {
	cases := []struct {
		name string
		// fn triggers the overrun and returns the expected file:line of the call.
		fn func(r *reader) (wantFile string, wantLine int)
	}{
		{"u8", func(r *reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.u8()
			return file, line + 1
		}},
		{"u16", func(r *reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.u16()
			return file, line + 1
		}},
		{"u32", func(r *reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.u32()
			return file, line + 1
		}},
		{"u64", func(r *reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.u64()
			return file, line + 1
		}},
		{"f32", func(r *reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.f32()
			return file, line + 1
		}},
		{"strN", func(r *reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.strN(1)
			return file, line + 1
		}},
		{"u32LenStr", func(r *reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.u32LenStr()
			return file, line + 1
		}},
		{"u8LenStr", func(r *reader) (string, int) {
			_, file, line, _ := runtime.Caller(0)
			r.u8LenStr()
			return file, line + 1
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReader([]byte{})
			wantFile, wantLine := tc.fn(r)
			checkLoc(t, r.Err(), wantFile, wantLine)
		})
	}
}

func checkLoc(t *testing.T, err error, wantFile string, wantLine int) {
	t.Helper()
	if err == nil {
		t.Error("expected error, got nil")
		return
	}
	wantLoc := fmt.Sprintf("%s:%d", wantFile, wantLine)
	if !strings.Contains(err.Error(), wantLoc) {
		t.Errorf("error %q does not contain location %q", err.Error(), wantLoc)
	}
}
