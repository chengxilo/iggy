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
	"math"
	"testing"
)

// TestWriter_roundTrip writes every method in sequence then reads back with
// reader and verifies the values survive the round-trip.
func TestWriter_roundTrip(t *testing.T) {
	const wantU8 uint8 = math.MaxUint8
	const wantU16 uint16 = math.MaxUint16
	const wantU32 uint32 = math.MaxUint32
	const wantU64 uint64 = math.MaxUint64
	const wantF32 float32 = math.Pi

	wantStrN := "str"
	wantU32LenStr := "uint32"
	wantU8LenStr := "uint8"

	w := newWriter()
	w.u8(wantU8)
	w.u16(wantU16)
	w.u32(wantU32)
	w.u64(wantU64)
	w.f32(wantF32)
	w.strN(wantStrN)
	w.u32LenStr(wantU32LenStr)
	w.u8LenStr(wantU8LenStr)

	r := newReader(w.bytes())
	u8 := r.u8()
	u16 := r.u16()
	u32 := r.u32()
	u64 := r.u64()
	f32 := r.f32()
	strN := r.strN(len(wantStrN))
	u32LenStr := r.u32LenStr()
	u8LenStr := r.u8LenStr()
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if r.remaining() != 0 {
		t.Errorf("unexpected trailing bytes: %d", r.remaining())
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
}

// TestWriterCap_noAlloc verifies that newWriterCap avoids
// reallocations when the provided capacity is sufficient.
func TestWriterCap_noAlloc(t *testing.T) {
	const n = 4 + 1 + len("name") // u32 + u8LenStr("name")
	w := newWriterCap(n)
	capBefore := cap(w.p)
	w.u32(42)
	w.u8LenStr("name")
	if cap(w.p) != capBefore {
		t.Errorf("reallocation occurred: cap before=%d, after=%d", capBefore, cap(w.p))
	}
	if len(w.p) != n {
		t.Errorf("unexpected length: got %d, want %d", len(w.p), n)
	}
}
