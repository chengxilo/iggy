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
// Reader and verifies the values survive the round-trip.
func TestWriter_roundTrip(t *testing.T) {
	const wantU8 uint8 = math.MaxUint8
	const wantU16 uint16 = math.MaxUint16
	const wantU32 uint32 = math.MaxUint32
	const wantU64 uint64 = math.MaxUint64
	const wantF32 float32 = math.Pi

	wantStr := "str"
	wantU32LenStr := "uint32"
	wantU8LenStr := "uint8"

	w := NewWriter()
	w.U8(wantU8)
	w.U16(wantU16)
	w.U32(wantU32)
	w.U64(wantU64)
	w.F32(wantF32)
	w.Str(wantStr)
	w.U32LenStr(wantU32LenStr)
	w.U8LenStr(wantU8LenStr)

	r := NewReader(w.Bytes())
	u8 := r.U8()
	u16 := r.U16()
	u32 := r.U32()
	u64 := r.U64()
	f32 := r.F32()
	str := r.Str(len(wantStr))
	u32LenStr := r.U32LenStr()
	u8LenStr := r.U8LenStr()
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected read error: %v", err)
	}
	if r.Remaining() != 0 {
		t.Errorf("unexpected trailing bytes: %d", r.Remaining())
	}

	if u8 != wantU8 {
		t.Errorf("U8: got %#x, want %#x", u8, wantU8)
	}
	if u16 != wantU16 {
		t.Errorf("U16: got %#x, want %#x", u16, wantU16)
	}
	if u32 != wantU32 {
		t.Errorf("U32: got %#x, want %#x", u32, wantU32)
	}
	if u64 != wantU64 {
		t.Errorf("U64: got %#x, want %#x", u64, wantU64)
	}
	if f32 != wantF32 {
		t.Errorf("F32: got %v, want %v", f32, wantF32)
	}
	if str != wantStr {
		t.Errorf("Str: got %q, want %q", str, wantStr)
	}
	if u32LenStr != wantU32LenStr {
		t.Errorf("U32LenStr: got %q, want %q", u32LenStr, wantU32LenStr)
	}
	if u8LenStr != wantU8LenStr {
		t.Errorf("U8LenStr: got %q, want %q", u8LenStr, wantU8LenStr)
	}
}

// TestWriterCap_noAlloc verifies that NewWriterCap avoids
// reallocations when the provided capacity is sufficient.
func TestWriterCap_noAlloc(t *testing.T) {
	const n = 4 + 1 + len("name") // U32 + U8LenStr("name")
	w := NewWriterCap(n)
	capBefore := cap(w.p)
	w.U32(42)
	w.U8LenStr("name")
	if cap(w.p) != capBefore {
		t.Errorf("reallocation occurred: cap before=%d, after=%d", capBefore, cap(w.p))
	}
	if len(w.p) != n {
		t.Errorf("unexpected length: got %d, want %d", len(w.p), n)
	}
}
