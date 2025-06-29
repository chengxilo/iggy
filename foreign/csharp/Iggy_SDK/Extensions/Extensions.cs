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

using System.Buffers.Binary;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using Apache.Iggy.Enums;

namespace Apache.Iggy.Extensions;

internal static class Extensions
{
    internal static string ToSnakeCase(this string input)
    {
        Debug.Assert(!string.IsNullOrEmpty(input));
        if (CountUppercaseLetters(input) == 0)
            return input.ToLower();

        var len = input.Length + CountUppercaseLetters(input) - 1;
        return string.Create(len, input, (span, value) =>
        {
            value.AsSpan().CopyTo(span);
            span[0] = char.ToLower(span[0]);

            for (int i = 0; i < len; ++i)
            {
                if (char.IsUpper(span[i]))
                {
                    span[i] = char.ToLower(span[i]);
                    span[i..].ShiftSliceRight();
                    span[i] = '_';
                }
            }
        });
    }

    internal static UInt128 ToUInt128(this Guid g)
    {
        Span<byte> array = stackalloc byte[16];
        #pragma warning disable CS9191 // The 'ref' modifier for an argument corresponding to 'in' parameter is equivalent to 'in'. Consider using 'in' instead.
        MemoryMarshal.TryWrite(array, ref g);
        var hi = BinaryPrimitives.ReadUInt64LittleEndian(array[..8]);
        var lo = BinaryPrimitives.ReadUInt64LittleEndian(array[8..16]);
        return new UInt128(hi, lo);
    }
    internal static UInt128 ToUInt128(this byte[] bytes)
    {
        var lo = BinaryPrimitives.ReadUInt64LittleEndian(bytes[..8]);
        var hi = BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..16]);
        return new UInt128(hi, lo);
    }
    internal static Int128 ToInt128(this byte[] bytes)
    {
        var lo = BinaryPrimitives.ReadUInt64LittleEndian(bytes[..8]);
        var hi = BinaryPrimitives.ReadUInt64LittleEndian(bytes[8..16]);
        return new Int128(hi, lo);
    }
    internal static byte[] GetBytesFromGuid(this Guid value)
    {
        Span<byte> bytes = stackalloc byte[16];
        #pragma warning disable CS9191 // The 'ref' modifier for an argument corresponding to 'in' parameter is equivalent to 'in'. Consider using 'in' instead.
        MemoryMarshal.TryWrite(bytes, ref value);
        return bytes.ToArray();
    }
    internal static byte[] GetBytesFromUInt128(this UInt128 value)
    {
        Span<byte> result = stackalloc byte[16];
        var span = MemoryMarshal.Cast<UInt128, byte>(MemoryMarshal.CreateReadOnlySpan(ref value, 1));
        return span.ToArray();
    }
    internal static byte[] GetBytesFromInt128(this Int128 value)
    {
        Span<byte> result = stackalloc byte[16];
        var span = MemoryMarshal.Cast<Int128, byte>(MemoryMarshal.CreateReadOnlySpan(ref value, 1));
        return span.ToArray();
    }

    internal static UInt128 GetUInt128(this JsonElement jsonElement)
    {
        return UInt128.Parse(jsonElement.ToString());
    }

    private static int CountUppercaseLetters(string input)
    {
        return input.Count(char.IsUpper);
    }
    private static void ShiftSliceRight(this Span<char> slice)
    {
        for (int i = slice.Length - 2; i >= 0; i--)
        {
            slice[i + 1] = slice[i];
        }
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteBytesFromStreamAndTopicIdentifiers(this Span<byte> bytes, Identifier streamId, Identifier topicId, int startPos = 0)
    {
        bytes[startPos] = streamId.Kind.GetByte();
        bytes[startPos + 1] = (byte)streamId.Length;
        streamId.Value.CopyTo(bytes[(startPos + 2)..(startPos + 2 + streamId.Length)]);

        var position = startPos + 2 + streamId.Length;
        bytes[position] = topicId.Kind.GetByte();
        bytes[position + 1] = (byte)topicId.Length;
        topicId.Value.CopyTo(bytes[(position + 2)..(position + 2 + topicId.Length)]);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteBytesFromIdentifier(this Span<byte> bytes, Identifier identifier, int startPos = 0)
    {
        bytes[startPos + 0] = identifier.Kind.GetByte();
        bytes[startPos + 1] = (byte)identifier.Length;
        identifier.Value.CopyTo(bytes[(startPos + 2)..]);
    }
    
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static void WriteBytesFromPartitioning(this Span<byte> bytes, Kinds.Partitioning identifier, int startPos = 0)
    {
        bytes[startPos + 0] = identifier.Kind.GetByte();
        bytes[startPos + 1] = (byte)identifier.Length;
        identifier.Value.CopyTo(bytes[(startPos + 2)..]);
    }
}

internal static class DateTimeOffsetUtils
{
    internal static DateTimeOffset FromUnixTimeMicroSeconds(ulong microSeconds)
    {
        return DateTimeOffset.FromUnixTimeMilliseconds((long)(microSeconds / 1000));
    }
    
    internal static ulong ToUnixTimeMicroSeconds(DateTimeOffset date)
    {
        return (ulong)(date.ToUnixTimeMilliseconds() * 1000);
    }
}