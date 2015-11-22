// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace NonBlocking
{
    internal abstract class NonBlockingTable
    {
        internal NonBlockingTable() { }

        internal enum ValueMatch
        {
            Any,            // sets new value unconditionally, used by index set
            NullOrDead,     // set value if original value is null or dead, used by Add/TryAdd
            NotNullOrDead,  // set value if original value is alive, used by Remove
            OldValue,       // sets new value if old value matches
        }

        internal sealed class Prime
        {
            internal object originalValue;

            public Prime(object originalValue)
            {
                this.originalValue = originalValue;
            }
        }

        internal static readonly object TOMBSTONE = new object();
        internal static readonly Prime TOMBPRIME = new Prime(TOMBSTONE);
        internal static readonly object NULLVALUE = new object();

        // represents forcefully dead entry 
        // we insert it in old table during rehashing
        // to reduce chances that more entries are added
        internal const int TOMBPRIMEHASH = 1 << 31;

        // we cannot distigush zero keys from uninitialized state
        // so we force them to have this special hash instead
        internal const int ZEROHASH = 1 << 30;

        // all regular hashes have these bits set
        // to be different from 0, TOMBPRIMEHASH or ZEROHASH
        internal const int REGULAR_HASH_BITS = TOMBPRIMEHASH | ZEROHASH;

        internal static bool EntryValueNullOrDead(object entryValue)
        {
            return entryValue == null | entryValue == TOMBSTONE;
        }

        internal static int ReduceHashToIndex(int fullHash, int lenMask)
        {
            var h = fullHash & ~REGULAR_HASH_BITS;
            if (h <= lenMask)
            {
                return h;
            }

            return MixAndMask((uint)h, lenMask);
        }

        private static int MixAndMask(uint h, int lenMask)
        {
            // 32-bit finalizer for MurmurHash3.
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;

            h &= (uint)lenMask;

            return (int)h;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        internal static object ToObjectValue<TValue>(TValue value)
        {
            if (default(TValue) == null)
            {
                return (object)value ?? NULLVALUE;
            }

            return (object)value;
        }

        internal static NonBlockingTable<TKey, TValue> CreateRef<TKey, TValue>(IEqualityComparer<TKey> comparer = null)
            where TKey : class
        {
            var result = new NonBlockingTableRef<TKey, TKey, TValue>();
            result.keyComparer = comparer ?? EqualityComparer<TKey>.Default;
            return result;
        }
    }
}
