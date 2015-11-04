// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace NonBlocking
{
    public abstract class NonBlockingDictionary
    {
        internal NonBlockingDictionary() { }

        public static NonBlockingDictionary<TKey, TValue> Create<TKey, TValue>(
            int cLevel,
            int size,
            IEqualityComparer<TKey> comparer = null)
        {
            return Create<TKey, TValue>(comparer);
        }

        public static NonBlockingDictionary<TKey, TValue> Create<TKey, TValue>(
            IEqualityComparer<TKey> comparer = null)
        {
            NonBlockingDictionary<TKey, TValue> result;

            if (default(TKey) == null)
            {
                if (typeof(TKey) == typeof(ValueType) ||
                    !(default(TKey) is ValueType))
                {
                    return CreateRefUnsafe<TKey, TValue>(comparer);
                }
            }
            else
            {
                if (typeof(TKey) == typeof(int))
                {
                    if (comparer == null)
                    {
                        return (NonBlockingDictionary<TKey, TValue>)(object)new NonBlockingDictionaryIntNoComparer<TValue>();
                    }

                    result = (NonBlockingDictionary<TKey, TValue>)(object)new NonBlockingDictionaryInt<TValue>();
                    result.keyComparer = comparer;
                    return result;
                }

                if (typeof(TKey) == typeof(long))
                {
                    if (comparer == null)
                    {
                        return (NonBlockingDictionary<TKey, TValue>)(object)new NonBlockingDictionaryLongNoComparer<TValue>();
                    }

                    result = (NonBlockingDictionary<TKey, TValue>)(object)new NonBlockingDictionaryLong<TValue>();
                    result.keyComparer = comparer;
                    return result;
                }
            }

            result = new NonBlockingDictionaryBoxed<TKey, TValue>();
            result.keyComparer = comparer ?? EqualityComparer<TKey>.Default;
            return result;
        }

        internal static NonBlockingDictionary<TKey, TValue> CreateRefUnsafe<TKey, TValue>(IEqualityComparer<TKey> comparer = null)
        {
            return (NonBlockingDictionary<TKey, TValue>)typeof(NonBlockingDictionary).
                GetMethod("CreateRef", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static).
                MakeGenericMethod(new Type[] { typeof(TKey), typeof(TValue) }).
                Invoke(null, new object[] { comparer });

            //var result = (NonBlockingDictionary<TKey, TValue>)
            //    Activator.CreateInstance(typeof(NonBlockingDictionaryRef<,>).MakeGenericType(new Type[] { typeof(TKey), typeof(TValue) }));

            //result.keyComparer = comparer ?? EqualityComparer<TKey>.Default;
            //return result;
        }

        internal static NonBlockingDictionary<TKey, TValue> CreateRef<TKey, TValue>(IEqualityComparer<TKey> comparer = null)
            where TKey : class
        {
            var result = new NonBlockingDictionaryRef<TKey, TKey, TValue>();
            result.keyComparer = comparer ?? EqualityComparer<TKey>.Default;
            return result;
        }

        ///////////////
        // Internal statics that do not need to be generic on TKey/TValue

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
    }
}
