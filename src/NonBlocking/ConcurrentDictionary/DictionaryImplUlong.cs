// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace NonBlocking
{
    internal sealed class DictionaryImplUlong<TValue>
                : DictionaryImpl<ulong, ulong, TValue>
    {
        internal DictionaryImplUlong(int capacity, ConcurrentDictionary<ulong, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplUlong(int capacity, DictionaryImplUlong<TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref ulong entryKey, ulong key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref ulong entryKey, ulong key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        private bool TryClaimSlot(ref ulong entryKey, ulong key)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = (ulong)Interlocked.CompareExchange(ref Unsafe.As<ulong, long>(ref entryKey), (long)key, 0);
                if (entryKeyValue == 0)
                {
                    // claimed a new slot
                    this.allocatedSlotCount.Increment();
                    return true;
                }
            }

            return key == entryKeyValue || _keyComparer.Equals(key, entryKey);
        }

        protected override int hash(ulong key)
        {
            if (key == 0)
            {
                return ZEROHASH;
            }

            return base.hash(key);
        }

        protected override bool keyEqual(ulong key, ulong entryKey)
        {
            return key == entryKey || _keyComparer.Equals(key, entryKey);
        }

        protected override DictionaryImpl<ulong, ulong, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplUlong<TValue>(capacity, this);
        }

        protected override ulong keyFromEntry(ulong entryKey)
        {
            return entryKey;
        }
    }

    internal sealed class DictionaryImplUlongNoComparer<TValue>
            : DictionaryImpl<ulong, ulong, TValue>
    {
        internal DictionaryImplUlongNoComparer(int capacity, ConcurrentDictionary<ulong, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplUlongNoComparer(int capacity, DictionaryImplUlongNoComparer<TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref ulong entryKey, ulong key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref ulong entryKey, ulong key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        private bool TryClaimSlot(ref ulong entryKey, ulong key)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = (ulong)Interlocked.CompareExchange(ref Unsafe.As<ulong, long>(ref entryKey), (long)key, 0);
                if (entryKeyValue == 0)
                {
                    // claimed a new slot
                    this.allocatedSlotCount.Increment();
                    return true;
                }
            }

            return key == entryKeyValue;
        }

        // inline the base implementation to devirtualize calls to hash and keyEqual
        internal override object TryGetValue(ulong key)
        {
            return base.TryGetValue(key);
        }

        protected override int hash(ulong key)
        {
            return (key == 0) ?
                ZEROHASH :
                key.GetHashCode() | SPECIAL_HASH_BITS;
        }

        protected override bool keyEqual(ulong key, ulong entryKey)
        {
            return key == entryKey;
        }

        protected override DictionaryImpl<ulong, ulong, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplUlongNoComparer<TValue>(capacity, this);
        }

        protected override ulong keyFromEntry(ulong entryKey)
        {
            return entryKey;
        }
    }
}
