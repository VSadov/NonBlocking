// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace NonBlocking
{
    internal sealed class DictionaryImplInt<TValue>
                : DictionaryImpl<int, int, TValue>
    {
        internal DictionaryImplInt(ConcurrentDictionary<int, TValue> topDict)
            : base(topDict)
        {
        }

        internal DictionaryImplInt(DictionaryImplInt<TValue> other, int capacity)
            : base(other, capacity)
        {
        }

        protected override bool TryClaimSlotForPut(ref int entryKey, int key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        protected override bool TryClaimSlotForCopy(ref int entryKey, int key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        private bool TryClaimSlot(ref int entryKey, int key, Counter slots)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, key, 0);
                if (entryKeyValue == 0)
                {
                    // claimed a new slot
                    slots.increment();
                    return true;
                }
            }

            return key == entryKeyValue || _keyComparer.Equals(key, entryKey);
        }

        protected override int hash(int key)
        {
            if (key == 0)
            {
                return ZEROHASH;
            }

            return base.hash(key);
        }

        protected override bool keyEqual(int key, int entryKey)
        {
            return key == entryKey || _keyComparer.Equals(key, entryKey);
        }

        protected override DictionaryImpl<int, int, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplInt<TValue>(this, capacity);
        }

        protected override int keyFromEntry(int entryKey)
        {
            return entryKey;
        }
    }

    internal sealed class DictionaryImplIntNoComparer<TValue>
            : DictionaryImpl<int, int, TValue>
    {
        internal DictionaryImplIntNoComparer(ConcurrentDictionary<int, TValue> topDict)
            : base(topDict)
        {
        }

        internal DictionaryImplIntNoComparer(DictionaryImplIntNoComparer<TValue> other, int capacity)
            : base(other, capacity)
        {
        }

        protected override bool TryClaimSlotForPut(ref int entryKey, int key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        protected override bool TryClaimSlotForCopy(ref int entryKey, int key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        private bool TryClaimSlot(ref int entryKey, int key, Counter slots)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, key, 0);
                if (entryKeyValue == 0)
                {
                    // claimed a new slot
                    slots.increment();
                    return true;
                }
            }

            return key == entryKeyValue;
        }

        protected override int hash(int key)
        {
            return (key == 0) ?
                ZEROHASH :
                key | REGULAR_HASH_BITS;
        }

        protected override bool keyEqual(int key, int entryKey)
        {
            return key == entryKey;
        }

        protected override DictionaryImpl<int, int, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplIntNoComparer<TValue>(this, capacity);
        }

        protected override int keyFromEntry(int entryKey)
        {
            return entryKey;
        }
    }
}
