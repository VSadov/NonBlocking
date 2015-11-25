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
    internal sealed class DictionaryImplRef<TKey, TKeyStore, TValue>
            : DictionaryImpl<TKey, TKey, TValue>
                    where TKey : class
    {
        internal DictionaryImplRef(ConcurrentDictionary<TKey, TValue> topDict)
            : base(topDict)
        {
        }

        internal DictionaryImplRef(DictionaryImplRef<TKey, TKeyStore, TValue> other, int capacity)
            : base(other, capacity)
        {
        }

        protected override bool TryClaimSlotForPut(ref TKey entryKey, TKey key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        protected override bool TryClaimSlotForCopy(ref TKey entryKey, TKey key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        private bool TryClaimSlot(ref TKey entryKey, TKey key, Counter slots)
        {
            var entryKeyValue = entryKey;
            if (entryKeyValue == null)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, key, null);
                if (entryKeyValue == null)
                {
                    // claimed a new slot
                    slots.increment();
                    return true;
                }
            }

            return key == entryKeyValue || _keyComparer.Equals(key, entryKeyValue);
        }

        protected override bool keyEqual(TKey key, TKey entryKey)
        {
            return key == entryKey || _keyComparer.Equals(key, entryKey);
        }

        protected override DictionaryImpl<TKey, TKey, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplRef<TKey, TKeyStore, TValue>(this, capacity);
        }

        protected override TKey keyFromEntry(TKey entryKey)
        {
            return entryKey;
        }
    }
}
