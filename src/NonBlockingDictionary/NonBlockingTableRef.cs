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
    internal sealed class NonBlockingTableRef<TKey, TKeyStore, TValue>
            : NonBlockingTable<TKey, TKey, TValue>
                    where TKey : class
    {
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

            return key == entryKeyValue || keyComparer.Equals(key, entryKeyValue);
        }

        protected override bool keyEqual(TKey key, TKey entryKey)
        {
            return key == entryKey || keyComparer.Equals(key, entryKey);
        }

        protected override NonBlockingTable<TKey, TKey, TValue> CreateNew()
        {
            return new NonBlockingTableRef<TKey, TKey, TValue>();
        }

        protected override TKey keyFromEntry(TKey entryKey)
        {
            return entryKey;
        }
    }
}
