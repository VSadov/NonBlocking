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
    internal sealed class NonBlockingDictionaryLong<TValue>
                : NonBlockingDictionary<long, long, TValue>
    {
        protected override bool TryClaimSlotForPut(ref long entryKey, long key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        protected override bool TryClaimSlotForCopy(ref long entryKey, long key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        private bool TryClaimSlot(ref long entryKey, long key, Counter slots)
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

            return key == entryKeyValue || keyComparer.Equals(key, entryKey);
        }

        protected override int hash(long key)
        {
            if (key == 0)
            {
                return ZEROHASH;
            }

            return base.hash(key);
        }

        protected override bool keyEqual(long key, long entryKey)
        {
            return key == entryKey || keyComparer.Equals(key, entryKey);
        }

        protected override NonBlockingDictionary<long, long, TValue> CreateNew()
        {
            return new NonBlockingDictionaryLong<TValue>();
        }

        protected override long keyFromEntry(long entryKey)
        {
            return entryKey;
        }
    }

    internal sealed class NonBlockingDictionaryLongNoComparer<TValue>
            : NonBlockingDictionary<long, long, TValue>
    {
        protected override bool TryClaimSlotForPut(ref long entryKey, long key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        protected override bool TryClaimSlotForCopy(ref long entryKey, long key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        private bool TryClaimSlot(ref long entryKey, long key, Counter slots)
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

        protected override int hash(long key)
        {
            return (key == 0) ?
                ZEROHASH :
                key.GetHashCode() | REGULAR_HASH_BITS;
        }

        protected override bool keyEqual(long key, long entryKey)
        {
            return key == entryKey;
        }

        protected override NonBlockingDictionary<long, long, TValue> CreateNew()
        {
            return new NonBlockingDictionaryLongNoComparer<TValue>();
        }

        protected override long keyFromEntry(long entryKey)
        {
            return entryKey;
        }
    }
}
