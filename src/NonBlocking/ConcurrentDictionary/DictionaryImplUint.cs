// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System.Runtime.CompilerServices;
using System.Threading;

namespace NonBlocking
{
    internal sealed class DictionaryImplUint<TValue>
                : DictionaryImpl<uint, uint, TValue>
    {
        internal DictionaryImplUint(int capacity, ConcurrentDictionary<uint, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplUint(int capacity, DictionaryImplUint<TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref uint entryKey, uint key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref uint entryKey, uint key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        private bool TryClaimSlot(ref uint entryKey, uint key)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = (uint)Interlocked.CompareExchange(ref Unsafe.As<uint, int>(ref entryKey), (int)key, 0);
                if (entryKeyValue == 0)
                {
                    // claimed a new slot
                    this.allocatedSlotCount.Increment();
                    return true;
                }
            }

            return key == entryKeyValue || _keyComparer.Equals(key, entryKey);
        }

        protected override int hash(uint key)
        {
            if (key == 0)
            {
                return ZEROHASH;
            }

            return base.hash(key);
        }

        protected override bool keyEqual(uint key, uint entryKey)
        {
            return key == entryKey || _keyComparer.Equals(key, entryKey);
        }

        protected override DictionaryImpl<uint, uint, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplUint<TValue>(capacity, this);
        }

        protected override uint keyFromEntry(uint entryKey)
        {
            return entryKey;
        }
    }

    internal sealed class DictionaryImplUintNoComparer<TValue>
            : DictionaryImpl<uint, uint, TValue>
    {
        internal DictionaryImplUintNoComparer(int capacity, ConcurrentDictionary<uint, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplUintNoComparer(int capacity, DictionaryImplUintNoComparer<TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref uint entryKey, uint key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref uint entryKey, uint key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        private bool TryClaimSlot(ref uint entryKey, uint key)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = (uint)Interlocked.CompareExchange(ref Unsafe.As<uint, int>(ref entryKey), (int)key, 0);
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
        internal override object TryGetValue(uint key)
        {
            return base.TryGetValue(key);
        }

        protected override int hash(uint key)
        {
            return (key == 0) ?
                ZEROHASH :
                (int)key | SPECIAL_HASH_BITS;
        }

        protected override bool keyEqual(uint key, uint entryKey)
        {
            return key == entryKey;
        }

        protected override DictionaryImpl<uint, uint, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplUintNoComparer<TValue>(capacity, this);
        }

        protected override uint keyFromEntry(uint entryKey)
        {
            return entryKey;
        }
    }
}
