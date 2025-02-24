// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System.Runtime.CompilerServices;
using System.Threading;

namespace NonBlocking
{
    internal sealed class DictionaryImplNuint<TValue>
                : DictionaryImpl<nuint, nuint, TValue>
    {
        internal DictionaryImplNuint(int capacity, ConcurrentDictionary<nuint, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplNuint(int capacity, DictionaryImplNuint<TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref nuint entryKey, nuint key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref nuint entryKey, nuint key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        private unsafe bool TryClaimSlot(ref nuint entryKey, nuint key)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = (nuint)Interlocked.CompareExchange(ref Unsafe.As<nuint, nint>(ref entryKey), (nint)key, (nint)0).ToPointer();
                if (entryKeyValue == 0)
                {
                    // claimed a new slot
                    this.allocatedSlotCount.Increment();
                    return true;
                }
            }

            return key == entryKeyValue || _keyComparer.Equals(key, entryKey);
        }

        protected override int hash(nuint key)
        {
            if (key == 0)
            {
                return ZEROHASH;
            }

            return base.hash(key);
        }

        protected override bool keyEqual(nuint key, nuint entryKey)
        {
            return key == entryKey || _keyComparer.Equals(key, entryKey);
        }

        protected override DictionaryImpl<nuint, nuint, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplNuint<TValue>(capacity, this);
        }

        protected override nuint keyFromEntry(nuint entryKey)
        {
            return entryKey;
        }
    }

    internal sealed class DictionaryImplNuintNoComparer<TValue>
            : DictionaryImpl<nuint, nuint, TValue>
    {
        internal DictionaryImplNuintNoComparer(int capacity, ConcurrentDictionary<nuint, TValue> topDict)
            : base(capacity, topDict)
        {
        }

        internal DictionaryImplNuintNoComparer(int capacity, DictionaryImplNuintNoComparer<TValue> other)
            : base(capacity, other)
        {
        }

        protected override bool TryClaimSlotForPut(ref nuint entryKey, nuint key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        protected override bool TryClaimSlotForCopy(ref nuint entryKey, nuint key)
        {
            return TryClaimSlot(ref entryKey, key);
        }

        private unsafe bool TryClaimSlot(ref nuint entryKey, nuint key)
        {
            var entryKeyValue = entryKey;
            //zero keys are claimed via hash
            if (entryKeyValue == 0 & key != 0)
            {
                entryKeyValue = (nuint)Interlocked.CompareExchange(ref Unsafe.As<nuint, nint>(ref entryKey), (nint)key, (nint)0).ToPointer();
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
        internal override object TryGetValue(nuint key)
        {
            return base.TryGetValue(key);
        }

        protected override int hash(nuint key)
        {
            return (key == 0) ?
                ZEROHASH :
                key.GetHashCode() | SPECIAL_HASH_BITS;
        }

        protected override bool keyEqual(nuint key, nuint entryKey)
        {
            return key == entryKey;
        }

        protected override DictionaryImpl<nuint, nuint, TValue> CreateNew(int capacity)
        {
            return new DictionaryImplNuintNoComparer<TValue>(capacity, this);
        }

        protected override nuint keyFromEntry(nuint entryKey)
        {
            return entryKey;
        }
    }
}
