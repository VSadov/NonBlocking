using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace NonBlocking
{
    internal sealed class NonBlockingDictionaryInt<TValue>
                : NonBlockingDictionary<int, int, TValue>
    {
        internal override bool TryClaimSlotForPut(ref int entryKey, int key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        internal override bool TryClaimSlotForCopy(ref int entryKey, int key, Counter slots)
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

            if (key == entryKeyValue || keyComparer.Equals(key, entryKey))
            {
                return true;
            }

            return false;
        }

        internal override int hash(int key)
        {
            if (key == 0)
            {
                return ZEROHASH;
            }

            return base.hash(key);
        }

        protected override bool keyEqual(int key, int entryKey)
        {
            return key == entryKey || keyComparer.Equals(key, entryKey);
        }

        protected override NonBlockingDictionary<int, int, TValue> CreateNew()
        {
            return new NonBlockingDictionaryInt<TValue>();
        }
    }

    internal sealed class NonBlockingDictionaryIntNoComparer<TValue>
            : NonBlockingDictionary<int, int, TValue>
    {
        internal override bool TryClaimSlotForPut(ref int entryKey, int key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        internal override bool TryClaimSlotForCopy(ref int entryKey, int key, Counter slots)
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

            if (key == entryKeyValue)
            {
                return true;
            }

            return false;
        }

        internal override int hash(int key)
        {
            return (key == 0)? 
                ZEROHASH: 
                key | REGULAR_HASH_BITS;
        }

        protected override bool keyEqual(int key, int entryKey)
        {
            return key == entryKey;
        }

        protected override NonBlockingDictionary<int, int, TValue> CreateNew()
        {
            return new NonBlockingDictionaryIntNoComparer<TValue>();
        }
    }
}
