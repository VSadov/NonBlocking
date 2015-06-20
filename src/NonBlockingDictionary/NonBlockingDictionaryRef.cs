using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace NonBlocking
{
    internal sealed class NonBlockingDictionaryRef<TKey, TKeyStore, TValue>
            : NonBlockingDictionary<TKey, TKey, TValue>
                    where TKey : class
    {
        internal override bool TryClaimSlotForPut(ref TKey entryKey, TKey key, Counter slots)
        {
            return TryClaimSlot(ref entryKey, key, slots);
        }

        internal override bool TryClaimSlotForCopy(ref TKey entryKey, TKey key, Counter slots)
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

            if (keyEqual(key, entryKeyValue))
            {
                // got existing slot
                return true;
            }

            return false;
        }

        protected override bool keyEqual(TKey key, TKey entryKey)
        {
            return key == entryKey ||
                keyComparer.Equals(key, entryKey);
        }

        protected override NonBlockingDictionary<TKey, TKey, TValue> CreateNew()
        {
            return new NonBlockingDictionaryRef<TKey, TKey, TValue>();
        }
    }
}
