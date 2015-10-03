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
    internal sealed class NonBlockingDictionaryBoxed<TKey, TValue>
            : NonBlockingDictionary<TKey, Boxed<TKey>, TValue>
    {
        internal override bool TryClaimSlotForPut(ref Boxed<TKey> entryKey, TKey key, Counter slots)
        {
            var entryKeyValue = entryKey;
            if (entryKeyValue == null)
            {
                entryKeyValue = Interlocked.CompareExchange(ref entryKey, new Boxed<TKey>(key), null);
                if (entryKeyValue == null)
                {
                    // claimed a new slot
                    slots.increment();
                    return true;
                }
            }

            if (keyComparer.Equals(key, entryKey.Value))
            {
                // got existing slot
                return true;
            }

            return false;
        }

        internal override bool TryClaimSlotForCopy(ref Boxed<TKey> entryKey,Boxed<TKey> key, Counter slots)
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

            if (keyComparer.Equals(key.Value, entryKey.Value))
            {
                // got existing slot
                return true;
            }

            return false;
        }

        protected override bool keyEqual(TKey key, Boxed<TKey> entryKey)
        {
            return keyComparer.Equals(key, entryKey.Value);
        }

        protected override NonBlockingDictionary<TKey, Boxed<TKey>, TValue> CreateNew()
        {
            return new NonBlockingDictionaryBoxed<TKey, TValue>();
        }

        protected override TKey keyFromEntry(Boxed<TKey> entryKey)
        {
            return entryKey.Value;
        }
    }

    internal class Boxed<T>
    {
        public readonly T Value;

        public Boxed(T key)
        {
            this.Value = key;
        }
    }
}
