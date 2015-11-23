// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

//
// Core algorithms are based on NonBlockingHashMap, 
// written and released to the public domain by Dr.Cliff Click.
// A good overview is here https://www.youtube.com/watch?v=HJ-719EGIts
//

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace NonBlocking
{
    internal abstract partial class DictionaryImpl<TKey, TKeyStore, TValue>
        : DictionaryImpl<TKey, TValue>
    {
        public struct Entry
        {
            internal int hash;
            internal TKeyStore key;
            internal object value;
        }

        private Entry[] _table;
        private uint _lastResizeTickMillis;

        private const int REPROBE_LIMIT = 4;
        private const int REPROBE_LIMIT_SHIFT = 1;
        private const int MIN_SIZE = 8;

        // targeted time span between resizes.
        // if resizing more often than this, try expanding.
        const uint RESIZE_MILLIS_TARGET = (uint)1000;

        // create a fresh empty dictionary (used in Clear) 
        protected abstract DictionaryImpl<TKey, TKeyStore, TValue> CreateNew();

        // convert key from its storage form (noop or unboxing) used in Key enumarators
        protected abstract TKey keyFromEntry(TKeyStore entryKey);

        // compares key with another in its storage form
        protected abstract bool keyEqual(TKey key, TKeyStore entryKey);

        // claiming (by writing atomically to the entryKey location) 
        // or getting existing slot suitable for storing a given key.
        protected abstract bool TryClaimSlotForPut(ref TKeyStore entryKey, TKey key, Counter slots);

        // claiming (by writing atomically to the entryKey location) 
        // or getting existing slot suitable for storing a given key in its store form (could be boxed).
        protected abstract bool TryClaimSlotForCopy(ref TKeyStore entryKey, TKeyStore key, Counter slots);

        // NOTE: Not Staitc For perf reasons
        private TableInfo GetTableInfo(Entry[] table)
        {
            return (TableInfo)table[table.Length - 1].value;
        }

        // NOTE: Not Staitc For perf reasons
        private int GetTableLength(Entry[] table)
        {
            return table.Length - 1;
        }

        // Heuristic to decide if we have reprobed toooo many times.  Running over
        // the reprobe limit on a 'get' call acts as a 'miss'; on a 'put' call it
        // can trigger a table resize.  Several places must have exact agreement on
        // what the reprobe_limit is, so we share it here.
        // NOTE: Not static for perf reasons    
        //       (some JITs insert useless code related to generics if this is a static)
        private int ReprobeLimit(int lenMask)
        {
            // 1/2 of table with some extra
            return REPROBE_LIMIT + (lenMask >> REPROBE_LIMIT_SHIFT);
        }

        internal DictionaryImpl() :
            this(MIN_SIZE)
        { }

        // TODO: VS need to make public entry point
        internal DictionaryImpl(int capacity)
        {
            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }

            capacity = AlignToPowerOfTwo(capacity);
            _table = new Entry[capacity + 1];
            _table[capacity].value = new TableInfo(new Counter());
        }

        private static uint CurrentTickMillis()
        {
            return (uint)Environment.TickCount;
        }

        private static int AlignToPowerOfTwo(int size)
        {
            Debug.Assert(size > 0);

            size--;
            size |= size >> 1;
            size |= size >> 2;
            size |= size >> 4;
            size |= size >> 8;
            size |= size >> 16;
            return size + 1;
        }

        protected virtual int hash(TKey key)
        {
            int h = keyComparer.GetHashCode(key);

            // ensure that hash never matches 0, TOMBPRIMEHASH or ZEROHASH
            return h | REGULAR_HASH_BITS;
        }

        /// <summary>
        /// returns null if value is not present in the table
        /// otherwise returns the actual value or NULLVALUE if null is the actual value 
        /// </summary>
        internal sealed override object TryGetValue(TKey key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            Entry[] table = this._table;
            int fullHash = this.hash(key);

            TRY_WITH_NEW_TABLE:

            var lenMask = GetTableLength(table) - 1;
            int idx = ReduceHashToIndex(fullHash, lenMask);

            // Main spin/reprobe loop
            int reprobeCnt = 0;
            while (true)
            {
                // hash, key and value are all CAS-ed down and follow a specific sequence of states.
                // hence the order of these reads is irrelevant and they do not need to be volatile
                var entryHash = table[idx].hash;
                if (entryHash == 0)
                {
                    // the slot has not been claimed - a clear miss
                    break;
                }

                // is this our slot?
                if (fullHash == entryHash &&
                    keyEqual(key, table[idx].key))
                {
                    var entryValue = table[idx].value;

                    if (EntryValueNullOrDead(entryValue))
                    {
                        break;
                    }

                    if (!(entryValue is Prime))
                    {
                        return entryValue;
                    }

                    // found a prime, that means copying has started 
                    // and all new values go to the new table
                    // help with copying and retry in the new table
                    var tableInfo = GetTableInfo(table);
                    table = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: true);

                    // return this.TryGet(newTable, entryKey, hash, out value); 
                    goto TRY_WITH_NEW_TABLE;
                }

                // get and put must have the same key lookup logic.
                // But only 'put' needs to force a table-resize for a too-long key-reprobe sequence
                // hitting reprobe limit or finding TOMBPRIMEHASH here means that the key is not in this table, 
                // but there could be more in the new table
                if (++reprobeCnt >= ReprobeLimit(lenMask) |
                    entryHash == TOMBPRIMEHASH)
                {
                    table = GetTableInfo(table)._newTable;
                    if (table != null)
                    {
                        this.HelpCopy();

                        //return this.TryGet(newTable, entryKey, hash, out value);
                        goto TRY_WITH_NEW_TABLE;
                    }

                    // no new table, so this is a miss
                    break;
                }

                // quadratic reprobe
                idx = (idx + reprobeCnt) & lenMask;
            }

            return null;
        }

        // 1) finds or creates a slot for the key
        // 2) sets the slot value to the putval if original value meets expVal condition
        // 3) returns true if the value was actually changed 
        // Note that pre-existence of the slot is irrelevant 
        // since slot without a value is as good as no slot at all
        internal sealed override bool PutIfMatch(TKey key, object newVal, ref object oldVal, ValueMatch match)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            var table = this._table;
            int fullHash = hash(key);

            TRY_WITH_NEW_TABLE:

            Debug.Assert(newVal != null);
            Debug.Assert(!(newVal is Prime));

            int lenMask = GetTableLength(table) - 1;
            var tableInfo = GetTableInfo(table);
            int idx = ReduceHashToIndex(fullHash, lenMask);

            // Spin till we get a slot for the key or force a resizing.
            int reprobe_cnt = 0;
            while (true)
            {
                // hash, key and value are all CAS-ed down and follow a specific sequence of states.
                // hence the order of their reads is irrelevant and they do not need to be volatile    
                var entryHash = table[idx].hash;
                if (entryHash == 0)
                {
                    // Found an unassigned slot - which means this 
                    // key has never been in this table.
                    if (newVal == TOMBSTONE)
                    {
                        Debug.Assert(match == ValueMatch.NotNullOrDead || match == ValueMatch.OldValue);
                        oldVal = null;
                        goto FAILED;
                    }
                    else
                    {
                        // Slot is completely clean, claim the hash first
                        Debug.Assert(fullHash != 0);
                        entryHash = Interlocked.CompareExchange(ref table[idx].hash, fullHash, 0);
                        if (entryHash == 0)
                        {
                            entryHash = fullHash;
                            if (entryHash == ZEROHASH)
                            {
                                // "added" entry for zero key
                                tableInfo._slots.increment();
                                break;
                            }
                        }
                    }
                }

                if (entryHash == fullHash)
                {
                    // hash is good, one way or another, 
                    // try claiming the slot for the key
                    if (TryClaimSlotForPut(ref table[idx].key, key, tableInfo._slots))
                    {
                        break;
                    }
                }

                // here we know that this slot does not map to our key
                // and must reprobe or resize
                // hitting reprobe limit or finding TOMBPRIMEHASH here means that the key is not in this table, 
                // but there could be more in the new table
                if (++reprobe_cnt >= ReprobeLimit(lenMask) |
                    entryHash == TOMBPRIMEHASH)
                {
                    // start resize or get new table if resize is already in progress
                    table = tableInfo.Resize(this, table);
                    // help along an existing copy
                    this.HelpCopy();
                    goto TRY_WITH_NEW_TABLE;
                }

                // quadratic reprobing
                idx = (idx + reprobe_cnt) & lenMask;
            }

            // Found the proper Key slot, now update the Value.  
            // We never put a null, so Value slots monotonically move from null to
            // not-null (deleted Values use Tombstone).

            // volatile read to make sure we read the element before we read the _newTable
            // that would guarantee that as long as _newTable == null, entryValue cannot be a Prime.
            var entryValue = Volatile.Read(ref table[idx].value);

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).
            var newTable = tableInfo._newTable;

            // newTable == entryValue only when both are nulls
            if ((object)newTable == (object)entryValue && 
                tableInfo.TableIsCrowded(lenMask))
            {
                // Force the new table copy to start
                newTable = tableInfo.Resize(this, table);
                Debug.Assert(tableInfo._newTable != null);
            }

            // See if we are moving to a new table.
            // If so, copy our slot and retry in the new table.
            if (newTable != null)
            {
                var newTable1 = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: true);
                Debug.Assert(newTable == newTable1);
                table = newTable;
                goto TRY_WITH_NEW_TABLE;
            }

            // We are finally prepared to update the existing table
            while (true)
            {
                Debug.Assert(!(entryValue is Prime));
                var entryValueNullOrDead = EntryValueNullOrDead(entryValue);

                switch (match)
                {
                    case ValueMatch.Any:
                        if (newVal == entryValue)
                        {
                            // Do not update!
                            goto FAILED;
                        }
                        break;

                    case ValueMatch.NullOrDead:
                        if (entryValueNullOrDead)
                        {
                            break;
                        }

                        oldVal = entryValue;
                        goto FAILED;

                    case ValueMatch.NotNullOrDead:
                        if (entryValueNullOrDead)
                        {
                            goto FAILED;
                        }
                        break;
                    case ValueMatch.OldValue:
                        Debug.Assert(oldVal != null);
                        if (!oldVal.Equals(entryValue))
                        {
                            oldVal = entryValue;
                            goto FAILED;
                        }
                        break;
                }

                // Actually change the Value 
                var prev = Interlocked.CompareExchange(ref table[idx].value, newVal, entryValue);
                if (prev == entryValue)
                {
                    // CAS succeeded - we did the update!
                    // Adjust sizes
                    if (entryValueNullOrDead)
                    {
                        oldVal = null;
                        if (newVal != TOMBSTONE)
                        {
                            tableInfo._size.increment();
                        }
                    }
                    else
                    {
                        oldVal = prev;
                        if (newVal == TOMBSTONE)
                        {
                            tableInfo._size.decrement();
                        }
                    }

                    return true;
                }
                // Else CAS failed

                // If a Prime'd value got installed, we need to re-run the put on the new table.  
                if (prev is Prime)
                {
                    newTable = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: true);
                    table = newTable;
                    goto TRY_WITH_NEW_TABLE;
                }

                // Otherwise we lost the CAS to another racing put.
                // Simply retry from the start.
                entryValue = prev;
            }

            FAILED:
            return false;
        }

        internal sealed override TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            if (valueFactory == null)
            {
                throw new ArgumentNullException("valueFactory");
            }

            object newValObj = null;
            TValue result = default(TValue);

            var table = this._table;
            int fullHash = hash(key);

            TRY_WITH_NEW_TABLE:

            int lenMask = GetTableLength(table) - 1;
            var tableInfo = GetTableInfo(table);
            int idx = ReduceHashToIndex(fullHash, lenMask);

            // Spin till we get a slot for the key or force a resizing.
            int reprobe_cnt = 0;
            while (true)
            {
                // hash, key and value are all CAS-ed down and follow a specific sequence of states.
                // hence the order of their reads is irrelevant and they do not need to be volatile    
                var entryHash = table[idx].hash;
                if (entryHash == 0)
                {
                    // Found an unassigned slot - which means this 
                    // key has never been in this table.
                    // Slot is completely clean, claim the hash first
                    Debug.Assert(fullHash != 0);
                    entryHash = Interlocked.CompareExchange(ref table[idx].hash, fullHash, 0);
                    if (entryHash == 0)
                    {
                        entryHash = fullHash;
                        if (entryHash == ZEROHASH)
                        {
                            // "added" entry for zero key
                            tableInfo._slots.increment();
                            break;
                        }
                    }
                }

                if (entryHash == fullHash)
                {
                    // hash is good, one way or another, 
                    // try claiming the slot for the key
                    if (TryClaimSlotForPut(ref table[idx].key, key, tableInfo._slots))
                    {
                        break;
                    }
                }

                // here we know that this slot does not map to our key
                // and must reprobe or resize
                // hitting reprobe limit or finding TOMBPRIMEHASH here means that the key is not in this table, 
                // but there could be more in the new table
                if (++reprobe_cnt >= ReprobeLimit(lenMask) |
                    entryHash == TOMBPRIMEHASH)
                {
                    // start resize or get new table if resize is already in progress
                    table = tableInfo.Resize(this, table);
                    // help along an existing copy
                    this.HelpCopy();
                    goto TRY_WITH_NEW_TABLE;
                }

                // quadratic reprobing
                idx = (idx + reprobe_cnt) & lenMask;
            }

            // Found the proper Key slot, now update the Value.  
            // We never put a null, so Value slots monotonically move from null to
            // not-null (deleted Values use Tombstone).

            // volatile read to make sure we read the element before we read the _newTable
            // that would guarantee that as long as _newTable == null, entryValue cannot be a Prime.
            var entryValue = Volatile.Read(ref table[idx].value);

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).
            var newTable = tableInfo._newTable;

            // newTable == entryValue only when both are nulls
            if ((object)newTable == (object)entryValue &&
                tableInfo.TableIsCrowded(lenMask))
            {
                // Force the new table copy to start
                newTable = tableInfo.Resize(this, table);
                Debug.Assert(tableInfo._newTable != null);
            }

            // See if we are moving to a new table.
            // If so, copy our slot and retry in the new table.
            if (newTable != null)
            {
                var newTable1 = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: true);
                Debug.Assert(newTable == newTable1);
                table = newTable;
                goto TRY_WITH_NEW_TABLE;
            }

            if (!EntryValueNullOrDead(entryValue))
            {
                goto GOT_PREV_VALUE;
            }

            // prev value is not null, dead or prime.
            // let's try install new value
            newValObj = newValObj ?? ToObjectValue(result = valueFactory(key));
            while (true)
            {
                Debug.Assert(!(entryValue is Prime));

                // Actually change the Value 
                var prev = Interlocked.CompareExchange(ref table[idx].value, newValObj, entryValue);
                if (prev == entryValue)
                {
                    // CAS succeeded - we did the update!
                    // Adjust sizes
                    tableInfo._size.increment();
                    goto DONE;
                }
                // Else CAS failed

                // If a Prime'd value got installed, we need to re-run the put on the new table.
                if (prev is Prime)
                {
                    table = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: true);
                    goto TRY_WITH_NEW_TABLE;
                }

                // Otherwise we lost the CAS to another racing put.
                entryValue = prev;
                if (!EntryValueNullOrDead(entryValue))
                {
                    goto GOT_PREV_VALUE;
                }
            }

            GOT_PREV_VALUE:
            // PERF: this would be nice to have as a helper, 
            // but it does not get inlined
            if (default(TValue) == null && entryValue == NULLVALUE)
            {
                entryValue = null;
            }
            result = (TValue)entryValue;

            DONE:
            return result;
        }

        private bool CopySlot(Entry[] table, TKeyStore key, object putval, int fullHash)
        {
            Debug.Assert(putval != TOMBSTONE);
            Debug.Assert(key != null);

            TRY_WITH_NEW_TABLE:

            Debug.Assert(putval != null);
            Debug.Assert(!(putval is Prime));

            int lenMask = GetTableLength(table) - 1;
            var tableInfo = GetTableInfo(table);
            int idx = ReduceHashToIndex(fullHash, lenMask);

            // Spin till we get a slot for the key or force a resizing.
            int reprobe_cnt = 0;
            while (true)
            {
                var entryHash = table[idx].hash;
                if (entryHash == 0)
                {
                    // Slot is completely clean, claim the hash
                    Debug.Assert(fullHash != 0);
                    entryHash = Interlocked.CompareExchange(ref table[idx].hash, fullHash, 0);
                    if (entryHash == 0)
                    {
                        entryHash = fullHash;
                        if (entryHash == ZEROHASH)
                        {
                            // "added" entry for zero key
                            tableInfo._slots.increment();
                            break;
                        }
                    }
                }

                if (entryHash == fullHash)
                {
                    // hash is good, one way or another, claim the key
                    if (TryClaimSlotForCopy(ref table[idx].key, key, tableInfo._slots))
                    {
                        break;
                    }
                }

                // this slot contains a different key

                // here we know that this slot does not map to our key
                // and must reprobe or resize
                // hitting reprobe limit or finding TOMBPRIMEHASH here means that 
                // we will not find an appropriate slot in this table
                // but there could be more in the new one
                if (++reprobe_cnt >= ReprobeLimit(lenMask) |
                    entryHash == TOMBPRIMEHASH)
                {
                    var resized = tableInfo.Resize(this, table);
                    table = resized;
                    goto TRY_WITH_NEW_TABLE;
                }

                // quadratic reprobing
                idx = (idx + reprobe_cnt) & lenMask; // Reprobe!    

            } // End of spinning till we get a Key slot

            // Found the proper Key slot, now update the Value. 
            var entryValue = table[idx].value;
            if (entryValue != null)
            {
                // someone else copied, not us
                return false;
            }

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to not-null
            var newTable = tableInfo._newTable;
            if (newTable == null && tableInfo.TableIsCrowded(lenMask))
            {
                newTable = tableInfo.Resize(this, table);
                Debug.Assert(tableInfo._newTable != null);
            }

            // See if we are moving to a new table.
            // If so, copy our slot and retry in the new table.
            if (newTable != null)
            {
                var newTable1 = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: false);
                Debug.Assert(newTable == newTable1);
                table = newTable;
                goto TRY_WITH_NEW_TABLE;
            }

            // We are finally prepared to update the existing table
            Debug.Assert(entryValue == null);

            // if CAS succeeded - we did the update!
            // table-copy does not (effectively) increase the number of live k/v pairs
            // so no need to update size
            // otherwise someone else copied the value
            return Interlocked.CompareExchange(ref table[idx].value, putval, null) == null;
        }

        // Help along an existing resize operation.  This is just a fast cut-out
        // wrapper, to encourage inlining for the fast no-copy-in-progress case.  We
        // always help the top-most table copy, even if there are nested table
        // copies in progress.
        private void HelpCopy()
        {
            // Read the top-level table only once.  We'll try to help this copy along,
            // even if it gets promoted out from under us (i.e., the copy completes
            // and another table becomes the top-level copy).
            var topTable = this._table;
            var topTableInfo = GetTableInfo(topTable);
            if (topTableInfo._newTable != null)
            {
                topTableInfo.HelpCopyImpl(this, topTable, false);
            }
        }

        internal sealed override int Count
        {
            get
            {
                return this.GetTableInfo(this._table).Size;
            }
        }

        internal sealed override void Clear()
        {
            var newTable = CreateNew()._table;
            Volatile.Write(ref _table, newTable);
        }
    }
}
