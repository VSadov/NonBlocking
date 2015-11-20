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
    internal abstract partial class NonBlockingDictionary<TKey, TKeyStore, TValue>
        : NonBlockingDictionary<TKey, TValue>
    {
        public struct Entry
        {
            internal int hash;
            internal TKeyStore key;
            internal object value;
        }

        private Entry[] _topTable;
        private uint _lastResizeTickMillis;

        private const int REPROBE_LIMIT = 4;
        private const int REPROBE_LIMIT_SHIFT = 1;
        private const int MIN_SIZE = 8;

        // targeted time span between resizes.
        // if resizing more often than this, try expanding.
        const uint RESIZE_MILLIS_TARGET = (uint)1000;

        // create a fresh empty dictionary (used in Clear) 
        protected abstract NonBlockingDictionary<TKey, TKeyStore, TValue> CreateNew();

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

        internal NonBlockingDictionary() :
            this(MIN_SIZE)
        { }

        // TODO: VS need to make public entry point
        internal NonBlockingDictionary(int capacity)
        {
            if (capacity < 0)
            {
                throw new ArgumentOutOfRangeException(nameof(capacity));
            }

            capacity = AlignToPowerOfTwo(capacity);
            _topTable = new Entry[capacity + 1];
            _topTable[capacity].value = new TableInfo(new Counter());
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
        protected sealed override object tryGetValue(TKey key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            Entry[] table = this._topTable;
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
        protected sealed override bool putIfMatch(TKey key, object newVal, ref object oldVal, ValueMatch match)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }

            var table = this._topTable;
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
                tableInfo.tableIsCrowded(lenMask))
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

        public sealed override TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
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

            var table = this._topTable;
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
                tableInfo.tableIsCrowded(lenMask))
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

        private bool copySlot(Entry[] table, TKeyStore key, object putval, int fullHash)
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
            if (newTable == null && tableInfo.tableIsCrowded(lenMask))
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
            var topTable = this._topTable;
            var topTableInfo = GetTableInfo(topTable);
            if (topTableInfo._newTable != null)
            {
                topTableInfo.HelpCopyImpl(this, topTable, false);
            }
        }

        private sealed class TableInfo
        {
            //TODO: VS perhaps order fields to have gaps between ones changing 

            internal readonly Counter _size;
            internal readonly Counter _slots;

            internal Entry[] _newTable;

            // Sometimes many threads race to create a new very large table.  Only 1
            // wins the race, but the losers all allocate a junk large table with
            // hefty allocation costs.  Attempt to control the overkill here by
            // throttling attempts to create a new table.  I cannot really block here
            // (lest I lose the non-blocking property) but late-arriving threads can
            // give the initial resizing thread a little time to allocate the initial
            // new table.
            //
            // count of threads attempting an initial resize
            private int _resizers;

            // The next part of the table to copy.  It monotonically transits from zero
            // to table.length.  Visitors to the table can claim 'work chunks' by
            // CAS'ing this field up, then copying the indicated indices from the old
            // table to the new table.  Workers are not required to finish any chunk;
            // the counter simply wraps and work is copied duplicately until somebody
            // somewhere completes the count.
            private int _claimedChunk = 0;

            // Work-done reporting.  Used to efficiently signal when we can move to
            // the new table.  From 0 to length of old table refers to copying from the old
            // table to the new.
            private int _copyDone = 0;

            internal TableInfo(Counter size)
            {
                _size = size;
                _slots = new Counter();
            }

            internal int Size
            {
                get
                {
                    // counter does not lose counts, but reports of increments/decrements can be delayed
                    // it might be confusing if we ever report negative size.
                    var size = _size.get();
                    var negMask = ~(size >> 31);
                    return size & negMask;
                }
            }

            internal int EstimatedSlotsUsed
            {
                get
                {
                    return (int)_slots.estimate_get();
                }
            }

            internal bool tableIsCrowded(int len)
            {
                // 80% utilization, switch to a bigger table
                return EstimatedSlotsUsed > (len >> 2) * 3;
            }

            // Help along an existing resize operation.  We hope its the top-level
            // copy (it was when we started) but this table might have been promoted 
            // out of the top position.
            internal void HelpCopyImpl(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] oldTable, bool copy_all)
            {
                Debug.Assert(topmap.GetTableInfo(oldTable) == this);
                Entry[] newTable = this._newTable;
                Debug.Assert(newTable != null);    // Already checked by caller
                int oldlen = topmap.GetTableLength(oldTable); // Total amount to copy

#if DEBUG
                const int CHUNK_SIZE = 16;
#else
                const int CHUNK_SIZE = 1024;
#endif
                int MIN_COPY_WORK = Math.Min(oldlen, CHUNK_SIZE); // Limit per-thread work

                bool panic = false;
                int claimedChunk = -1;
                while (this._copyDone < oldlen)
                {
                    // Still needing to copy?
                    // Carve out a chunk of work.
                    if (!panic)
                    {
                        claimedChunk = this._claimedChunk;

                        for (;;)
                        {
                            // panic check
                            // We "panic" if we have tried TWICE to copy every slot - and it still
                            // has not happened.  i.e., twice some thread somewhere claimed they
                            // would copy 'slot X' (by bumping _copyIdx) but they never claimed to
                            // have finished (by bumping _copyDone).  Our choices become limited:
                            // we can wait for the work-claimers to finish (and become a blocking
                            // algorithm) or do the copy work ourselves.  Tiny tables with huge
                            // thread counts trying to copy the table often 'panic'. 
                            if (claimedChunk > (oldlen / (CHUNK_SIZE / 2)))
                            {
                                panic = true;
                                break;
                            }

                            var alreadyClaimed = Interlocked.CompareExchange(ref this._claimedChunk, claimedChunk + 1, claimedChunk);
                            if (alreadyClaimed == claimedChunk)
                            {
                                break;
                            }

                            claimedChunk = alreadyClaimed;
                        }
                    }
                    else
                    {
                        // we went through the whole table in panic mode
                        // there cannot be possibly anything left to copy.
                        if (claimedChunk > ((oldlen / (CHUNK_SIZE / 2)) + oldlen / CHUNK_SIZE))
                        {
                            _copyDone = oldlen;
                            Promote(topmap, oldTable);
                            return;
                        }

                        claimedChunk++;
                    }

                    // We now know what to copy.  Try to copy.
                    int workdone = 0;
                    int copyStart = claimedChunk * CHUNK_SIZE;
                    for (int i = 0; i < MIN_COPY_WORK; i++)
                    {
                        if (this._copyDone >= oldlen)
                        {
                            Promote(topmap, oldTable);
                            return;
                        }

                        if (CopySlot(topmap, (copyStart + i) & (oldlen - 1), oldTable, newTable))
                        {
                            workdone++;
                        }
                    }

                    if (workdone > 0)
                    {
                        // See if we can promote
                        var copyDone = Interlocked.Add(ref this._copyDone, workdone);

                        // Check for copy being ALL done, and promote.  
                        if (copyDone >= oldlen)
                        {
                            Promote(topmap, oldTable);
                        }
                    }

                    if (!copy_all && !panic)
                    {
                        return;
                    }
                }

                // Extra promotion check, in case another thread finished all copying
                // then got stalled before promoting.
                Promote(topmap, oldTable);
            }

            private void Promote(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] oldTable)
            {
                // Looking at the top-level table?
                // Note that we might have
                // nested in-progress copies and manage to finish a nested copy before
                // finishing the top-level copy.  We only promote top-level copies.
                if (topmap._topTable == oldTable)
                {
                    // Attempt to promote
                    if (Interlocked.CompareExchange(ref topmap._topTable, this._newTable, oldTable) == oldTable)
                    {
                        // System.Console.WriteLine("size: " + _newTable.Length);
                        topmap._lastResizeTickMillis = CurrentTickMillis();
                    }
                }
            }

            // Copy slot 'idx' from the old table to the new table.  If this thread
            // confirmed the copy, update the counters and check for promotion.
            //
            // Returns the result of reading the new table, mostly as a
            // convenience to callers.  We come here with 1-shot copy requests
            // typically because the caller has found a Prime, and has not yet read
            // the new table - which must have changed from null-to-not-null
            // before any Prime appears.  So the caller needs to read the new table
            // field to retry his operation in the new table, but probably has not
            // read it yet.
            internal Entry[] CopySlotAndCheck(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] oldTable, int idx, bool shouldHelp)
            {
                Debug.Assert(topmap.GetTableInfo(oldTable) == this);
                var newTable = this._newTable;

                // We're only here because the caller saw a Prime, which implies a
                // table-copy is in progress.
                Debug.Assert(newTable != null);

                if (CopySlot(topmap, idx, oldTable, newTable))
                {
                    // Record the slot copied
                    var copyDone = Interlocked.Increment(ref this._copyDone);

                    // Check for copy being ALL done, and promote.  
                    if (copyDone >= topmap.GetTableLength(oldTable))
                    {
                        Promote(topmap, oldTable);
                    }
                }

                // Generically help along any copy (except if called recursively from a helper)
                if (shouldHelp)
                {
                    topmap.HelpCopy();
                }

                return newTable;
            }

            // Copy one K/V pair from old table to new table. 
            // Returns true if we actually did the copy.
            // Regardless, once this returns, the copy is available in the new table and 
            // slot in the old table is no longer usable.
            private bool CopySlot(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, int idx, Entry[] oldTable, Entry[] newTable)
            {
                // Blindly set the hash from 0 to TOMBPRIMEHASH, to eagerly stop
                // fresh put's from claiming new slots in the old table when the old
                // table is mid-resize.
                var hash = oldTable[idx].hash;
                if (hash == 0)
                {
                    if (Interlocked.CompareExchange(ref oldTable[idx].hash, TOMBPRIMEHASH, 0) == 0)
                    {
                        // slot was not claimed
                        return false;
                    }
                }

                // Prevent new values from appearing in the old table.
                // Box what we see in the old table, to prevent further updates.
                // NOTE: Read of the value below must happen before reading of the key, 
                // however this read does not need to be volatile since we will have 
                // some fences in between reads.
                object oldval = oldTable[idx].value;

                // already boxed?
                Prime box = oldval as Prime;
                if (box != null)
                {
                    // volatile read here since we need to make sure 
                    // that the key read below happens after we have read oldval above
                    Volatile.Read(ref box.originalValue);
                }
                else
                {
                    do
                    {
                        box = EntryValueNullOrDead(oldval) ?
                            TOMBPRIME :
                            new Prime(oldval);

                        // CAS down a box'd version of oldval
                        // also works as a complete fence between reading the value and the key
                        object prev = Interlocked.CompareExchange(ref oldTable[idx].value, box, oldval);

                        if (prev == oldval)
                        {
                            // If we made the Value slot hold a TOMBPRIME, then we both
                            // prevented further updates here but also the (absent)
                            // oldval is vacuously available in the new table.  We
                            // return with true here: any thread looking for a value for
                            // this key can correctly go straight to the new table and
                            // skip looking in the old table.
                            if (box == TOMBPRIME)
                            {
                                return true;
                            }

                            // Break loop; oldval is now boxed by us
                            // it still needs to be copied into the new table.
                            break;
                        }

                        oldval = prev;
                        box = oldval as Prime;
                    }
                    while (box == null);
                }

                if (box == TOMBPRIME)
                {
                    // Copy already complete here!
                    return false;
                }

                // Copy the value into the new table, but only if we overwrite a null.
                // If another value is already in the new table, then somebody else
                // wrote something there and that write is happens-after any value that
                // appears in the old table.  If putIfMatch does not find a null in the
                // new table - somebody else should have recorded the null-not_null
                // transition in this copy.
                object originalValue = box.originalValue;
                Debug.Assert(originalValue != TOMBSTONE);

                // since we have a real value, there must be a nontrivial key in the table
                // regular read is ok because because value is always CASed down after the key
                // and we ensured that we read the key after the value with fences above
                var key = oldTable[idx].key;
                bool copiedIntoNew = topmap.copySlot(newTable, key, originalValue, hash);

                // Finally, now that any old value is exposed in the new table, we can
                // forever hide the old-table value by gently inserting TOMBPRIME value.  
                // This will stop other threads from uselessly attempting to copy this slot
                // (i.e., it's a speed optimization not a correctness issue).
                // Check if we are not too late though, to not pay for MESI RFO and 
                // GC fence needlessly.
                if (oldTable[idx].value != TOMBPRIME)
                {
                    oldTable[idx].value = TOMBPRIME;
                }

                return copiedIntoNew;
            }

            // Resizing after too many probes.  "How Big???" heuristics are here.
            // Callers will (not this routine) help any in-progress copy.
            // Since this routine has a fast cutout for copy-already-started, callers
            // MUST 'help_copy' lest we have a path which forever runs through
            // 'resize' only to discover a copy-in-progress which never progresses.
            internal Entry[] Resize(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] table)
            {
                Debug.Assert(topmap.GetTableInfo(table) == this);

                // Check for resize already in progress, probably triggered by another thread
                // all reads of this_newTable here are not volatile
                // we are just opportunistically checking if a new table has arrived.
                var newTable = this._newTable;

                // See if resize is already in progress
                if (newTable != null)
                {
                    // Use the new table already
                    return newTable;
                }

                // No copy in-progress, so start one.  
                //First up: compute new table size.
                int oldlen = topmap.GetTableLength(table);

                const int MAX_SIZE = 1 << 30;
                const int MAX_CHURN_SIZE = 1 << 15;

                // First size estimate is roughly inverse of ProbeLimit
                int sz = Size + (MIN_SIZE >> REPROBE_LIMIT_SHIFT);
                int newsz = sz < (MAX_SIZE >> REPROBE_LIMIT_SHIFT) ?
                                                sz << REPROBE_LIMIT_SHIFT :
                                                sz;

                // if new table would shrink or hold steady, 
                // we must be resizing because of churn.
                // target churn based resize rate to be about 1 per RESIZE_TICKS_TARGET
                if (newsz <= oldlen)
                {
                    var resizeSpan = CurrentTickMillis() - topmap._lastResizeTickMillis;

                    // note that CurrentTicks() will wrap around every 50 days.
                    // For our purposes that is tolerable since it just 
                    // adds a possibility that in some rare cases a churning resize will not be 
                    // considered a churning one.
                    if (resizeSpan < RESIZE_MILLIS_TARGET)
                    {
                        // last resize too recent, expand
                        newsz = oldlen < MAX_CHURN_SIZE ? oldlen << 1 : oldlen;
                    }
                    else
                    {
                        // do not allow shrink too fast
                        newsz = Math.Max(newsz, (int)((long)oldlen * RESIZE_MILLIS_TARGET / resizeSpan));
                    }
                }

                // Align up to a power of 2
                newsz = AlignToPowerOfTwo(newsz);

                // Size calculation: 2 words (K+V) per table entry, plus a handful.  We
                // guess at 32-bit pointers; 64-bit pointers screws up the size calc by
                // 2x but does not screw up the heuristic very much.
                //
                // TODO: VS some tuning may be needed
                int kBs4 = (((newsz << 1) + 4) << 3/*word to bytes*/) >> 12/*kBs4*/;

                // Now, if allocation is big enough,
                // limit the number of threads actually allocating memory to a
                // handful - lest we have 750 threads all trying to allocate a giant
                // resized array.
                // conveniently, Increment is also a full fence
                if (kBs4 > 0 && Interlocked.Increment(ref _resizers) >= 2)
                {
                    // Already 2 guys trying; wait and see
                    newTable = this._newTable;
                    // See if resize is already in progress
                    if (newTable != null)
                    {
                        return newTable;         // Use the new table already
                    }

                    SpinWait.SpinUntil(() => this._newTable != null, 8 * kBs4);
                }

                // Last check, since the 'new' below is expensive and there is a chance
                // that another thread slipped in a new table while we ran the heuristic.
                newTable = this._newTable;
                // See if resize is already in progress
                if (newTable != null)
                {
                    return newTable;          // Use the new table already
                }

                // add 1 for table info
                newTable = new Entry[newsz + 1];
                newTable[newTable.Length - 1].value = new TableInfo(_size);

                // The new table must be CAS'd in to ensure only 1 winner
                var prev = this._newTable ??
                            Interlocked.CompareExchange(ref this._newTable, newTable, null);

                if (prev != null)
                {
                    return prev;
                }
                else
                {
                    //Console.WriteLine("resized: " + newsz);
                    return newTable;
                }
            }
        }
    }
}
