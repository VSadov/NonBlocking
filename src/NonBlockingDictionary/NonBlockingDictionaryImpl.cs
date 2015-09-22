// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.


//
// Heavily based on Cliff Click's Non-Blocking HashTable (public domain)
//

using System;
using System.Diagnostics;
using System.Threading;

namespace NonBlocking
{
    internal abstract class NonBlockingDictionary<TKey, TKeyStore, TValue>
        : NonBlockingDictionary<TKey, TValue>
    {
        public struct Entry
        {
            internal int hash;
            internal TKeyStore key;
            internal object value;
        }

        private Entry[] _topTable;
        private long _lastResizeMilli;

        private const int REPROBE_LIMIT = 4;
        private const int MIN_SIZE = 8;

        // represents forcefully dead entry 
        // we insert it in old table during rehashing
        // to reduce chances that more entries are added
        private const int TOMBPRIMEHASH = 1 << 31;

        // we cannot distigush zero keys from uninitialized state
        // so we force them to have this special hash instead
        internal const int ZEROHASH = 1 << 30;

        // all regular hashes have these bits set
        // to be different from 0, TOMBPRIMEHASH or ZEROHASH
        internal const int REGULAR_HASH_BITS = TOMBPRIMEHASH | ZEROHASH;

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
            // 1/4 of table with some extra
            return REPROBE_LIMIT + (lenMask >> 2);
        }

        internal NonBlockingDictionary() :
            this(MIN_SIZE)
        { }

        // TODO: need to make public entry point
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

        protected abstract NonBlockingDictionary<TKey, TKeyStore, TValue> CreateNew();

        public sealed override void Clear()
        {
            var newTable = CreateNew()._topTable;
            Volatile.Write(ref _topTable, newTable);
        }

        internal static long CurrentTimeMillis()
        {
            return DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
        }

        internal static int AlignToPowerOfTwo(int size)
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

        internal virtual int hash(TKey key)
        {
            unchecked
            {
                uint origHash = (uint)keyComparer.GetHashCode(key);

                // being an open addressed, this hashtable has some sensitivity
                // to clustering behavior of the provided hash, so in theory
                // it makes sense to mix the hash bits.
                //
                // In practice, however, this may make hash worse,
                // for example, by turning a perfect hash into nonperfect.
                //
                // Overal this is a mitigation of worst case clastering 
                // scenario at the cost of the average scenario.
                // I am not convinced that it is worth it.
                // In particular, since user can add mixing in the comparer.
                //
                // I will disable this pending more data.

#if MIX_HASH
                // variant of FNV-1a hash.
                const uint FnvOffsetBias = 2166136261;
                const uint FnvPrime = 16777619;

                uint hashCode = FnvOffsetBias;

                hashCode = (hashCode ^ (origHash & 0xFF)) * FnvPrime;
                hashCode = (hashCode ^ ((origHash >> 8) & 0xFF)) * FnvPrime;
                hashCode = (hashCode ^ ((origHash >> 16) & 0xFF)) * FnvPrime;
                hashCode = (hashCode ^ ((origHash >> 24))) * FnvPrime;

                // ensure that hash never matches 0, TOMBPRIMEHASH or ZEROHASH
                return (int)hashCode | REGULAR_HASH_BITS;
#else
                // ensure that hash never matches 0, TOMBPRIMEHASH or ZEROHASH
                return (int)(origHash | REGULAR_HASH_BITS);
#endif
            }
        }

        protected abstract bool keyEqual(TKey key, TKeyStore entryKey);

        protected sealed override bool tryGetValue(TKey key, out object value)
        {
            Entry[] table = this._topTable;
            int fullHash = this.hash(key);

            tailCall:

            var lenMask = GetTableLength(table) - 1;
            int idx = fullHash & lenMask;

            // Main spin/reprobe loop
            int reprobeCnt = 0;
            while (true)
            {
                // hash, key and value are all CAS-ed down and follow a specific sequence of states.
                // hence the order of these reads is irrelevant and they do not need to be volatile    
                var entryHash = table[idx].hash;
                var entryKey = table[idx].key;

                if (entryHash == 0)
                {
                    // the slot has not been claimed - a clear miss
                    break;
                }

                // is this our slot?
                if (fullHash == entryHash &&
                    key != null &&
                    keyEqual(key, entryKey))
                {
                    var entryValue = table[idx].value;

                    if (entryValue == null | entryValue == TOMBSTONE)
                    {
                        break;
                    }

                    if (!(entryValue is Prime))
                    {
                        value = entryValue == NULLVALUE? 
                            null : 
                            entryValue;

                        return true;
                    }

                    // found a prime, that means copying has started 
                    // and all new values go to the new table
                    // help with copying and retry in the new table
                    var tableInfo = GetTableInfo(table);
                    table = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: true);

                    // return this.TryGet(newTable, entryKey, hash, out value); 
                    goto tailCall;
                }

                // get and put must have the same key lookup logic.
                // But only 'put'needs to force a table-resize for a too-long key-reprobe sequence
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
                        goto tailCall;
                    }

                    // no new table, so this is a miss
                    break;
                }

                // quadratic reprobe
                idx = (idx + reprobeCnt) & lenMask;
            }

            value = null;
            return false;
        }

        internal abstract bool TryClaimSlotForPut(ref TKeyStore entryKey, TKey key, Counter slots);
        internal abstract bool TryClaimSlotForCopy(ref TKeyStore entryKey, TKeyStore key, Counter slots);

        // 1) finds or creates a slot for the key
        // 2) sets the slot value to the putval if original value meets expVal condition
        // 3) returns true if the value was actually changed 
        // Note that pre-existence of the slot is irrelevant 
        // since slot without a value is as good as no slot at all
        protected sealed override bool putIfMatch(TKey key, object newVal, out object oldVal, ValueMatch match)
        {
            if (newVal == null)
            {
                newVal = NULLVALUE;
            }

            var table = this._topTable;
            int fullhash = hash(key);

            TRY_WITH_NEW_TABLE:

            Debug.Assert(newVal != null);
            Debug.Assert(!(newVal is Prime));

            int lenMask = GetTableLength(table) - 1;
            var tableInfo = GetTableInfo(table);
            int idx = fullhash & lenMask;

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
                        Debug.Assert(match == ValueMatch.NotNullOrDead);
                        goto FAILED;
                    }
                    else
                    {
                        // Slot is completely clean, claim the hash first
                        Debug.Assert(fullhash != 0);
                        entryHash = Interlocked.CompareExchange(ref table[idx].hash, fullhash, 0);
                        if (entryHash == 0)
                        {
                            entryHash = fullhash;
                            if (entryHash == ZEROHASH)
                            {
                                // "added" entry for zero key
                                tableInfo._slots.increment();
                                break;
                            }
                        }
                    }
                }

                if (entryHash == fullhash)
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
            //
            // TODO: VS would it be better to volatile read here so that we do not need the isPrime check below?
            //       "is Prime" may cause cache miss since we otherwise do not need to touch entryValue and will always be false on x86/64.
            //
            //       basically, the volatile read is only needed on ARM and the like and that is also where it could be expensive
            //       is DMB more costly than "is Prime"?
            var entryValue = table[idx].value;
            if (newVal == entryValue)
            {
                //the exact same value is already there
                goto FAILED;
            }

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).
            // Or we found a Prime, but the VM allowed reordering such that we
            // did not spot the new table (very rare race here: the writing
            // thread did a CAS of new table then a CAS store of a Prime.  This thread
            // does regular read of the Prime, then volatile read of new table - 
            // but the read of Prime was so delayed (or the read of new table was 
            // so accelerated) that they swapped and we still read a null new table.  
            // The resize call below will do a CAS on new table forcing the read.
            var newTable = tableInfo._newTable;
            if (newTable == null &&       
                ((entryValue == null && tableInfo.tableIsCrowded(lenMask)) || entryValue is Prime))
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
                var entryValueNullOrDead = entryValue == null | entryValue == TOMBSTONE;

                switch (match)
                {
                    case ValueMatch.Any:
                        break;

                    case ValueMatch.NullOrDead:
                        if (entryValueNullOrDead)
                        {
                            break;
                        }

                        goto FAILED;

                    case ValueMatch.NotNullOrDead:
                        if (entryValueNullOrDead)
                        {
                            goto FAILED;
                        }
                        break;
                }

                if (newVal == entryValue)
                {
                    // Do not update!
                    goto FAILED;
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
            oldVal = null;
            return false;
        }

        private bool copySlot(Entry[] table, TKeyStore key, object putval, int fullhash)
        {
            Debug.Assert(putval != TOMBSTONE);

            TRY_WITH_NEW_TABE:

            Debug.Assert(putval != null);
            Debug.Assert(!(putval is Prime));

            int lenMask = GetTableLength(table) - 1;
            var tableInfo = GetTableInfo(table);
            int idx = fullhash & lenMask;

            // Spin till we get a slot for the key or force a resizing.
            int reprobe_cnt = 0;
            while (true)
            {
                var entryHash = table[idx].hash;
                if (entryHash == 0)
                {
                    // Slot is completely clean, claim the hash
                    Debug.Assert(fullhash != 0);
                    entryHash = Interlocked.CompareExchange(ref table[idx].hash, fullhash, 0);
                    if (entryHash == 0)
                    {
                        entryHash = fullhash;
                        if (entryHash == ZEROHASH)
                        {
                            // "added" entry for zero key
                            tableInfo._slots.increment();
                            break;
                        }
                    }
                }

                if (entryHash == fullhash)
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
                    goto TRY_WITH_NEW_TABE;
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
                goto TRY_WITH_NEW_TABE;
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
            internal readonly Counter _size;
            internal readonly Counter _slots;

            volatile internal Entry[] _newTable;

            // Sometimes many threads race to create a new very large table.  Only 1
            // wins the race, but the losers all allocate a junk large table with
            // hefty allocation costs.  Attempt to control the overkill here by
            // throttling attempts to create a new table.  I cannot really block here
            // (lest I lose the non-blocking property) but late-arriving threads can
            // give the initial resizing thread a little time to allocate the initial
            // new table.
            //
            // count of threads attempting an initial resize
            volatile int _resizers;

            // The next part of the table to copy.  It monotonically transits from zero
            // to table.length.  Visitors to the table can claim 'work chunks' by
            // CAS'ing this field up, then copying the indicated indices from the old
            // table to the new table.  Workers are not required to finish any chunk;
            // the counter simply wraps and work is copied duplicately until somebody
            // somewhere completes the count.
            volatile int _copyIdx = 0;

            // Work-done reporting.  Used to efficiently signal when we can move to
            // the new table.  From 0 to length of old table refers to copying from the old
            // table to the new.
            volatile int _copyDone = 0;

            public TableInfo(Counter size)
            {
                _size = size;
                _slots = new Counter();
            }

            public int size() { return (int)_size.get(); }
            public int slots() { return (int)_slots.get(); }

            public bool tableIsCrowded(int len)
            {
                // 80% utilization, switch to a bigger table
                return _slots.estimate_get() > (len >> 2) * 3;
            }

            // Help along an existing resize operation.  We hope its the top-level
            // copy (it was when we started) but this table might have been promoted 
            // out of the top position.
            public void HelpCopyImpl(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] oldTable, bool copy_all)
            {
                Debug.Assert(topmap.GetTableInfo(oldTable) == this);
                Entry[] newTable = this._newTable;
                Debug.Assert(newTable != null);    // Already checked by caller
                int oldlen = topmap.GetTableLength(oldTable); // Total amount to copy

#if DEBUG
                int MIN_COPY_WORK = Math.Min(oldlen, 17); // Limit per-thread work
#else
                int MIN_COPY_WORK = Math.Min(oldlen, 1024); // Limit per-thread work
#endif

                int panic_start = -1;
                int copyidx = -1;
                while (_copyDone < oldlen)
                {
                    // Still needing to copy?
                    // Carve out a chunk of work.  The counter wraps around so every
                    // thread eventually tries to copy every slot repeatedly.

                    // We "panic" if we have tried TWICE to copy every slot - and it still
                    // has not happened.  i.e., twice some thread somewhere claimed they
                    // would copy 'slot X' (by bumping _copyIdx) but they never claimed to
                    // have finished (by bumping _copyDone).  Our choices become limited:
                    // we can wait for the work-claimers to finish (and become a blocking
                    // algorithm) or do the copy work ourselves.  Tiny tables with huge
                    // thread counts trying to copy the table often 'panic'. 
                    if (panic_start == -1)
                    { // No panic?
                        copyidx = (int)_copyIdx;
                        while (copyidx < (oldlen << 1) && // 'panic' check
                               !(Interlocked.CompareExchange(ref _copyIdx, copyidx + MIN_COPY_WORK, copyidx) == copyidx))
                        {
                            // Re-read
                            copyidx = (int)_copyIdx;
                        }

                        if (!(copyidx < (oldlen << 1)))  // Panic!
                        {
                            // Record where we started to panic-copy
                            panic_start = copyidx;
                        }
                    }

                    // We now know what to copy.  Try to copy.
                    int workdone = 0;
                    for (int i = 0; i < MIN_COPY_WORK; i++)
                    {
                        // Made an oldtable slot go dead?
                        if (CopySlot(topmap, (copyidx + i) & (oldlen - 1), oldTable, newTable))
                        {
                            workdone++;
                        }
                    }

                    if (workdone > 0)
                    {
                        // See if we can promote
                        CopyCheckAndPromote(topmap, oldTable, workdone);
                    }

                    copyidx += MIN_COPY_WORK;

                    // Uncomment these next 2 lines to turn on incremental table-copy.
                    // Otherwise this thread continues to copy until it is all done.
                    if (!copy_all && panic_start == -1) // No panic?
                    {
                        // Then done copying after doing MIN_COPY_WORK
                        return;
                    }
                }

                // Extra promotion check, in case another thread finished all copying
                // then got stalled before promoting.
                CopyCheckAndPromote(topmap, oldTable, 0);
            }

            private void CopyCheckAndPromote(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] oldTable, int workdone)
            {
                Debug.Assert(topmap.GetTableInfo(oldTable) == this);
                int oldlen = topmap.GetTableLength(oldTable);
                // We made a slot unusable and so did some of the needed copy work
                int copyDone = _copyDone;
                Debug.Assert((copyDone + workdone) <= oldlen);
                if (workdone > 0)
                {
                    while (Interlocked.CompareExchange(ref _copyDone, copyDone + workdone, copyDone) != copyDone)
                    {
                        copyDone = _copyDone; // Reload, retry
                        Debug.Assert((copyDone + workdone) <= oldlen);
                    }
                }

                // Check for copy being ALL done, and promote.  Note that we might have
                // nested in-progress copies and manage to finish a nested copy before
                // finishing the top-level copy.  We only promote top-level copies.
                if (copyDone + workdone == oldlen && // Ready to promote this table?
                    topmap._topTable == oldTable) // Looking at the top-level table?                    
                {
                    // Attempt to promote
                    if (Interlocked.CompareExchange(ref topmap._topTable, _newTable, oldTable) == oldTable)
                    {
                        topmap._lastResizeMilli = CurrentTimeMillis();
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
            public Entry[] CopySlotAndCheck(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] oldTable, int idx, bool shouldHelp)
            {
                Debug.Assert(topmap.GetTableInfo(oldTable) == this);

                // VOLATILE READ
                var newTable = _newTable;

                // We're only here because the caller saw a Prime, which implies a
                // table-copy is in progress.
                Debug.Assert(newTable != null);

                if (CopySlot(topmap, idx, oldTable, newTable))
                {
                    // record 1 copy
                    CopyCheckAndPromote(topmap, oldTable, 1);
                }

                // Record the slot copied
                // Generically help along any copy (except if called recursively from a helper)
                if (shouldHelp)
                {
                    topmap.HelpCopy();
                }

                return newTable;
            }

            // Copy one K/V pair from old table to new table.  Returns true if we can
            // confirm that the new table guaranteed has a value for this old-table
            // slot.  We need an accurate confirmed-copy count so that we know when we
            // can promote (if we promote the new table too soon, other threads may
            // 'miss' on values not-yet-copied from the old table).  We don't allow
            // any direct updates on the new table, unless they first happened to the
            // old table - so that any transition in the new table from null to
            // not-null must have been from a copy_slot (or other old-table overwrite)
            // and not from a thread directly writing in the new table.  Thus we can
            // count null-to-not-null transitions in the new table.
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
                object oldval = oldTable[idx].value; // Read OLD table
                while (!(oldval is Prime))
                {
                    Prime box = (oldval == null | oldval == TOMBSTONE) ?
                        TOMBPRIME :
                        new Prime(oldval);

                    // CAS down a box'd version of oldval
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

                        // Otherwise we boxed something, but it still needs to be
                        // copied into the new table.
                        oldval = box;         // Record updated oldval
                        break;                // Break loop; oldval is now boxed by us
                    }
                    oldval = prev; // Else try, try again
                }

                if (oldval == TOMBPRIME)
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
                object originalValue = ((Prime)oldval).originalValue;
                Debug.Assert(originalValue != TOMBSTONE);
                // since we have a real value, there must be a nontrivial key in the table
                // non-volatile read because the CAS of a boxed value above is a complete fence
                var key = oldTable[idx].key;

                bool copiedIntoNew = topmap.copySlot(newTable, key, originalValue, hash);

                // Finally, now that any old value is exposed in the new table, we can
                // forever hide the old-table value by slapping a TOMBPRIME down.  This
                // will stop other threads from uselessly attempting to copy this slot
                // (i.e., it's a speed optimization not a correctness issue).
                oldTable[idx].value = TOMBPRIME;

                //TODO: (vsadov) what if current thread croaks with Thread.Abort here or delayed forever?
                return copiedIntoNew;
            }

            // Resizing after too many probes.  "How Big???" heuristics are here.
            // Callers will (not this routine) will 'help_copy' any in-progress copy.
            // Since this routine has a fast cutout for copy-already-started, callers
            // MUST 'help_copy' lest we have a path which forever runs through
            // 'resize' only to discover a copy-in-progress which never progresses.
            public Entry[] Resize(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] table)
            {
                Debug.Assert(topmap.GetTableInfo(table) == this);

                // Check for resize already in progress, probably triggered by another thread
                // VOLATILE READ
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
                int sz = size();
                const int MAX_SIZE = 1 << 30;

                // First size estimate is 2x of size
                int newsz = Math.Max(sz < MAX_SIZE? sz << 1 : sz  , MIN_SIZE);

                // if new table would shrink or hold steady, 
                // we must be resizing because of churn.
                // target churn based resize rate to be about 1 per second
                if (newsz <= oldlen)
                {
                    var resizeSpan = CurrentTimeMillis() - topmap._lastResizeMilli;
                    if (resizeSpan < 1000)
                    {
                        // last resize too recent, expand
                        newsz = oldlen < MAX_SIZE ? oldlen << 1 : oldlen;
                    }
                    else
                    {
                        // do not allow shrink too fast
                        newsz = Math.Max(newsz, (int)(oldlen * 1000 / resizeSpan));
                    }
                }

                // Align up to a power of 2
                newsz = AlignToPowerOfTwo(newsz);
                                
                // Now limit the number of threads actually allocating memory to a
                // handful - lest we have 750 threads all trying to allocate a giant
                // resized array.
                int r = Interlocked.Increment(ref _resizers);

                // Size calculation: 2 words (K+V) per table entry, plus a handful.  We
                // guess at 32-bit pointers; 64-bit pointers screws up the size calc by
                // 2x but does not screw up the heuristic very much.
                // TODO: some tuning may be needed
                int kBs4 = (((newsz << 1) + 4) << 3/*word to bytes*/) >> 12/*kBs4*/;
                if (r >= 2 && kBs4 > 0)
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
                // that another thread slipped in a new thread while we ran the heuristic.
                newTable = this._newTable;
                // See if resize is already in progress
                if (newTable != null)
                {
                    return newTable;          // Use the new table already
                }

                // add 1 for table info
                // This can get expensive for big arrays
                newTable = new Entry[newsz + 1];
                newTable[newTable.Length - 1].value = new TableInfo(_size);

#if DEBUG
                // System.Console.WriteLine(newsz);
#endif

                // Another check after the slow allocation
                var curNewTable = this._newTable;
                // See if resize is already in progress
                if (curNewTable != null)
                {
                    // Use the new table already
                    return curNewTable;
                }

                // The new table must be CAS'd in to ensure only 1 winner
                //return Interlocked.CompareExchange(ref this._newTable, newTable, null) ?? newTable;

                var prev = Interlocked.CompareExchange(ref this._newTable, newTable, null);

                if (prev != null)
                {
                    return prev;
                }
                else
                {
                    // Console.WriteLine("resized: " + newsz);
                    return newTable;
                }
            }
        }
    }
}
