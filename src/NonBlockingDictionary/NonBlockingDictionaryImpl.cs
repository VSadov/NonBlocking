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
            return table[table.Length - 1].value as TableInfo;
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
        
        protected sealed override bool putIfMatch(TKey key, object newVal, ValueMatch match)
        {
            // TODO: take out to callers
            if (newVal == null)
            {
                throw new ArgumentNullException();
            }

            // TODO: merge in
            return this.putIfMatch(this._topTable, key, newVal, match);
        }

        protected abstract NonBlockingDictionary<TKey, TKeyStore, TValue> CreateNew();

        public sealed override void Clear()
        {
            // Smack a new empty table down
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
                // to clastering behavior of the provided hash, so in theory
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

        protected sealed override bool TryGet(TKey key, out object value)
        {
            Entry[] table = this._topTable;
            int fullHash = this.hash(key);

        tailCall: 

            var lenMask = GetTableLength(table) - 1;
            int idx = fullHash & lenMask;

            // Main spin/reprobe loop, looking for a Key hit
            int reprobeCnt = 0;
            while (true)
            {
                // TODO: volatile ?
                //       note that hash, key and value are all CAS-ed down
                //       and follow a specific sequence of states.
                var entryHash = table[idx].hash;
                var entryKey = table[idx].key;

                if (entryHash == 0)
                {
                    // the slot has not been claimed - a clear miss
                    break;
                }

                // Key-compare
                if (fullHash == entryHash && 
                    key != null &&
                    keyEqual(key, entryKey))
                {
                    var entryValue = table[idx].value;
                    if (!(entryValue is Prime))
                    {
                        if (entryValue == null | entryValue == TOMBSTONE)
                        {
                            break;
                        }

                        value = entryValue;
                        return true;
                    }

                    // copying started
                    // all new values go to the new table
                    // Help with copying and retry in the new table
                    var tableInfo = GetTableInfo(table);
                    table = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: true);

                    // return this.TryGet(newTable, entryKey, hash, out value); 
                    goto tailCall;
                }

                // get and put must have the same key lookup logic!  But only 'put'
                // needs to force a table-resize for a too-long key-reprobe sequence.
                // hitting reprobe limit or finding TOMBPRIMEHASH mean there are no
                // more keys in this bucket, but there could be more in the new table
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

        protected enum KeyClaimResult
        {
            Failed,
            GotExisting,
            ClaimedNew
        }

        internal abstract bool TryClaimSlotForPut(ref TKeyStore entryKey, TKey key, Counter slots);
        internal abstract bool TryClaimSlotForCopy(ref TKeyStore entryKey, TKeyStore key, Counter slots);
        
        // 1) finds or creates a slot for the key
        // 2) sets the slot value to the putval if original value meets expVal condition
        // 3) returns true if the value was actually changed 
        // Note that pre-existence of the slot is irrelevant 
        // since slot without a value is as good as no slot at all
        private bool putIfMatch(Entry[] table, TKey key, object putval, ValueMatch expVal)
        {
            int fullhash = hash(key);

          tailCall:

            Debug.Assert(putval != null);
            Debug.Assert(!(putval is Prime));

            int lenMask = GetTableLength(table) - 1;
            var tableInfo = GetTableInfo(table); 
            int idx = fullhash & lenMask;

            object entryValue;
            int reprobe_cnt = 0;

            // Spin till we get a Key slot or force a resizing.
            while (true)
            {
                var entry = table[idx];
                var entryHash = entry.hash;
                var entryKey = entry.key;
                entryValue = entry.value;

                if (entryHash == 0)
                {
                    // Found an unassigned slot - which means this 
                    // Key has never been in this table.
                    if (putval == TOMBSTONE)
                    {
                        Debug.Assert(expVal == ValueMatch.NotNullOrDead);
                        return false;
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
                    // hash is good, one way or another 
                    // try claiming the slot for the key
                    if (TryClaimSlotForPut(ref table[idx].key, key, tableInfo._slots))
                    {
                        break;
                    }
                }

                // here we know that this slot does not map to our key
                // and must reprobe or resize

                // get and put must have the same key lookup logic!
                if (++reprobe_cnt >= ReprobeLimit(lenMask) | 
                    entryHash == TOMBPRIMEHASH)
                {
                    table = tableInfo.Resize(this, table);

                    // help along an existing copy
                    this.HelpCopy();

                    // return this.putIfMatch(resized, key, putval, expVal);
                    goto tailCall;
                }

                // quadratic reprobing
                idx = (idx + reprobe_cnt) & lenMask;
            }

            // Found the proper Key slot, now update the matching Value slot.  We
            // never put a null, so Value slots monotonically move from null to
            // not-null (deleted Values use Tombstone).
            if (putval == entryValue)
            {
                //the exact same value is already there
                return false;
            }

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).  Of course we got a 'free' check
            // of new table once per key-compare (not really free, but paid-for by the
            // time we get here).
            var newTable = tableInfo._newTable;
            if (newTable == null &&       // New table-copy already spotted?
                ((entryValue == null && tableInfo.tableFull(reprobe_cnt, lenMask + 1)) ||
                 // Or we found a Prime, but the VM allowed reordering such that we
                 // did not spot the new table (very rare race here: the writing
                 // thread did a CAS of new table then a CAS store of a Prime.  This thread
                 // does regular read of the Prime, then volatile read of new table - 
                 // but the read of Prime was so delayed (or the read of new table was 
                 // so accelerated) that they swapped and we still read a null new table.  
                 // The resize call below will do a CAS on new table forcing the read.
                 entryValue is Prime))
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

                // return this.putIfMatch(newTable, key, putval, expVal);
                table = newTable;
                goto tailCall;
            }

            // ---
            // We are finally prepared to update the existing table
            while (true)
            {
                Debug.Assert(!(entryValue is Prime));

                switch (expVal)
                {
                    case ValueMatch.Any:
                        break;

                    case ValueMatch.NullOrDead:
                        if (entryValue == null | entryValue == TOMBSTONE )
                        {
                            break;
                        }

                        return false;

                    case ValueMatch.NotNullOrDead:
                        if (entryValue == null | entryValue == TOMBSTONE)
                        {
                            return false;
                        }
                        break;
                }

                if (putval == entryValue)
                {
                    // Do not update!
                    return false; 
                }

                // Actually change the Value in the Key,Value pair
                if (Interlocked.CompareExchange(ref table[idx].value, putval, entryValue) == entryValue)
                {
                    // CAS succeeded - we did the update!
                    // Adjust sizes
                    if (entryValue == null || entryValue == TOMBSTONE)
                    {
                        if (putval != TOMBSTONE)
                        {
                            tableInfo._size.increment();
                        }
                    }
                    else
                    {
                        if (putval == TOMBSTONE)
                        {
                            tableInfo._size.decrement();
                        }
                    }

                    return true;
                }
                // Else CAS failed

                // Get new value
                entryValue = table[idx].value;

                // If a Prime'd value got installed, we need to re-run the put on the
                // new table.  Otherwise we lost the CAS to another racing put.
                // Simply retry from the start.
                if (entryValue is Prime)
                {
                    newTable = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: true);

                    // return this.putIfMatch(newTable, key, putval, expVal);
                    table = newTable;
                    goto tailCall;
                }
            }
        }

        private bool copyIfMatch(Entry[] table, TKeyStore key, object putval, int fullhash)
        {
            Debug.Assert(putval != TOMBSTONE);

            tailCall:

            Debug.Assert(putval != null);
            Debug.Assert(!(putval is Prime));

            int lenMask = GetTableLength(table) - 1;
            var tableInfo = GetTableInfo(table);
            int idx = fullhash & lenMask;

            // ---
            // Key-Claim stanza: spin till we can claim a Key (or force a resizing).
            object entryValue;
            int reprobe_cnt = 0;

            // Spin till we get a Key slot
            while (true)
            {
                var entry = table[idx];
                var entryHash = entry.hash;
                var entryKey = entry.key;
                entryValue = entry.value;

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

                // get and put must have the same key lookup logic!  Lest 'get' give
                // up looking too soon.
                if (++reprobe_cnt >= ReprobeLimit(lenMask) | // too many probes or
                    entryHash == TOMBPRIMEHASH)
                {
                    // found a TOMBPRIMEHASH, means no more keys
                    // We simply must have a new table to do a 'put'.  At this point a
                    // 'get' will also go to the new table (if any).  We do not need
                    // to claim a key slot (indeed, we cannot find a free one to claim!).
                    var resized = tableInfo.Resize(this, table);

                    // return this.putIfMatch(resized, key, putval, expVal);
                    table = resized;
                    goto tailCall;
                }

                // quadratic reprobing
                idx = (idx + reprobe_cnt) & lenMask; // Reprobe!    

            } // End of spinning till we get a Key slot

            // Found the proper Key slot, now update the matching Value slot.  We
            // never put a null, so Value slots monotonically move from null to
            // not-null (deleted Values use Tombstone).  Thus if 'V' is null we
            // fail this fast cutout and fall into the check for table-full.
            if (putval == entryValue)
            {
                return false; // Fast cutout for no-change
            }

            // See if we want to move to a new table (to avoid high average re-probe counts).  
            // We only check on the initial set of a Value from null to
            // not-null (i.e., once per key-insert).  Of course we got a 'free' check
            // of new table once per key-compare (not really free, but paid-for by the
            // time we get here).
            var newTable = tableInfo._newTable;
            if (newTable == null &&
                ((entryValue == null && tableInfo.tableFull(reprobe_cnt, lenMask + 1)) ||
                 // Or we found a Prime, but the VM allowed reordering such that we
                 // did not spot the new table (very rare race here: the writing
                 // thread did a CAS of new table then a CAS store of a Prime.  This thread
                 // does regular read of the Prime, then volatile read of new table - 
                 // but the read of Prime was so delayed (or the read of new table was 
                 // so accelerated) that they swapped and we still read a null new table.  
                 // The resize call below will do a CAS on new table forcing the read.
                 entryValue is Prime))
            {
                newTable = tableInfo.Resize(this, table); // Force the new table copy to start
                Debug.Assert(tableInfo._newTable != null);
            }

            // See if we are moving to a new table.
            // If so, copy our slot and retry in the new table.
            if (newTable != null)
            {
                var newTable1 = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: false);
                Debug.Assert(newTable == newTable1);

                // return this.putIfMatch(newTable, key, putval, expVal);
                table = newTable;
                goto tailCall;
            }

            // ---
            // We are finally prepared to update the existing table
            while (true)
            {
                Debug.Assert(!(entryValue is Prime));

                // is there already a value in the new table?
                if (entryValue != null)
                {
                    return false;
                }                
                
                // Actually change the Value in the Key,Value pair
                if (Interlocked.CompareExchange(ref table[idx].value, putval, null) == null)
                {
                    // CAS succeeded - we did the update!
                    // table-copy does not (effectively) increase the number of live k/v pairs.
                    return true;
                }

                // Get new value
                entryValue = table[idx].value;

                // If a Prime'd value got installed, we need to re-run the put on the
                // new table.  Otherwise we lost the CAS to another racing put.
                // Simply retry from the start.
                if (entryValue is Prime)
                {
                    table = tableInfo.CopySlotAndCheck(this, table, idx, shouldHelp: false);
                    goto tailCall;
                }
            }
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

            public bool tableFull(int reprobe_cnt, int len)
            {
                return
                    /*
                                      // Do the cheap check first: we allow some number of reprobes always
                                      reprobe_cnt >= REPROBE_LIMIT &&
                                      // More expensive check: see if the table is > 1/4 full.
                                      _slots.estimate_get() >= ReprobeLimit(len);
                    */

                    // 80% utilization, switch to a bigger table
                _slots.estimate_get() > (len * 3) / 4;
                //_slots.estimate_get() > (len * 4) / 5;
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
                        // oldval is vaccuously available in the new table.  We
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
                var key = oldTable[idx].key;

                bool copiedIntoNew = topmap.copyIfMatch(newTable, key, originalValue, hash);

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
            public Entry[] Resize(NonBlockingDictionary<TKey, TKeyStore, TValue> topmap, Entry[] tabe)
            {
                Debug.Assert(topmap.GetTableInfo(tabe) == this);

                // Check for resize already in progress, probably triggered by another thread
                var newTable = this._newTable; // VOLATILE READ
                // See if resize is already in progress
                if (newTable != null)
                {
                    // Use the new table already
                    return newTable;           
                }

                // No copy in-progress, so start one.  First up: compute new table size.
                int oldlen = topmap.GetTableLength(tabe);    // Old count of K,V pairs allowed
                int sz = size();          // Get current table count of active K,V pairs
                int newsz = sz;           // First size estimate

                // Heuristic to determine new size.  We expect plenty of dead-slots-with-keys
                // and we need some decent padding to avoid endless reprobing.

                //if (sz >= (oldlen >> 2))
                //{ // If we are >25% full of keys then...
                //    newsz = oldlen << 1;      // Double size
                //    // If we are >50% full of keys then...
                //    if (sz >= (oldlen >> 1))
                //    {
                //        newsz = oldlen << 2;    // Double double size
                //    }
                //}

                // This heuristic in the next 2 lines leads to a much denser table
                // with a higher reprobe rate
                //if (sz >= (oldlen >> 1))
                //{
                //    // If we are >50% full of keys then...
                //    newsz = oldlen << 1;    // Double size
                //}

                //// Last (re)size operation was very recent?  Then double again; slows
                //// down resize operations for tables subject to a high key churn rate.
                //long tm = CurrentTimeMillis();
                //int q = 0;
                //if (newsz <= oldlen && // New table would shrink or hold steady?
                //    tm <= topmap._lastResizeMilli + 10000 && // Recent resize (less than 1 sec ago)
                //    (q = _slots.estimate_get()) >= (sz << 1)) // 1/2 of keys are dead?
                //{
                //    newsz = oldlen << 1;      // Double the existing size
                //}

                //TODO: (vsadov) just double the actual size for now
                newsz <<= 1;

                // Do not shrink, ever
                //TODO: (vsadov) really?
                if (newsz < oldlen)
                {
                    newsz = oldlen;
                }

                // Convert to power-of-2
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
                return Interlocked.CompareExchange(ref this._newTable, newTable, null) ?? newTable;
            }
        }
    }
}
