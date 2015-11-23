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
        private sealed class TableInfo
        {
            internal Entry[] _newTable;

            internal readonly Counter _size;
            internal readonly Counter _slots;

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

            internal bool TableIsCrowded(int len)
            {
                // 80% utilization, switch to a bigger table
                return EstimatedSlotsUsed > (len >> 2) * 3;
            }

            // Help along an existing resize operation.  We hope its the top-level
            // copy (it was when we started) but this table might have been promoted 
            // out of the top position.
            internal void HelpCopyImpl(DictionaryImpl<TKey, TKeyStore, TValue> topDict, Entry[] oldTable, bool copy_all)
            {
                Debug.Assert(topDict.GetTableInfo(oldTable) == this);
                Entry[] newTable = this._newTable;
                Debug.Assert(newTable != null);    // Already checked by caller
                int oldlen = topDict.GetTableLength(oldTable); // Total amount to copy

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
                            Promote(topDict, oldTable);
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
                            Promote(topDict, oldTable);
                            return;
                        }

                        if (CopySlot(topDict, (copyStart + i) & (oldlen - 1), oldTable, newTable))
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
                            Promote(topDict, oldTable);
                        }
                    }

                    if (!copy_all && !panic)
                    {
                        return;
                    }
                }

                // Extra promotion check, in case another thread finished all copying
                // then got stalled before promoting.
                Promote(topDict, oldTable);
            }

            private void Promote(DictionaryImpl<TKey, TKeyStore, TValue> topDict, Entry[] oldTable)
            {
                // Looking at the top-level table?
                // Note that we might have
                // nested in-progress copies and manage to finish a nested copy before
                // finishing the top-level copy.  We only promote top-level copies.
                if (topDict._table == oldTable)
                {
                    // Attempt to promote
                    if (Interlocked.CompareExchange(ref topDict._table, this._newTable, oldTable) == oldTable)
                    {
                        // System.Console.WriteLine("size: " + _newTable.Length);
                        topDict._lastResizeTickMillis = CurrentTickMillis();
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
            internal Entry[] CopySlotAndCheck(DictionaryImpl<TKey, TKeyStore, TValue> topDict, Entry[] oldTable, int idx, bool shouldHelp)
            {
                Debug.Assert(topDict.GetTableInfo(oldTable) == this);
                var newTable = this._newTable;

                // We're only here because the caller saw a Prime, which implies a
                // table-copy is in progress.
                Debug.Assert(newTable != null);

                if (CopySlot(topDict, idx, oldTable, newTable))
                {
                    // Record the slot copied
                    var copyDone = Interlocked.Increment(ref this._copyDone);

                    // Check for copy being ALL done, and promote.  
                    if (copyDone >= topDict.GetTableLength(oldTable))
                    {
                        Promote(topDict, oldTable);
                    }
                }

                // Generically help along any copy (except if called recursively from a helper)
                if (shouldHelp)
                {
                    topDict.HelpCopy();
                }

                return newTable;
            }

            // Copy one K/V pair from old table to new table. 
            // Returns true if we actually did the copy.
            // Regardless, once this returns, the copy is available in the new table and 
            // slot in the old table is no longer usable.
            private bool CopySlot(DictionaryImpl<TKey, TKeyStore, TValue> topDict, int idx, Entry[] oldTable, Entry[] newTable)
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
                bool copiedIntoNew = topDict.CopySlot(newTable, key, originalValue, hash);

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
            internal Entry[] Resize(DictionaryImpl<TKey, TKeyStore, TValue> topDict, Entry[] table)
            {
                Debug.Assert(topDict.GetTableInfo(table) == this);

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
                int oldlen = topDict.GetTableLength(table);

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
                    var resizeSpan = CurrentTickMillis() - topDict._lastResizeTickMillis;

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
