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
    internal sealed class Counter
    {
        [StructLayout(LayoutKind.Explicit)]
        private struct Cell
        {
            // do not overlap with other counters or with array.Length
            [FieldOffset(60)]
            public int cnt;
        }

        private Cell[] cells;

        //private int lastCnt;
        //private long lastTicks;

        public Counter()
            : this(4)
        {
        }

        private Counter(int size)
        {
            cells = new Cell[size];
        }

        public int get()
        {
            var cells = this.cells;
            var sum = 0;
            for(int i = 0; i < cells.Length; i++)
            {
                sum += cells[i].cnt;
            }

            return sum;
        }

        internal int estimate_get()
        {
            return get();

            // TODO: is there a scenario where the following is cheaper?
            //       we woud need to have a lot of counters.

            //var curTicks = DateTime.UtcNow.Ticks;
            //// more than millisecond passed?
            //if (curTicks - lastTicks > TimeSpan.TicksPerMillisecond)
            //{
            //    lastCnt = get();
            //    lastTicks = curTicks;
            //}

            //return lastCnt;
        }

        public void increment()
        {
            var cells = this.cells;
            int idx = GetIndex(cells);

            int val = cells[idx].cnt;
            Interlocked.Increment(ref cells[idx].cnt);
        }

        public void decrement()
        {
            var cells = this.cells;
            int idx = GetIndex(cells);

            int val = cells[idx].cnt;
            Interlocked.Decrement(ref cells[idx].cnt);
        }

        private static int GetIndex(Cell[] cells)
        {
            var mask = cells.Length - 1;
            return mask == 0 ?
                mask :
                Environment.CurrentManagedThreadId & mask;
        }

        internal static long CurrentTimeMillis()
        {
            return DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
        }
    }
}
