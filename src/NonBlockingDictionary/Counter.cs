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
    public sealed class Counter
    {
        public const int MAX_DRIFT = 42;
        public static readonly int MAX_CELL_COUNT = Environment.ProcessorCount * 2 - 1;

        private class Cell
        {
            [StructLayout(LayoutKind.Explicit)]
            public struct SpacedCounter
            {
                // 64 bytes - sizeof(int) - sizeof(objecHeader32)
                [FieldOffset(52)]
                public int cnt;
            }

            public SpacedCounter counter;
        }

        // spaced out counters
        private Cell[] cells;

        // default counter
        private int cnt;

        // how many cells we have
        private int cellCount;
        
        public Counter()
        {
        }

        public int get()
        {
            var count = this.cnt;
            var cells = this.cells;

            if (cells != null)
            {
                for(int i = 0; i < cells.Length; i++)
                {
                    var cell = cells[i];
                    if (cell != null)
                    {
                        count += cell.counter.cnt;
                    }
                    else
                    {
                        break;
                    }
                }
            }

            return count;
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
            var cellCount = this.cellCount;
            var idx = GetIndex(cellCount);

            int retries;
            if (idx == 0)
            {
                retries = increment(ref this.cnt);
            }
            else
            {
                retries = increment(ref this.cells[idx - 1].counter.cnt);
            }

            if (retries > MAX_DRIFT)
            {
                //System.Console.WriteLine(retries);
                TryAddCell(cellCount);
            }
        }

        private static int increment(ref int val)
        {
            var expected = val + 1;
            var newVal = Interlocked.Increment(ref val);

            var drift = fastAbs(newVal - expected);

            return drift;
        }

        public void decrement()
        {
            var cellCount = this.cellCount;
            var idx = GetIndex(cellCount);

            int retries;
            if (idx == 0)
            {
                retries = decrement(ref this.cnt);
            }
            else
            {
                retries = decrement(ref this.cells[idx - 1].counter.cnt);
            }

            if (retries > MAX_DRIFT)
            {
                //System.Console.WriteLine(retries);
                TryAddCell(cellCount);
            }
        }

        private static int decrement(ref int val)
        {
            var expected = val - 1;
            var newVal = Interlocked.Decrement(ref val);

            var drift = fastAbs(newVal - expected);

            return drift;
        }

        // this is faster than Math.Abs, 
        // but will not throw for int.MinValue, which we can ignore
        // as tolearable and extremely unlikely
        private static int fastAbs(int arg)
        {
            // -1 when arg is negative, 0 otherwise
            var minOneWhenNegative = arg >> 31;

            // ~(arg + 1)   is same as negation
            return (arg - minOneWhenNegative) ^ minOneWhenNegative;
        }

        private void TryAddCell(int cellCount)
        {
            if (cellCount < MAX_CELL_COUNT)
            {
                var cells = this.cells;
                if (cells == null)
                {
                    var newCells = new Cell[32];
                    cells = Interlocked.CompareExchange(ref this.cells, newCells, null) ?? newCells;
                }

                if (cells[cellCount] == null)
                {
                    Interlocked.CompareExchange(ref cells[cellCount], new Cell(), null);
                }

                if (this.cellCount == cellCount)
                {
                    Interlocked.CompareExchange(ref this.cellCount, cellCount + 1, cellCount);
                    //if (Interlocked.CompareExchange(ref this.cellCount, cellCount + 1, cellCount) == cellCount)
                    //{
                    //    System.Console.WriteLine(cellCount + 1);
                    //}
                }
            }
        }

        private static int GetIndex(int cellCount)
        {
            return cellCount == 0 ?
                0 :
                Environment.CurrentManagedThreadId % (cellCount + 1);
        }

        internal static long CurrentTimeMillis()
        {
            return DateTime.UtcNow.Ticks / TimeSpan.TicksPerMillisecond;
        }
    }
}
