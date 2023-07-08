﻿// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace NonBlocking
{
    /// <summary>
    /// Scalable 64bit counter that can be used from multiple threads.
    /// </summary>
    public sealed class Counter64 : CounterBase
    {
        private class Cell
        {
            [StructLayout(LayoutKind.Explicit, Size = CACHE_LINE * 2 - OBJ_HEADER_SIZE)]
            public struct SpacedCounter
            {
                [FieldOffset(CACHE_LINE - OBJ_HEADER_SIZE)]
                public long count;
            }

            public SpacedCounter counter;
        }

        // spaced out counters
        private Cell[]? cells;

        // default counter
        private long count;

        // delayed count
        private long lastCount;

        /// <summary>
        /// Returns the value of the counter at the time of the call.
        /// </summary>
        /// <remarks>
        /// The value may miss in-progress updates if the counter is being concurrently modified.
        /// </remarks>
        public long Value
        {
            get
            {
                var count = this.count;
                var cells = this.cells;

                if (cells != null)
                {
                    for (int i = 0; i < cells.Length; i++)
                    {
                        var cell = cells[i];
                        if (cell != null)
                        {
                            count += cell.counter.count;
                        }
                        else
                        {
                            break;
                        }
                    }
                }

                return count;
            }
        }

        /// <summary>
        /// Returns the approximate value of the counter at the time of the call.
        /// </summary>
        /// <remarks>
        /// EstimatedValue could be significantly cheaper to obtain, but may be slightly delayed.
        /// </remarks>
        public long EstimatedValue
        {
            get
            {
                if (this.cellCount == 0)
                {
                    return Value;
                }

                var curTicks = (uint)Environment.TickCount;
                // more than a millisecond passed?
                if (curTicks != lastCountTicks)
                {
                    lastCountTicks = curTicks;
                    lastCount = Value;
                }

                return lastCount;
            }
        }

        /// <summary>
        /// Increments the counter by 1.
        /// </summary>
        public void Increment()
        {
            int curCellCount = this.cellCount;
            var drift = increment(ref GetCountRef(curCellCount));

            if (drift != 0)
            {
                TryAddCell(curCellCount);
            }
        }

        /// <summary>
        /// Decrements the counter by 1.
        /// </summary>
        public void Decrement()
        {
            int curCellCount = this.cellCount;
            var drift = decrement(ref GetCountRef(curCellCount));

            if (drift != 0)
            {
                TryAddCell(curCellCount);
            }
        }

        /// <summary>
        /// Increments the counter by 'value'.
        /// </summary>
        public void Add(int value)
        {
            int curCellCount = this.cellCount;
            var drift = add(ref GetCountRef(curCellCount), value);

            if (drift != 0)
            {
                TryAddCell(curCellCount);
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private ref long GetCountRef(int curCellCount)
        {
            ref var countRef = ref count;

            Cell[]? cells;
            if ((cells = this.cells) != null && curCellCount > 1)
            {
                var cell = cells[GetIndex((uint)curCellCount)];
                if (cell != null)
                {
                    countRef = ref cell.counter.count;
                }
            }

            return ref countRef;
        }

        private static long increment(ref long val)
        {
            return -val - 1 + Interlocked.Increment(ref val);
        }

        private static long add(ref long val, int inc)
        {
            return -val - inc + Interlocked.Add(ref val, inc);
        }

        private static long decrement(ref long val)
        {
            return val - 1 - Interlocked.Decrement(ref val);
        }

        private void TryAddCell(int curCellCount)
        {
            if (curCellCount < s_MaxCellCount)
            {
                TryAddCellCore(curCellCount);
            }
        }

        private void TryAddCellCore(int curCellCount)
        {
            var cells = this.cells;
            if (cells == null)
            {
                var newCells = new Cell[s_MaxCellCount];
                cells = Interlocked.CompareExchange(ref this.cells, newCells, null) ?? newCells;
            }

            if (cells[curCellCount] == null)
            {
                Interlocked.CompareExchange(ref cells[curCellCount], new Cell(), null);
            }

            if (this.cellCount == curCellCount)
            {
                Interlocked.CompareExchange(ref this.cellCount, curCellCount + 1, curCellCount);
                //if (Interlocked.CompareExchange(ref this.cellCount, curCellCount + 1, curCellCount) == curCellCount)
                //{
                //    System.Console.WriteLine(curCellCount + 1);
                //}
            }
        }
    }
}
