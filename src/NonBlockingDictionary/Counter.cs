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
    //TODO: replace with counter that scales
    internal sealed class Counter
    {
        private int cnt;
        public int get()
        {
            return Volatile.Read(ref cnt);
        }
        internal int estimate_get()
        {
            return cnt;
        }

        public void increment()
        {
            Interlocked.Increment(ref cnt);
        }
        public void decrement()
        {
            Interlocked.Decrement(ref cnt);
        }
    }
}
