// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Threading;
using Xunit;

namespace NonBlockingTests
{
    public class EnumerationTests
    {
        // Integer keys use 0 as the empty key, and a real key 0 is stored under a reserved hash. A
        // slot an insert has claimed but not yet given a key reads as key 0, so an enumerator that
        // resolves such a slot through a lookup yields the real key 0 a second time. Key 0 is in the
        // stable set while unique keys churn through a small window. Each round starts from a fresh
        // dictionary, because a young table resizes often and every resize copies its slots.
        [Fact]
        public static void EnumerationDuringChurnYieldsIntKeyZeroOnce()
            => CheckEnumerationDuringChurn(() => new NonBlocking.ConcurrentDictionary<int, long>(), n => (int)n);

        [Fact]
        public static void EnumerationDuringChurnYieldsLongKeyZeroOnce()
            => CheckEnumerationDuringChurn(() => new NonBlocking.ConcurrentDictionary<long, long>(), n => n);

        private static void CheckEnumerationDuringChurn<TKey>(Func<NonBlocking.ConcurrentDictionary<TKey, long>> create, Func<long, TKey> key)
        {
            const int StableKeys = 1_000;
            // Oversubscribed, so writers are preempted between claiming a slot and writing its key.
            int threads = Math.Max(4, 2 * Environment.ProcessorCount);
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(3);
            string problem = null;
            int enumerations = 0;
            var seen = new HashSet<long>();

            for (int round = 0; problem == null && DateTime.UtcNow < deadline; round++)
            {
                var d = create();
                for (long k = 0; k < StableKeys; k++)
                {
                    d[key(k)] = Expected(k);
                }

                using var stop = new CancellationTokenSource();
                var writers = new Thread[threads];
                for (int t = 0; t < threads; t++)
                {
                    int id = t;
                    writers[t] = new Thread(() =>
                    {
                        var window = new Queue<TKey>();
                        for (long n = 0; !stop.IsCancellationRequested; n++)
                        {
                            long k = StableKeys + n * threads + id;
                            if (k > int.MaxValue)
                            {
                                break;
                            }

                            d.TryAdd(key(k), -k);
                            window.Enqueue(key(k));
                            if (window.Count > 64)
                            {
                                d.TryRemove(window.Dequeue(), out _);
                            }
                        }
                    });
                    writers[t].Start();
                }

                // The round's clock starts once every writer has been started, and each round
                // enumerates at least once, so slow thread startup cannot leave it unchecked.
                stop.CancelAfter(TimeSpan.FromMilliseconds(200));
                do
                {
                    seen.Clear();
                    foreach (var kv in d)
                    {
                        long k = Convert.ToInt64(kv.Key);
                        if (k >= StableKeys)
                        {
                            continue;
                        }

                        if (!seen.Add(k))
                        {
                            problem = $"round {round}: key {k} yielded twice in one enumeration";
                        }
                        else if (kv.Value != Expected(k))
                        {
                            problem = $"round {round}: key {k} enumerated with value {kv.Value}";
                        }
                    }

                    if (problem == null && seen.Count != StableKeys)
                    {
                        problem = $"round {round}: enumeration yielded {seen.Count} of {StableKeys} stable keys";
                    }

                    enumerations++;
                }
                while (!stop.IsCancellationRequested && problem == null);

                stop.Cancel();
                foreach (var w in writers)
                {
                    w.Join();
                }
            }

            Assert.True(problem == null, $"{typeof(TKey).Name} keys: {problem}");
            Assert.True(enumerations > 0, "no enumeration completed");

            static long Expected(long k) => k * 2 + 1;
        }
    }
}
