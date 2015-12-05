// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using NonBlocking;
using System.Threading;
using Concurrent = System.Collections.Concurrent;

namespace NonBlockingTests
{
    class Program
    {
        static void Main(string[] args)
        {
            if (Stopwatch.IsHighResolution)
            {
                System.Console.WriteLine("Timer: High Resolution");
            }
            else
            {
                System.Console.WriteLine("Timer: Low Resolution");
            }

            EmptyAction();
            InterlockedIncrement();

            Counter32Perf();
            Counter64Perf();

            GetBenchNB();
            GetBenchCD();
            GetBenchRndNB();
            GetBenchRndCD();

            AddBenchRndNB();
            AddBenchRndCD();

            GetOrAddFuncBenchRndNB();
            GetOrAddFuncBenchRndCD();
        }

        private static void EmptyAction()
        {
            var benchmarkName = "======== EmptyAction 1M Ops/sec:";
            Action<int> act = _ => {};
            RunBench(benchmarkName, act);
        }

        private static void InterlockedIncrement()
        {
            var benchmarkName = "======== InterlockedIncrement 1M Ops/sec:";
            int cnt = 0;
            Action<int> act = _ => { Interlocked.Increment(ref cnt); };

            RunBench(benchmarkName, act);
        }

        private static void Counter32Perf()
        {
            var benchmarkName = "======== Counter32 1M Ops/sec:";

            Counter32 cnt = new Counter32();
            Action<int> act = _ => { cnt.Increment(); };

            RunBench(benchmarkName, act);
        }

        private static void Counter64Perf()
        {
            var benchmarkName = "======== Counter64 1M Ops/sec:";

            Counter64 cnt = new Counter64();
            Action<int> act = _ => { cnt.Increment(); };

            RunBench(benchmarkName, act);
        }

        private static void GetBenchNB()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();

            Parallel.For(0, 100000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100000, (i) => { var dummy = dict[i]; });

            var benchmarkName = "======== Get NonBlocking 1M Ops/sec:";

            Action<int> act = (i) => {var dummy = dict[i % 100000]; };

            RunBench(benchmarkName, act);
        }

        private static void GetBenchCD()
        {
            var dict = new Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 100000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100000, (i) => { var dummy = dict[i]; });

            var benchmarkName = "======== Get Concurrent 1M Ops/sec:";

            Action<int> act = (i) => {var dummy = dict[i % 100000]; };

            RunBench(benchmarkName, act);
        }

        private static void GetBenchRndNB()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();

            Parallel.For(0, 100000, (i) => dict[i] = "qq");
            Parallel.For(0, 100000, (i) => { var dummy = dict[i]; });

            var benchmarkName = "======== Random Get NonBlocking 1M Ops/sec:";

            Action<int> act = (i) => 
            {
                string dummy;
                dict.TryGetValue(RandomizeBits(i), out dummy);
            };

            RunBench(benchmarkName, act);
        }

        private static void GetBenchRndCD()
        {
            var dict = new Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 100000, (i) => dict[i] = "qq");
            Parallel.For(0, 100000, (i) => { var dummy = dict[i]; });

            var benchmarkName = "======== Random Get Concurrent 1M Ops/sec:";

            Action<int> act = (i) =>
            {
                string dummy;
                dict.TryGetValue(RandomizeBits(i), out dummy);
            };

            RunBench(benchmarkName, act);
        }

        private static void AddBenchRndNB()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();

            var benchmarkName = "======== Random Add NonBlocking 1M Ops/sec:";

            Action<int> act = (i) =>
            {
                if (i > 100000)
                {
                    dict = new NonBlocking.ConcurrentDictionary<int, string>();
                }
                dict.TryAdd(RandomizeBits(i), "qq");
            };

            RunBench(benchmarkName, act);
        }

        private static void AddBenchRndCD()
        {
            var dict = new Concurrent.ConcurrentDictionary<int, string>();

            var benchmarkName = "======== Random Add Concurrent 1M Ops/sec:";

            Action<int> act = (i) =>
            {
                if (i > 100000)
                {
                    dict = new Concurrent.ConcurrentDictionary<int, string>();
                }
                dict.TryAdd(RandomizeBits(i), "qq");
            };

            RunBench(benchmarkName, act);
        }

        private static void GetOrAddFuncBenchRndNB()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();

            var benchmarkName = "======== Random GetOrAdd Func NonBlocking 1M Ops/sec:";

            Action<int> act = (i) =>
            {
                if (i > 100000)
                {
                    dict = new NonBlocking.ConcurrentDictionary<int, string>();
                }
                dict.GetOrAdd(RandomizeBits(i), (_)=>"qq");
            };

            RunBench(benchmarkName, act);
        }

        private static void GetOrAddFuncBenchRndCD()
        {
            var dict = new Concurrent.ConcurrentDictionary<int, string>();

            var benchmarkName = "======== Random GetOrAdd Func Concurrent 1M Ops/sec:";

            Action<int> act = (i) =>
            {
                if (i > 100000)
                {
                    dict = new Concurrent.ConcurrentDictionary<int, string>();
                }
                dict.GetOrAdd(RandomizeBits(i), (_) => "qq");
            };

            RunBench(benchmarkName, act);
        }

        private static long RunBenchmark(Action<int> action, int threads, int time)
        {
            Counter64 cnt = new Counter64();
            Task[] workers = new Task[threads];
            Stopwatch sw = Stopwatch.StartNew();
            ManualResetEventSlim e = new ManualResetEventSlim();
            long stop_time = 0;

            Action body = () =>
            {
                int iteration = 0;
                e.Wait();
                while (sw.ElapsedMilliseconds < stop_time)
                {
                    const int batch = 10000;
                    for (int i = 0; i < batch; i++)
                    {
                        action(iteration++);
                    }
                    cnt.Add(batch);
                }
            };

            for (int i = 0; i < workers.Length; i++)
            {
                workers[i] = Task.Factory.StartNew(body, TaskCreationOptions.LongRunning);
            }

            stop_time = sw.ElapsedMilliseconds + time;
            e.Set();

            Task.WaitAll(workers);
            return cnt.Value;
        }

        private static void RunBench(string benchmarkName, Action<int> action)
        {
            System.Console.WriteLine(benchmarkName);
            var max_threads = Environment.ProcessorCount;
            for (int i = 1; i <= max_threads; i++)
            {
                var MOps = RunBenchmark(action, i, 3000)  / 3000000.0;
                if (MOps > 1000)
                {
                    System.Console.Write("{0:f0} ", MOps);
                }
                else if (MOps > 100)
                {
                    System.Console.Write("{0:f1} ", MOps);
                }
                else if (MOps > 10)
                {
                    System.Console.Write("{0:f2} ", MOps);
                }
                else if (MOps > 1)
                {
                    System.Console.Write("{0:f3} ", MOps);
                }
                else
                {
                    System.Console.Write("{0:f4} ", MOps);
                }
            }
            System.Console.WriteLine();
        }

        private static int RandomizeBits(int i)
        {
            uint h = (uint)i;
            // 32-bit finalizer for MurmurHash3.
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;

            return (int)h;
        }


        private static void ChurnSequential()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();

            for (int i = 0; i < 1000000; i++)
            {
                dict.Add(i, "dummy");
                dict.Remove(i);
                //Thread.Sleep(10);
            }

            for (int i = 0; i < 100000; i++)
            {
                dict.Add(i, "dummy");
                dict.Remove(i);
                Thread.Sleep(5);
            }

        }

        private static void ChurnConcurrent()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new ConcurrentDictionary<int, string>();

            var threadCnt = 200;
            List<Task> tasks = new List<Task>(threadCnt);

            var sw = Stopwatch.StartNew();

            for (int i = 0; i < threadCnt; i++)
            {
                var task = new Task(() =>
                {
                    for (int j = i * 1000000, l = j + 1000000; j < l; j++)
                    {
                        string dummy;
                        dict.TryAdd(j, "dummy");
                        dict.TryRemove(j, out dummy);
                        //Thread.Sleep(10);
                    }
                    System.Console.Write('.');
                }, TaskCreationOptions.LongRunning);

                tasks.Add(task);
                task.Start();
            }

            Task.WaitAll(tasks.ToArray());

            System.Console.WriteLine(sw.ElapsedMilliseconds);
        }
    }

}
