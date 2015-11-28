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
            CounterPerf();

            GetBenchNB();
            GetBenchCD();
            GetBenchRndNB();
            GetBenchRndCD();
        }

        private static void EmptyAction()
        {
            var benchmarkName = "======== EmptyAction 1M Ops/sec:";

            Counter cnt = new Counter();
            Action<int> act = _ => {};

            RunBench(benchmarkName, act);
        }

        private static void CounterPerf()
        {
            var benchmarkName = "======== Counter 1M Ops/sec:";

            Counter cnt = new Counter();
            Action<int> act = _ => { cnt.increment(); };

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

            Parallel.For(0, 100000, (i) => dict[MixBits(i)] = "qq");
            Parallel.For(0, 100000, (i) => { var dummy = dict[MixBits(i)]; });

            var benchmarkName = "======== Get NonBlocking Rnd 1M Ops/sec:";

            Action<int> act = (i) => 
            {
                int randIdx = MixBits(i);
                string dummy;
                dict.TryGetValue(randIdx, out dummy);
            };

            RunBench(benchmarkName, act);
        }

        private static void GetBenchRndCD()
        {
            var dict = new Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 100000, (i) => dict[MixBits(i)] = "qq");
            Parallel.For(0, 100000, (i) => { var dummy = dict[MixBits(i)]; });

            var benchmarkName = "======== Get Concurrent Rnd 1M Ops/sec:";

            Action<int> act = (i) =>
            {
                int randIdx = MixBits(i);
                string dummy;
                dict.TryGetValue(randIdx, out dummy);
            };

            RunBench(benchmarkName, act);
        }





        private static long RunBenchmark(Action<int> action, int threads, int time)
        {
            Counter cnt = new Counter();
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
                    cnt.increment(batch);
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
                var MOps = RunBenchmark(action, i, 3000) / 3000000;
                System.Console.Write(MOps + " ");
            }
            System.Console.WriteLine();
        }

        private static int MixBits(int i)
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

        private static long GetBenchRef()
        {
            var dict = new NonBlocking.ConcurrentDictionary<object, string>();
            //var dict = new Concurrent.ConcurrentDictionary<object, string>();

            var rnd = new Random();

            var list = new List<object>();
            for (int i = 0; i < 100000; i++)
            {
                list.Add(rnd.Next());
            }

            Parallel.For(0, 100000, (i) => dict[list[i]] = i.ToString());
            Parallel.For(0, 100000, (i) => { var dummy = dict[list[i]]; });

            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 3000; j++)
            {
                Parallel.For(0, 50000, (i) => { var dummy = dict[list[i]]; });
            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static void AssignBenchSmall()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new Concurrent.ConcurrentDictionary<int, string>();

            var listV = new List<string>();
            for (int i = 0; i < 10000; i++)
            {
                listV.Add(i.ToString());
            }

            Parallel.For(0, 10000, (i) => dict[i] = listV[i]);
            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 10000; j++)
            {
                Parallel.For(1, 10000, (i) => dict[i - 1] = listV[i]);
                Parallel.For(0, 10000, (i) => dict[i] = listV[i]);
            }

            sw.Stop();
            System.Console.WriteLine(sw.ElapsedMilliseconds);
        }

        private static long  AddBenchSmall()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new Concurrent.ConcurrentDictionary<int, string>();

            var listV = new List<string>();
            for (int i = 0; i < 10000; i++)
            {
                listV.Add(i.ToString());
            }

            Parallel.For(0, 10000, (i) => dict[i] = listV[i]);
            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 5000; j++)
            {
                Parallel.For(1, 10000, (i) => dict[i - 1] = listV[i]);
                Parallel.For(1, 10000, (i) => { string s; dict.TryRemove(i - 1, out s); });
                Parallel.For(0, 10000, (i) => dict[i] = listV[i]);
                Parallel.For(0, 10000, (i) => { string s; dict.TryRemove(i, out s); });
            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static long GetOrAddTBenchSmall()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 10000, (i) => dict[i] = "ww");
            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 5000; j++)
            {
                Parallel.For(1, 10000, (i) => dict.GetOrAdd(i - 1, "aa"));
                Parallel.For(0, 10000, (i) => dict.GetOrAdd(i, "bb"));
                Parallel.For(1, 10000, (i) => dict.GetOrAdd(i - 1, "aa"));
                Parallel.For(0, 10000, (i) => dict.GetOrAdd(i, "bb"));
            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static long GetOrAddFuncBenchSmall()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 10000, (i) => dict[i] = "ww");
            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 5000; j++)
            {
                Parallel.For(1, 10000, (i) => dict.GetOrAdd(i - 1, (_)=>"aa"));
                Parallel.For(0, 10000, (i) => dict.GetOrAdd(i, (_) => "bb"));
            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static long GetOrAddTBenchSmall1()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 10000, (i) => dict[i] = "ww");
            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 10000; j++)
            {
                Parallel.For(0, 10000, (i) =>
                {
                    string s;
                    if (i % 3 == 0)
                    {
                        dict.TryRemove(i, out s);
                    }
                });
                Debug.Assert(dict.Count == 6666);

                Parallel.For(0, 10000, (i) => dict.GetOrAdd(i, "aa"));
                Debug.Assert(dict.Count == 10000);
            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static long GetOrAddFuncBenchSmall1()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 10000, (i) => dict[i] = "ww");
            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 10000; j++)
            {
                //dict.Clear();
                Parallel.For(0, 10000, (i) =>
                {
                    string s;
                    //if (i % 3 == 0)
                    {
                        dict.TryRemove(i, out s);
                    }
                });
                //Debug.Assert(dict.Count == 6666);

                Parallel.For(0, 10000, (i) => dict.GetOrAdd(i, (_) => "aa"));
                Debug.Assert(dict.Count == 10000);
            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static long AddBenchSmallSequential()
        {
            //var dict = new System.Collections.Generic.Dictionary<int, string>();
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new Concurrent.ConcurrentDictionary<int, string>();

            var listV = new List<string>();
            for (int i = 0; i < 10000; i++)
            {
                listV.Add(i.ToString());
            }

            for (int i = 0; i < 10000; i++)
            {
                dict[i] = listV[i];
            }

            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 5000; j++)
            {
                for(int i = 0; i < 10000; i++)
                {
                    dict[i - 1] = listV[i];
                }

                for (int i = 0; i < 10000; i++)
                {
                    string s; dict.TryRemove(i - 1, out s);
                }

                for (int i = 0; i < 10000; i++)
                {
                    dict[i] = listV[i];
                }

                for (int i = 0; i < 10000; i++)
                {
                    string s; dict.TryRemove(i, out s);
                }

            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static void AssignBench()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();
            //var dict = new Concurrent.ConcurrentDictionary<int, string>();

            var listV = new List<string>();
            for (int i = 0; i < 100000; i++)
            {
                listV.Add(i.ToString());
            }

            Parallel.For(0, 100000, (i) => dict[i] = listV[i]);
            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 1000; j++)
            {
                Parallel.For(1, 100000, (i) => dict[i - 1] = listV[i]);
                Parallel.For(0, 100000, (i) => dict[i] = listV[i]);
            }

            sw.Stop();
            System.Console.WriteLine(sw.ElapsedMilliseconds);
        }

        private static void AssignBenchRef()
        {
            var dict = new NonBlocking.ConcurrentDictionary<object, string>();
            //var dict = new Concurrent.ConcurrentDictionary<object, string>();

            var list = new List<object>();
            for (int i = 0; i < 100000; i++)
            {
                list.Add(i);
            }

            var listV = new List<string>();
            for (int i = 0; i < 100000; i++)
            {
                listV.Add(i.ToString());
            }

            Parallel.For(0, 100000, (i) => dict[list[i]] = listV[i]);
            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 1000; j++)
            {
                Parallel.For(1, 100000, (i) => dict[list[i - 1]] = listV[i]);
                Parallel.For(0, 100000, (i) => dict[list[i]] = listV[i]);
            }

            sw.Stop();
            System.Console.WriteLine(sw.ElapsedMilliseconds);
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
