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
using System.Collections.Concurrent;

namespace NonBlockingTests
{
    class Program
    {
        static void Main(string[] args)
        {
            //for (;;)
            //{
                RunOnce();
                RunOnce();
                RunOnce();
                RunOnce();
            //}

            //RunMany();
            //ChurnSequential();

            //ChurnSequential();
            //ChurnConcurrent();
        }

        //private static void RunMany()
        //{
        //    for (int i = 0; i < 32; i++)
        //    {
        //        NonBlocking.Counter.MAX_CELL_COUNT = i;
        //        for (int j = 0; j < 10; j++)
        //        {
        //            NonBlocking.Counter.MAX_DRIFT = j * j;
        //            RunOnce();
        //            System.Console.Write(" ");
        //        }
        //        System.Console.WriteLine();
        //    }
        //}

        private static void RunOnce()
        {
            var arr = new long[] {
            GetBenchSmall(),
            GetBenchSmall(),
            GetBenchSmall(),
            GetBenchSmall(),
            GetBenchSmall(),};

            System.Console.WriteLine(arr.Min());
        }

        private static void GetBench()
        {
            var dict = NonBlockingDictionary.Create<int, string>();
            //var dict = new System.Collections.Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 100000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100000, (i) => { var dummy = dict[i]; });

            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 5000; j++)
            {
                Parallel.For(0, 50000, (i) => { var dummy = dict[i]; });
            }

            sw.Stop();
            System.Console.WriteLine(sw.ElapsedMilliseconds);
        }

        private static long GetBenchSmall()
        {
            var dict = NonBlockingDictionary.Create<int, string>();
            //var dict = new System.Collections.Concurrent.ConcurrentDictionary<int, string>();

            Parallel.For(0, 10000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 10000, (i) => { var dummy = dict[i]; });

            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 50000; j++)
            {
                Parallel.For(0, 5000, (i) => { var dummy = dict[i]; });
            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static void GetBenchRef()
        {
            var dict = NonBlockingDictionary.Create<object, string>();
            //var dict = new System.Collections.Concurrent.ConcurrentDictionary<object, string>();

            var rnd = new Random();

            var list = new List<object>();
            for (int i = 0; i < 100000; i++)
            {
                list.Add(rnd.Next());
            }

            Parallel.For(0, 100000, (i) => dict[list[i]] = i.ToString());
            Parallel.For(0, 100000, (i) => { var dummy = dict[list[i]]; });

            var sw = Stopwatch.StartNew();

            for (int j = 0; j < 5000; j++)
            {
                Parallel.For(0, 50000, (i) => { var dummy = dict[list[i]]; });
            }

            sw.Stop();
            System.Console.WriteLine(sw.ElapsedMilliseconds);
        }

        private static void AssignBenchSmall()
        {
            var dict = NonBlockingDictionary.Create<int, string>();
            //var dict = new System.Collections.Concurrent.ConcurrentDictionary<int, string>();

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
                // dict.print();
                //string value;
                //Parallel.For(0, 100000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
            }

            sw.Stop();
            System.Console.WriteLine(sw.ElapsedMilliseconds);
        }

        private static long  AddBenchSmall()
        {
            var dict = NonBlockingDictionary.Create<int, string>();
            //var dict = new System.Collections.Concurrent.ConcurrentDictionary<int, string>();

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
                // dict.print();
                //string value;
                //Parallel.For(0, 100000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
            }

            sw.Stop();
            return sw.ElapsedMilliseconds;
        }

        private static void AssignBench()
        {
            var dict = NonBlockingDictionary.Create<int, string>();
            //var dict = new System.Collections.Concurrent.ConcurrentDictionary<int, string>();

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
                // dict.print();
                //string value;
                //Parallel.For(0, 100000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
            }

            sw.Stop();
            System.Console.WriteLine(sw.ElapsedMilliseconds);
        }

        private static void AssignBenchRef()
        {
            var dict = NonBlockingDictionary.Create<object, string>();
            //var dict = new System.Collections.Concurrent.ConcurrentDictionary<object, string>();

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
                // dict.print();
                //string value;
                //Parallel.For(0, 100000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
            }

            sw.Stop();
            System.Console.WriteLine(sw.ElapsedMilliseconds);
        }

        private static void ChurnSequential()
        {
            var dict = NonBlockingDictionary.Create<int, string>();

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
            var dict = NonBlockingDictionary.Create<int, string>();
            //var dict = new ConcurrentDictionary<int, string>();

            var threadCnt = 100;
            List<Task> tasks = new List<Task>(threadCnt);

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
        }
    }

}
