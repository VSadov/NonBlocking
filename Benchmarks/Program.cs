using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using NonBlocking;

namespace NonBlockingTests
{
    class Program
    {
        static void Main(string[] args)
        {
            GetBenchSmall();
            GetBenchSmall();
            GetBenchSmall();
            GetBenchSmall();
            GetBenchSmall();
            GetBenchSmall();
            GetBenchSmall();
            GetBenchSmall();
            GetBenchSmall();
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

        private static void GetBenchSmall()
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
            System.Console.WriteLine(sw.ElapsedMilliseconds);
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
    }

}
