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
using System.Collections.Concurrent;
using Xunit;

namespace NonBlockingTests
{
    public class Program
    {
        static void Main(string[] args)
        {
            for (;;)
            {
                CDTests();

                System.Console.WriteLine("AddSetRemove");
                AddSetRemove();
                System.Console.WriteLine("AddSetRemoveConcurrent");
                AddSetRemoveConcurrent();
                System.Console.WriteLine("AddSetRemoveConcurrentInt");
                AddSetRemoveConcurrentInt();
                AddSetRemoveConcurrent();

                System.Console.WriteLine("AddSetRemoveConcurrentIntInt");
                AddSetRemoveConcurrentIntInt();
                System.Console.WriteLine("AddSetRemoveConcurrentUIntInt");
                AddSetRemoveConcurrentUIntInt();
                System.Console.WriteLine("AddSetRemoveConcurrentLongInt");
                AddSetRemoveConcurrentLongInt();
                System.Console.WriteLine("AddSetRemoveConcurrentULongInt");
                AddSetRemoveConcurrentULongInt();
                System.Console.WriteLine("AddSetRemoveConcurrentIntPtrInt");
                AddSetRemoveConcurrentIntPtrInt();

                System.Console.WriteLine("AddSetRemoveConcurrentStruct");
                AddSetRemoveConcurrentStruct();

                System.Console.WriteLine("NullValueRef");
                NullValueRef();
                System.Console.WriteLine("NullValueNub");
                NullValueNub();

                System.Console.WriteLine("Continuity001");
                Continuity001();
                System.Console.WriteLine("Continuity002");
                Continuity002();

                System.Console.WriteLine("ContinuityOfRemove001");
                ContinuityOfRemove001();
                System.Console.WriteLine("ContinuityOfRemove002");
                ContinuityOfRemove002();

                System.Console.WriteLine("Relativity001");
                Relativity001();
                System.Console.WriteLine("Relativity002");
                Relativity002();

                System.Console.WriteLine("Relativity003");
                Relativity003();
                System.Console.WriteLine("Relativity004");
                Relativity004();

                System.Console.WriteLine("BadHashAdd");
                BadHashAdd();

                System.Console.WriteLine("============================= PASS");
                System.Console.WriteLine();
            }
        }

        private static void CDTests()
        {
            var tests = from mi in typeof(DictionaryImplTests).GetMethods()
                        where mi.CustomAttributes.Any(a => a.AttributeType.Name.Contains("Fact"))
                        select mi;

            foreach (var test in tests)
            {
                System.Console.WriteLine(test.Name);
                test.Invoke(null, Array.Empty<object>());
            }
        }

        private static void TimeIt(Action a)
        {
            a();

            for (int i = 0; i < 10; i++)
            {
                Stopwatch sw = Stopwatch.StartNew();
                a();
                sw.Stop();

                System.Console.WriteLine(sw.ElapsedMilliseconds);
            }
        }

        [Fact()]
        private static void NullValueRef()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();

            string s;
            Assert.False(dict.TryGetValue(0, out s));

            dict.Add(0, null);
            Assert.True(dict.TryGetValue(0, out s));
            Assert.Null(s);
            Assert.True(dict.Remove(0));
            Assert.False(dict.TryGetValue(0, out s));

            dict.Add(0, null);
            Assert.True(dict.TryRemove(0, out s));
            Assert.Null(s);
            Assert.False(dict.TryRemove(0, out s));

            Assert.Null(dict.GetOrAdd(0, (string)null));
            Assert.Null(dict.GetOrAdd(0, (string)null));
            Assert.True(dict.TryRemove(0, out s));

            Assert.Null(dict.GetOrAdd(0, _ => null));
            Assert.Null(dict.GetOrAdd(0, _ => null));
            Assert.True(dict.TryRemove(0, out s));

            Assert.Equal(dict, new KeyValuePair<int, string>[] { });
            Assert.Null(dict.GetOrAdd(0, (string)null));
            Assert.Equal(dict, new KeyValuePair<int, string>[] { new KeyValuePair<int, string>(0, null) });
        }

        [Fact()]
        private static void NullValueNub()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, int?>();

            int? s;
            if (dict.TryGetValue(0, out s))
            {
                throw new Exception();
            }

            dict.Add(0, null);
            if (!dict.TryGetValue(0, out s) || s != null)
            {
                throw new Exception();
            }

            if (!dict.Remove(0))
            {
                throw new Exception();
            }

            if (dict.TryGetValue(0, out s))
            {
                throw new Exception();
            }
        }

        [Fact()]
        private static void AddSetRemove()
        {
            var dict = new NonBlocking.ConcurrentDictionary<object, string>();

            for (int i = 0; i < 10; i++)
            {
                dict.Add(i, i.ToString());
            }

            for (int i = 0; i < 10; i++)
            {
                if (dict[i] != i.ToString()) throw new Exception();
            }

            for (int i = 0; i < 10; i++)
            {
                if (!dict.Remove(i)) throw new Exception();
            }


            for (int i = 0; i < 100; i++)
            {
                dict.Add(i, i.ToString());
            }

            for (int i = 0; i < 100; i++)
            {
                if (dict[i] != i.ToString()) throw new Exception();
            }

            for (int i = 0; i < 100; i++)
            {
                if (!dict.Remove(i)) throw new Exception();
            }


            for (int i = 0; i < 1000; i++)
            {
                dict.Add(i, i.ToString());
            }

            for (int i = 0; i < 1000; i++)
            {
                if (dict[i] != i.ToString()) throw new Exception();
            }

            for (int i = 0; i < 1000; i++)
            {
                if (!dict.Remove(i)) throw new Exception();
            }

        }

        [Fact()]
        private static void AddSetRemoveConcurrent()
        {
            var dict = new NonBlocking.ConcurrentDictionary<object, string>();

            Parallel.For(0, 10, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 10, (i) => dict[i] = i.ToString());
            Parallel.For(0, 10, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 10, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 100, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 100, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 1000, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 1000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 1000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 10000, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 10000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 10000, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 100000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 100000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
        }

        class NullIntolerantComparer : IEqualityComparer<object>
        {
            bool IEqualityComparer<object>.Equals(object x, object y)
            {
                if (x == null) throw new Exception("unexpected null");
                if (y == null) throw new Exception("unexpected null");

                return x == y;
            }

            int IEqualityComparer<object>.GetHashCode(object obj)
            {
                return obj.GetHashCode();
            }
        }
        
        [Fact()]
        private static void AddSetRemoveConcurrentNullIntolerant()
        {
            var dict = new NonBlocking.ConcurrentDictionary<object, int>(new NullIntolerantComparer());

            var keys = new object[100000];
            for (int i = 0; i < keys.Length; i++) keys[i] = i;

            Parallel.For(0, 10, (i) => dict.Add(keys[i], i));
            Parallel.For(0, 10, (i) => dict[keys[i]] = i);
            Parallel.For(0, 10, (i) => { if (dict[keys[i]] != i) throw new Exception(); });
            Parallel.For(0, 10, (i) => { if (!dict.Remove(keys[i])) throw new Exception(); });

            Parallel.For(0, 100, (i) => dict[keys[i]] = i);
            Parallel.For(0, 100, (i) => { if (dict[keys[i]] != i) throw new Exception(); });
            Parallel.For(0, 100, (i) => { if (!dict.Remove(keys[i])) throw new Exception(); });

            Parallel.For(0, 1000, (i) => dict.Add(keys[i], i));
            Parallel.For(0, 1000, (i) => dict[keys[i]] = i);
            Parallel.For(0, 1000, (i) => { if (dict[keys[i]] != i) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (!dict.Remove(keys[i])) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (dict.Remove(keys[i])) throw new Exception(); });

            Parallel.For(0, 10000, (i) => dict.Add(keys[i], i));
            Parallel.For(0, 10000, (i) => { if (dict[keys[i]] != i) throw new Exception(); });
            Parallel.For(0, 10000, (i) => { if (!dict.Remove(keys[i])) throw new Exception(); });

            Parallel.For(0, 100000, (i) => dict[keys[i]] = i);
            Parallel.For(0, 100000, (i) => { if (dict[keys[i]] != i) throw new Exception(); });
            Parallel.For(0, 100000, (i) => { if (!dict.Remove(keys[i])) throw new Exception(); });
        }

        [Fact()]
        private static void AddSetRemoveConcurrentInt()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, string>();

            Parallel.For(0, 10, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 10, (i) => dict[i] = i.ToString());
            Parallel.For(0, 10, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 10, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 100, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 100, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 1000, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 1000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 1000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 10000, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 10000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 10000, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 100000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 100000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
        }

        [Fact()]
        private static void AddSetRemoveConcurrentIntInt()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, int>();

            Parallel.For(0, 10, (i) => dict.Add(i, i));
            Parallel.For(0, 10, (i) => dict[i] = i);
            Parallel.For(0, 10, (i) => { if (dict[i] != i) throw new Exception(); });
            Parallel.For(0, 10, (i) => { int ii;  if (!dict.TryRemove(i, out ii)) throw new Exception(); });

            Parallel.For(0, 100, (i) => dict[i] = i);
            Parallel.For(0, 100, (i) => { if (dict[i] != i) throw new Exception(); });
            Parallel.For(0, 100, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 1000, (i) => dict.Add(i, i));
            Parallel.For(0, 1000, (i) => dict[i] = i);
            Parallel.For(0, 1000, (i) => { if (dict[i] != i) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 10000, (i) => dict.Add(i, i));
            Parallel.For(0, 10000, (i) => { if (dict[i] != i) throw new Exception(); });
            Parallel.For(0, 10000, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 100000, (i) => dict[i] = i);
            Parallel.For(0, 100000, (i) => { if (dict[i] != i) throw new Exception(); });
            Parallel.For(0, 100000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
        }

        [Fact()]
        private static void AddSetRemoveConcurrentUIntInt()
        {
            var dict = new NonBlocking.ConcurrentDictionary<uint, int>();

            Parallel.For(0, 10, (i) => dict.Add((uint)i, i));
            Parallel.For(0, 10, (i) => dict[(uint)i] = i);
            Parallel.For(0, 10, (i) => { if (dict[(uint)i] != i) throw new Exception(); });
            Parallel.For(0, 10, (i) => { int ii; if (!dict.TryRemove((uint)i, out ii)) throw new Exception(); });

            Parallel.For(0, 100, (i) => dict[(uint)i] = i);
            Parallel.For(0, 100, (i) => { if (dict[(uint)i] != i) throw new Exception(); });
            Parallel.For(0, 100, (i) => { if (!dict.Remove((uint)i)) throw new Exception(); });

            Parallel.For(0, 1000, (i) => dict.Add((uint)i, i));
            Parallel.For(0, 1000, (i) => dict[(uint)i] = i);
            Parallel.For(0, 1000, (i) => { if (dict[(uint)i] != i) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (!dict.Remove((uint)i)) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (dict.Remove((uint)i)) throw new Exception(); });

            Parallel.For(0, 10000, (i) => dict.Add((uint)i, i));
            Parallel.For(0, 10000, (i) => { if (dict[(uint)i] != i) throw new Exception(); });
            Parallel.For(0, 10000, (i) => { if (!dict.Remove((uint)i)) throw new Exception(); });

            Parallel.For(0, 100000, (i) => dict[(uint)i] = i);
            Parallel.For(0, 100000, (i) => { if (dict[(uint)i] != i) throw new Exception(); });
            Parallel.For(0, 100000, (i) => { if (!dict.Remove((uint)i)) throw new Exception(); });
        }

        [Fact()]
        private static void AddSetRemoveConcurrentLongInt()
        {
            var dict = new NonBlocking.ConcurrentDictionary<long, long>();

            Parallel.For(0, 10, (i) => dict.Add((long)i, i));
            Parallel.For(0, 10, (i) => dict[(long)i] = i);
            Parallel.For(0, 10, (i) => { if (dict[(long)i] != i) throw new Exception(); });
            Parallel.For(0, 10, (i) => { long ii; if (!dict.TryRemove((long)i, out ii)) throw new Exception(); });

            Parallel.For(0, 100, (i) => dict[(long)i] = i);
            Parallel.For(0, 100, (i) => { if (dict[(long)i] != i) throw new Exception(); });
            Parallel.For(0, 100, (i) => { if (!dict.Remove((long)i)) throw new Exception(); });

            Parallel.For(0, 1000, (i) => dict.Add((long)i, i));
            Parallel.For(0, 1000, (i) => dict[(long)i] = i);
            Parallel.For(0, 1000, (i) => { if (dict[(long)i] != i) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (!dict.Remove((long)i)) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (dict.Remove((long)i)) throw new Exception(); });

            Parallel.For(0, 10000, (i) => dict.Add((long)i, i));
            Parallel.For(0, 10000, (i) => { if (dict[(long)i] != i) throw new Exception(); });
            Parallel.For(0, 10000, (i) => { if (!dict.Remove((long)i)) throw new Exception(); });

            Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => dict[(long)i] = i);
            Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => { if (dict[(long)i] != i) throw new Exception(); });
            Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => { if (!dict.Remove((long)i)) throw new Exception(); });
        }

        [Fact()]
        private static void AddSetRemoveConcurrentULongInt()
        {
            var dict = new NonBlocking.ConcurrentDictionary<ulong, long>();

            Parallel.For(0, 10, (i) => dict.Add((ulong)i, i));
            Parallel.For(0, 10, (i) => dict[(ulong)i] = i);
            Parallel.For(0, 10, (i) => { if (dict[(ulong)i] != i) throw new Exception(); });
            Parallel.For(0, 10, (i) => { long ii; if (!dict.TryRemove((ulong)i, out ii)) throw new Exception(); });

            Parallel.For(0, 100, (i) => dict[(ulong)i] = i);
            Parallel.For(0, 100, (i) => { if (dict[(ulong)i] != i) throw new Exception(); });
            Parallel.For(0, 100, (i) => { if (!dict.Remove((ulong)i)) throw new Exception(); });

            Parallel.For(0, 1000, (i) => dict.Add((ulong)i, i));
            Parallel.For(0, 1000, (i) => dict[(ulong)i] = i);
            Parallel.For(0, 1000, (i) => { if (dict[(ulong)i] != i) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (!dict.Remove((ulong)i)) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (dict.Remove((ulong)i)) throw new Exception(); });

            Parallel.For(0, 10000, (i) => dict.Add((ulong)i, i));
            Parallel.For(0, 10000, (i) => { if (dict[(ulong)i] != i) throw new Exception(); });
            Parallel.For(0, 10000, (i) => { if (!dict.Remove((ulong)i)) throw new Exception(); });

            Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => dict[(ulong)i] = i);
            Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => { if (dict[(ulong)i] != i) throw new Exception(); });
            Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => { if (!dict.Remove((ulong)i)) throw new Exception(); });
        }

        [Fact()]
        private static void AddSetRemoveConcurrentIntPtrInt()
        {
            if (IntPtr.Size == 8)
            {
                var dict = new NonBlocking.ConcurrentDictionary<IntPtr, long>();

                Parallel.For(0, 10, (i) => dict.Add((IntPtr)i, i));
                Parallel.For(0, 10, (i) => dict[(IntPtr)i] = i);
                Parallel.For(0, 10, (i) => { if (dict[(IntPtr)i] != i) throw new Exception(); });
                Parallel.For(0, 10, (i) => { long ii; if (!dict.TryRemove((IntPtr)i, out ii)) throw new Exception(); });

                Parallel.For(0, 100, (i) => dict[(IntPtr)i] = i);
                Parallel.For(0, 100, (i) => { if (dict[(IntPtr)i] != i) throw new Exception(); });
                Parallel.For(0, 100, (i) => { if (!dict.Remove((IntPtr)i)) throw new Exception(); });

                Parallel.For(0, 1000, (i) => dict.Add((IntPtr)i, i));
                Parallel.For(0, 1000, (i) => dict[(IntPtr)i] = i);
                Parallel.For(0, 1000, (i) => { if (dict[(IntPtr)i] != i) throw new Exception(); });
                Parallel.For(0, 1000, (i) => { if (!dict.Remove((IntPtr)i)) throw new Exception(); });
                Parallel.For(0, 1000, (i) => { if (dict.Remove((IntPtr)i)) throw new Exception(); });

                Parallel.For(0, 10000, (i) => dict.Add((IntPtr)i, i));
                Parallel.For(0, 10000, (i) => { if (dict[(IntPtr)i] != i) throw new Exception(); });
                Parallel.For(0, 10000, (i) => { if (!dict.Remove((IntPtr)i)) throw new Exception(); });

                Parallel.For(0, 100000, (i) => dict[(IntPtr)i] = i);
                Parallel.For(0, 100000, (i) => { if (dict[(IntPtr)i] != i) throw new Exception(); });
                Parallel.For(0, 100000, (i) => { if (!dict.Remove((IntPtr)i)) throw new Exception(); });

                Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => dict[(IntPtr)i] = i);
                Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => { if (dict[(IntPtr)i] != i) throw new Exception(); });
                Parallel.For((long)int.MaxValue + 1L, (long)int.MaxValue + 100000, (i) => { if (!dict.Remove((IntPtr)i)) throw new Exception(); });
            }
        }

        struct S1
        {
            private int i;

            public static implicit operator S1(int x)
            {
                return new S1() { i = x };
            }

            public override string ToString()
            {
                return i.ToString();
            }

            public class Comparer : IEqualityComparer<S1>
            {
                bool IEqualityComparer<S1>.Equals(S1 x, S1 y)
                {
                    return x.i == y.i;
                }

                int IEqualityComparer<S1>.GetHashCode(S1 obj)
                {
                    return obj.i;
                }
            }
        }

        [Fact()]
        private static void AddSetRemoveConcurrentStruct()
        {
            var dict = new NonBlocking.ConcurrentDictionary<S1, string>(new S1.Comparer());

            Parallel.For(0, 10, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 10, (i) => dict[i] = i.ToString());
            Parallel.For(0, 10, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 10, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 100, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 100, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 1000, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 1000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 1000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
            Parallel.For(0, 1000, (i) => { if (dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 10000, (i) => dict.Add(i, i.ToString()));
            Parallel.For(0, 10000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 10000, (i) => { if (!dict.Remove(i)) throw new Exception(); });

            Parallel.For(0, 100000, (i) => dict[i] = i.ToString());
            Parallel.For(0, 100000, (i) => { if (dict[i] != i.ToString()) throw new Exception(); });
            Parallel.For(0, 100000, (i) => { if (!dict.Remove(i)) throw new Exception(); });
        }

        [Fact()]
        private static void Continuity001()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, int>();

            Parallel.ForEach(Enumerable.Range(0, 10000),
                (i) =>
                {
                    dict[i] = 0;
                    if (i % 2 == 0)
                    {
                        // increment slot
                        for (int j = 0; j < 10000; j++)
                        {
                            dict[i] += 1;
                        }

                        if (dict[i] != 10000) throw new Exception();
                    }
                    else
                    {
                        //add more slots
                        dict[i] = i;
                    }
                });

            Parallel.ForEach(Enumerable.Range(0, 10000),
                    (i) =>
                    {
                        if (i % 2 == 0)
                        {
                            // increment slot
                            for (int j = 0; j < 10000; j++)
                            {
                                dict[i] += 1;
                            }
                        }
                        else
                        {
                            //add more slots
                            dict[i * 10000] = i;
                        }
                    });

            Parallel.For(0, 1000,
                (i) =>
                {
                    if (i % 2 == 0 && dict[i] != 20000) throw new Exception();
                });

        }

        [Fact()]
        private static void Continuity002()
        {
            var dict = new NonBlocking.ConcurrentDictionary<object, int>();

            Parallel.ForEach(Enumerable.Range(0, 10000),
                (i) =>
                {
                    dict[i] = 0;
                    if (i % 2 == 0)
                    {
                        // increment slot
                        for (int j = 0; j < 10000; j++)
                        {
                            dict[i] += 1;
                        }

                        if (dict[i] != 10000) throw new Exception();
                    }
                    else
                    {
                        //add more slots
                        dict[i] = i;
                    }
                });

            Parallel.ForEach(Enumerable.Range(0, 10000),
                    (i) =>
                    {
                        if (i % 2 == 0)
                        {
                            // increment slot
                            for (int j = 0; j < 10000; j++)
                            {
                                dict[i] += 1;
                            }
                        }
                        else
                        {
                            //add more slots
                            dict[i * 10000] = i;
                        }
                    });

            Parallel.For(0, 1000,
                (i) =>
                {
                    if (i % 2 == 0 && dict[i] != 20000) throw new Exception();
                });

        }

        [Fact()]
        private static void ContinuityOfRemove001()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, int>();

            Parallel.ForEach(Enumerable.Range(0, 10000),
                (i) =>
                {
                    int val;
                    int d;

                    if (i % 2 == 0)
                    {
                        // flip/flop slot
                        for (int j = 0; j < 9999; j++)
                        {
                            if (dict.TryGetValue(i, out val))
                            {
                               Assert.True(dict.TryRemove(i, out d));
                               Assert.Equal(d, i);
                            }
                            else
                            {
                               Assert.True(dict.TryAdd(i, i));
                            }
                        }

                        if (!dict.TryGetValue(i, out val))
                            throw new Exception();
                    }
                    else
                    {
                        //add more slots
                        dict[i] = i;
                    }
                });

            Parallel.ForEach(Enumerable.Range(0, 10000),
                    (i) =>
                    {
                        int val;
                        int d;
                        if (i % 2 == 0)
                        {
                            // flip/flop slot
                            for (int j = 0; j < 9999; j++)
                            {
                                if (dict.TryGetValue(i, out val))
                                {
                                    Assert.True(dict.TryRemove(i, out d));
                                    Assert.Equal(d, i);
                                }
                                else
                                {
                                    Assert.True(dict.TryAdd(i, i));
                                }
                            }
                        }
                        else
                        {
                            //add more slots
                            dict[i * 10000] = i;
                        }
                    });

            Parallel.For(0, 1000,
                    (i) =>
                    {
                        int val;
                        if (i % 2 == 0 && dict.TryGetValue(i, out val)) throw new Exception();
                    });
        }

        [Fact()]
        private static void ContinuityOfRemove002()
        {
            var dict = new NonBlocking.ConcurrentDictionary<object, int>();

            Parallel.ForEach(Enumerable.Range(0, 10000),
                (i) =>
                {
                    int val;
                    int d;
                    if (i % 2 == 0)
                    {
                        // flip/flop slot
                        for (int j = 0; j < 9999; j++)
                        {
                            if (dict.TryGetValue(i, out val))
                            {
                                Assert.True(dict.TryRemove(i, out d));
                                Assert.Equal(d, i);
                            }
                            else
                            {
                                Assert.True(dict.TryAdd(i, i));
                            }
                        }

                        if (!dict.TryGetValue(i, out val))
                            throw new Exception();
                    }
                    else
                    {
                        //add more slots
                        dict[i] = i;
                    }
                });

            Parallel.ForEach(Enumerable.Range(0, 10000),
                    (i) =>
                    {
                        int val;
                        int d;
                        if (i % 2 == 0)
                        {
                            // flip/flop slot
                            for (int j = 0; j < 9999; j++)
                            {
                                if (dict.TryGetValue(i, out val))
                                {
                                    Assert.True(dict.TryRemove(i, out d));
                                    Assert.Equal(d, i);
                                }
                                else
                                {
                                    Assert.True(dict.TryAdd(i, i));
                                }
                            }
                        }
                        else
                        {
                            //add more slots
                            dict[i * 10000] = i;
                        }
                    });

            Parallel.For(0, 1000,
                    (i) =>
                    {
                        int val;
                        if (i % 2 == 0 && dict.TryGetValue(i, out val)) throw new Exception();
                    });
        }

        [Fact()]
        private static void Relativity001()
        {
            var dict = new NonBlocking.ConcurrentDictionary<int, int>();

            Parallel.ForEach(Enumerable.Range(0, 10001),
                (i) =>
                {
                    if (i % 2 == 0)
                    {
                        // maintain "dict[i] < dict[i+1]"
                        for (int j = 0; j < 10000; j++)
                        {
                            dict[i + 1] = j + 1;
                            dict[i] = j;
                        }
                    }
                    else
                    {
                        for (int j = 0; j < 10000; j++)
                        {
                            int first;
                            int second;
                            if (dict.TryGetValue(i - 1, out first))
                            {
                                if (dict.TryGetValue(i, out second))
                                {
                                    if (first >= second)
                                    {
                                        throw new Exception("value relation is incorrect");
                                    }
                                }
                                else
                                {
                                    throw new Exception("value must exist");
                                }
                            }
                        }
                    }

                    // just add an item
                    dict[10000 + i] = 0;
                });
        }

        [Fact()]
        private static void Relativity002()
        {
            var dict = new NonBlocking.ConcurrentDictionary<object, int>();

            Parallel.ForEach(Enumerable.Range(0, 10001),
                (i) =>
                {
                    if (i % 2 == 0)
                    {
                        // maintain "dict[i] < dict[i+1]"
                        for (int j = 0; j < 10000; j++)
                        {
                            dict[i + 1] = j + 1;
                            dict[i] = j;
                        }
                    }
                    else
                    {
                        for (int j = 0; j < 10000; j++)
                        {
                            int first;
                            int second;
                            if (dict.TryGetValue(i - 1, out first))
                            {
                                if (dict.TryGetValue(i, out second))
                                {
                                    if (first >= second)
                                    {
                                        throw new Exception("value relation is incorrect");
                                    }
                                }
                                else
                                {
                                    throw new Exception("value must exist");
                                }
                            }
                        }
                    }

                    // just add an item
                    dict[10000 + i] = 0;
                });
        }

        [Fact()]
        private static void Relativity003()
        {
            var dict = new NonBlocking.ConcurrentDictionary<S1, int>(new S1.Comparer());

            Parallel.ForEach(Enumerable.Range(0, 10001),
                (i) =>
                {
                    if (i % 2 == 0)
                    {
                        // maintain "dict[i] < dict[i+1]"
                        for (int j = 0; j < 10000; j++)
                        {
                            dict[i + 1] = j + 1;
                            dict[i] = j;
                        }
                    }
                    else
                    {
                        for (int j = 0; j < 10000; j++)
                        {
                            int first;
                            int second;
                            if (dict.TryGetValue(i - 1, out first))
                            {
                                if (dict.TryGetValue(i, out second))
                                {
                                    if (first >= second)
                                    {
                                        throw new Exception("value relation is incorrect");
                                    }
                                }
                                else
                                {
                                    throw new Exception("value must exist");
                                }
                            }
                        }
                    }

                    // just add an item
                    dict[10000 + i] = 0;
                });
        }

        [Fact()]
        private static void Relativity004()
        {
            var dict = new NonBlocking.ConcurrentDictionary<string, int>(new NullIntolerantComparer());

            var keys = new string[30000];
            for (int i = 0; i < keys.Length; i++) keys[i] = i.ToString();

            Parallel.ForEach(Enumerable.Range(0, 10001),
                (i) =>
                {
                    if (i % 2 == 0)
                    {
                        // maintain "dict[i] < dict[i+1]"
                        for (int j = 0; j < 10000; j++)
                        {
                            dict[keys[i + 1]] = j + 1;
                            dict[keys[i]] = j;
                        }
                    }
                    else
                    {
                        for (int j = 0; j < 10000; j++)
                        {
                            int first;
                            int second;
                            if (dict.TryGetValue(keys[i - 1], out first))
                            {
                                if (dict.TryGetValue(keys[i], out second))
                                {
                                    if (first >= second)
                                    {
                                        throw new Exception("value relation is incorrect");
                                    }
                                }
                                else
                                {
                                    throw new Exception("value must exist");
                                }
                            }
                        }
                    }

                    // just add an item
                    dict[keys[10000 + i]] = 0;
                });
        }

        class BadHash
        {
            public override int GetHashCode()
            {
                return 1;
            }
        }

        [Fact()]
        private static void BadHashAdd()
        {
            var dict = new NonBlocking.ConcurrentDictionary<BadHash, int>();

            for(int i = 0; i < 10000; i++)
            {
                var o = new BadHash();
                dict.TryAdd(o, i);
                Assert.Equal(i, dict[o]);
            }
        }
    }
}
