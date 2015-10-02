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
    class Program
    {
        static void Main(string[] args)
        {
            for (;;)
            {
                System.Console.WriteLine("AddSetRemove");
                AddSetRemove();
                System.Console.WriteLine("AddSetRemoveConcurrent");
                AddSetRemoveConcurrent();
                System.Console.WriteLine("AddSetRemoveConcurrentInt");
                AddSetRemoveConcurrentInt();
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

                System.Console.WriteLine("BadHashAdd");
                BadHashAdd();

                System.Console.WriteLine("=========== PASS");
                System.Console.WriteLine();
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
            var dict = NonBlockingDictionary.Create<int, string>();

            string s;
            Assert.False(dict.TryGetValue(0, out s));

            dict.Add(0, null);
            Assert.True(dict.TryGetValue(0, out s));
            Assert.Null(s);
            Assert.True(dict.Remove(0));
            Assert.False(dict.TryGetValue(0, out s));
        }

        [Fact()]
        private static void NullValueNub()
        {
            var dict = NonBlockingDictionary.Create<int, int?>();

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
            var dict = NonBlockingDictionary.Create<object, string>();

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
            var dict = NonBlockingDictionary.Create<object, string>();

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
        private static void AddSetRemoveConcurrentInt()
        {
            var dict = NonBlockingDictionary.Create<int, string>();

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
        }

        [Fact()]
        private static void AddSetRemoveConcurrentStruct()
        {
            var dict = NonBlockingDictionary.Create<S1, string>();

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
            var dict = NonBlockingDictionary.Create<int, int>();

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
            var dict = NonBlockingDictionary.Create<object, int>();

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
            var dict = NonBlockingDictionary.Create<int, int>();

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
            var dict = NonBlockingDictionary.Create<object, int>();

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
            var dict = NonBlockingDictionary.Create<int, int>();

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
            var dict = NonBlockingDictionary.Create<object, int>();

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
            var dict = NonBlockingDictionary.Create<BadHash, int>();

            for(int i = 0; i < 10000; i++)
            {
                var o = new BadHash();
                dict.TryAdd(o, i);
                Assert.Equal(i, dict[o]);
            }
        }
    }
}
