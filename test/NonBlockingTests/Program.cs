using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using NonBlocking;
using System.Collections.Concurrent;

namespace NonBlockingTests
{
    class Program
    {
        static void Main(string[] args)
        {
            AddSetRemove();
            AddSetRemoveConcurrent();
            AddSetRemoveConcurrentInt();
            AddSetRemoveConcurrentStruct();

            Continuity001();
            Continuity002();

            ContinuityOfRemove001();
            ContinuityOfRemove002();

            System.Console.WriteLine("PASS");
        }

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

        private static void ContinuityOfRemove001()
        {
            var dict = NonBlockingDictionary.Create<int, int>();

            Parallel.ForEach(Enumerable.Range(0, 10000),
                (i) =>
                {
                    int val;
                    if (i % 2 == 0)
                    {
                        // flip/flop slot
                        for (int j = 0; j < 9999; j++)
                        {
                            if (dict.TryGet(i, out val))
                            {
                                dict.Remove(i);
                            }
                            else
                            {
                                dict.Add(i, i);
                            }
                        }

                        if (!dict.TryGet(i, out val))
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
                        if (i % 2 == 0)
                        {
                            // flip/flop slot
                            for (int j = 0; j < 9999; j++)
                            {
                                if (dict.TryGet(i, out val))
                                {
                                    dict.Remove(i);
                                }
                                else
                                {
                                    dict.Add(i, i);
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
                        if (i % 2 == 0 && dict.TryGet(i, out val)) throw new Exception();
                    });
        }

        private static void ContinuityOfRemove002()
        {
            var dict = NonBlockingDictionary.Create<object, int>();

            Parallel.ForEach(Enumerable.Range(0, 10000),
                (i) =>
                {
                    int val;
                    if (i % 2 == 0)
                    {
                        // flip/flop slot
                        for (int j = 0; j < 9999; j++)
                        {
                            if (dict.TryGet(i, out val))
                            {
                                dict.Remove(i);
                            }
                            else
                            {
                                dict.Add(i, i);
                            }
                        }

                        if (!dict.TryGet(i, out val))
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
                        if (i % 2 == 0)
                        {
                            // flip/flop slot
                            for (int j = 0; j < 9999; j++)
                            {
                                if (dict.TryGet(i, out val))
                                {
                                    dict.Remove(i);
                                }
                                else
                                {
                                    dict.Add(i, i);
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
                        if (i % 2 == 0 && dict.TryGet(i, out val)) throw new Exception();
                    });
        }

    }
}
