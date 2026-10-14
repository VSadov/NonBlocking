// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit;

namespace NonBlockingTests
{
    // 64-bit keys with the default comparer. A hash of lo ^ hi gives every key with equal halves
    // the same hash and packed pairs only a few, which made the table grow far past the key count.
    public class Hash64Tests
    {
        private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;

        [Fact]
        public static void KeysWithEqualHalvesKeepTheTableSmall()
        {
            var d = new NonBlocking.ConcurrentDictionary<long, int>();
            for (int i = 1; i <= 2048; i++)
            {
                Assert.True(d.TryAdd(((long)i << 32) | (uint)i, i));
            }

            AssertAllPresent(d, i => ((long)i << 32) | (uint)i, 2048);
            AssertTableAtMost(d, 16_384);
        }

        [Fact]
        public static void PackedPairsKeepTheTableSmall()
        {
            var d = new NonBlocking.ConcurrentDictionary<ulong, int>();
            for (int x = 0; x < 64; x++)
            {
                for (int y = 0; y < 64; y++)
                {
                    Assert.True(d.TryAdd(((ulong)x << 32) | (uint)y, x * 64 + y));
                }
            }

            for (int x = 0; x < 64; x++)
            {
                for (int y = 0; y < 64; y++)
                {
                    Assert.True(d.TryGetValue(((ulong)x << 32) | (uint)y, out int v));
                    Assert.Equal(x * 64 + y, v);
                }
            }

            AssertTableAtMost(d, 16_384);
        }

        [Fact]
        public static void NativeIntKeysWithEqualHalvesKeepTheTableSmall()
        {
            if (!Environment.Is64BitProcess)
            {
                return;
            }

            var d = new NonBlocking.ConcurrentDictionary<nint, int>();
            var u = new NonBlocking.ConcurrentDictionary<nuint, int>();
            for (int i = 1; i <= 2048; i++)
            {
                Assert.True(d.TryAdd((nint)(((long)i << 32) | (uint)i), i));
                Assert.True(u.TryAdd((nuint)(((ulong)i << 32) | (uint)i), i));
            }

            for (int i = 1; i <= 2048; i++)
            {
                Assert.True(d.TryGetValue((nint)(((long)i << 32) | (uint)i), out int v));
                Assert.Equal(i, v);
                Assert.True(u.TryGetValue((nuint)(((ulong)i << 32) | (uint)i), out v));
                Assert.Equal(i, v);
            }

            AssertTableAtMost(d, 16_384);
            AssertTableAtMost(u, 16_384);
        }

        [Fact]
        public static void ExplicitDefaultComparerKeepsTheTableSmall()
        {
            var d = new NonBlocking.ConcurrentDictionary<long, int>(EqualityComparer<long>.Default);
            var u = new NonBlocking.ConcurrentDictionary<ulong, int>(EqualityComparer<ulong>.Default);
            for (int i = 1; i <= 2048; i++)
            {
                Assert.True(d.TryAdd(((long)i << 32) | (uint)i, i));
                Assert.True(u.TryAdd(((ulong)i << 32) | (uint)i, i));
            }

            AssertAllPresent(d, i => ((long)i << 32) | (uint)i, 2048);
            for (int i = 1; i <= 2048; i++)
            {
                Assert.True(u.TryGetValue(((ulong)i << 32) | (uint)i, out int v));
                Assert.Equal(i, v);
            }
            Assert.Equal(2048, u.Count);
            AssertTableAtMost(d, 16_384);
            AssertTableAtMost(u, 16_384);
        }

        [Fact]
        public static void NativeIntExplicitDefaultComparerKeepsTheTableSmall()
        {
            if (!Environment.Is64BitProcess)
            {
                return;
            }

            var d = new NonBlocking.ConcurrentDictionary<nint, int>(EqualityComparer<nint>.Default);
            var u = new NonBlocking.ConcurrentDictionary<nuint, int>(EqualityComparer<nuint>.Default);
            for (int i = 1; i <= 2048; i++)
            {
                Assert.True(d.TryAdd((nint)(((long)i << 32) | (uint)i), i));
                Assert.True(u.TryAdd((nuint)(((ulong)i << 32) | (uint)i), i));
            }

            for (int i = 1; i <= 2048; i++)
            {
                Assert.True(d.TryGetValue((nint)(((long)i << 32) | (uint)i), out int v));
                Assert.Equal(i, v);
                Assert.True(u.TryGetValue((nuint)(((ulong)i << 32) | (uint)i), out v));
                Assert.Equal(i, v);
            }

            AssertTableAtMost(d, 16_384);
            AssertTableAtMost(u, 16_384);
        }

        // Keys that differ only in bits 62 and 63 differed only in the bits the tag overwrites, so
        // every such group shared one hash. Too few keys collide to force a resize, so compare the
        // hashes the table computes.
        [Fact]
        public static void KeysDifferingOnlyInTheTopBitsGetDistinctHashes()
        {
            var d = new NonBlocking.ConcurrentDictionary<long, int>();
            for (long b = 1; b <= 1024; b++)
            {
                var hashes = new HashSet<int>();
                for (long t = 0; t < 4; t++)
                {
                    long key = (b * 0x10001) | (t << 62);
                    Assert.True(d.TryAdd(key, (int)b));
                    hashes.Add(TableHash(d, key));
                }

                Assert.True(hashes.Count == 4, $"the four variants of {b} got only {hashes.Count} distinct hash(es)");
            }

            Assert.Equal(4096, d.Count);
        }

        // A comparer other than the default keeps its own equality and hash; the routing change only
        // concerns the default comparer.
        [Fact]
        public static void CustomComparerIsRespected()
        {
            AssertCustomComparer<long>(i => i, k => (ulong)k);
            AssertCustomComparer<ulong>(i => (ulong)i, k => k);
            AssertCustomComparer<nint>(i => (nint)i, k => (ulong)k);
            AssertCustomComparer<nuint>(i => (nuint)i, k => (ulong)k);
        }

        private static void AssertCustomComparer<TKey>(Func<int, TKey> key, Func<TKey, ulong> value)
            where TKey : notnull
        {
            var comparer = new ModuloComparer<TKey>(value);
            var d = new NonBlocking.ConcurrentDictionary<TKey, int>(comparer);
            Assert.Same(comparer, d.Comparer);
            Assert.True(d.TryAdd(key(5), 1));
            Assert.False(d.TryAdd(key(1005), 2));
            Assert.True(d.TryGetValue(key(2005), out int v));
            Assert.Equal(1, v);
            Assert.Equal(1, d.Count);

            object initialTable = FindField(d.GetType(), "_table").GetValue(d);
            // These distinct, nonzero remainders force a resize without equating any key to zero.
            for (int i = 1; i <= 512; i++)
            {
                if (i != 5)
                {
                    Assert.True(d.TryAdd(key(i), i));
                }
            }

            // Complete any pending copy and verify every entry through a comparer-equivalent key.
            for (int i = 1; i <= 512; i++)
            {
                Assert.True(d.TryGetValue(key(i + 1000), out v));
                Assert.Equal(i == 5 ? 1 : i, v);
            }
            Assert.NotSame(initialTable, FindField(d.GetType(), "_table").GetValue(d));
            Assert.Same(comparer, d.Comparer);
            int hashCalls = comparer.HashCalls;
            Assert.True(d.TryGetValue(key(2005), out v));
            Assert.Equal(1, v);
            Assert.True(comparer.HashCalls > hashCalls);
            Assert.True(d.TryUpdate(key(1005), 3, 1));
            Assert.True(d.TryRemove(key(2005), out v));
            Assert.Equal(3, v);
            Assert.False(d.ContainsKey(key(5)));
            Assert.True(d.TryAdd(key(1005), 4));
            Assert.True(d.TryGetValue(key(5), out v));
            Assert.Equal(4, v);

            d.Clear();
            Assert.Equal(0, d.Count);
            Assert.Same(comparer, d.Comparer);
            hashCalls = comparer.HashCalls;
            Assert.True(d.TryAdd(key(5), 6));
            Assert.False(d.TryAdd(key(1005), 7));
            Assert.True(d.TryGetValue(key(2005), out v));
            Assert.Equal(6, v);
            Assert.True(comparer.HashCalls > hashCalls);
            Assert.True(d.TryRemove(key(1005), out v));
            Assert.Equal(6, v);
            Assert.Equal(0, d.Count);
        }

        private sealed class ModuloComparer<TKey> : IEqualityComparer<TKey>
        {
            private readonly Func<TKey, ulong> _value;
            public int HashCalls;

            public ModuloComparer(Func<TKey, ulong> value) => _value = value;

            public bool Equals(TKey x, TKey y) => _value(x) % 1000 == _value(y) % 1000;

            public int GetHashCode(TKey key)
            {
                HashCalls++;
                return (int)(_value(key) % 1000);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public static void EdgeKeysRoundTrip(bool explicitDefaultComparer)
        {
            long[] keys = { 0, 1, -1, 2, -2, 1L << 30, 1L << 31, 1L << 32, -(1L << 32), 1L << 62, long.MinValue, long.MaxValue };
            AssertEdgeKeysRoundTrip(keys, explicitDefaultComparer);
            AssertEdgeKeysRoundTrip(Array.ConvertAll(keys, k => unchecked((ulong)k)), explicitDefaultComparer);
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public static void NativeIntEdgeKeysRoundTrip(bool explicitDefaultComparer)
        {
            // This test also runs on 32-bit processes; only use values representable at native width.
            long[] signed = Environment.Is64BitProcess
                ? new long[] { 0, 1, -1, 2, -2, 1L << 30, 1L << 31, 1L << 32, -(1L << 32), 1L << 62, long.MinValue, long.MaxValue }
                : new long[] { 0, 1, -1, 2, -2, 1L << 30, int.MinValue, int.MaxValue };
            ulong[] unsigned = Environment.Is64BitProcess
                ? new ulong[] { 0, 1, 2, 1UL << 30, 1UL << 31, 1UL << 32, 1UL << 62, 1UL << 63, ulong.MaxValue }
                : new ulong[] { 0, 1, 2, 1UL << 30, 1UL << 31, uint.MaxValue };
            AssertEdgeKeysRoundTrip(Array.ConvertAll(signed, k => (nint)k), explicitDefaultComparer);
            AssertEdgeKeysRoundTrip(Array.ConvertAll(unsigned, k => (nuint)k), explicitDefaultComparer);
        }

        private static void AssertEdgeKeysRoundTrip<TKey>(TKey[] keys, bool explicitDefaultComparer)
            where TKey : notnull
        {
            var d = explicitDefaultComparer
                ? new NonBlocking.ConcurrentDictionary<TKey, int>(EqualityComparer<TKey>.Default)
                : new NonBlocking.ConcurrentDictionary<TKey, int>();
            for (int i = 0; i < keys.Length; i++)
            {
                Assert.True(d.TryAdd(keys[i], i));
            }

            for (int i = 0; i < keys.Length; i++)
            {
                Assert.True(d.TryGetValue(keys[i], out int v));
                Assert.Equal(i, v);
                Assert.True(d.TryUpdate(keys[i], ~i, i));
                Assert.True(d.TryRemove(keys[i], out v));
                Assert.Equal(~i, v);
                Assert.False(d.ContainsKey(keys[i]));
                Assert.True(d.TryAdd(keys[i], ~i));
                Assert.True(d.TryGetValue(keys[i], out v));
                Assert.Equal(~i, v);
            }

            Assert.Equal(keys.Length, d.Count);
            // Later operations may have resized the table; recheck all earlier reinsertions.
            for (int i = 0; i < keys.Length; i++)
            {
                Assert.True(d.TryGetValue(keys[i], out int v));
                Assert.Equal(~i, v);
            }
        }

        // The hash the dictionary's current table computes for a key.
        private static int TableHash(object dictionary, long key)
        {
            object table = FindField(dictionary.GetType(), "_table").GetValue(dictionary);
            for (Type t = table.GetType(); t != null; t = t.BaseType)
            {
                MethodInfo hash = t.GetMethod("hash", Any | BindingFlags.DeclaredOnly, null, new[] { typeof(long) }, null);
                if (hash != null)
                {
                    return (int)hash.Invoke(table, new object[] { key });
                }
            }

            throw new MissingMethodException("hash");
        }

        private static void AssertAllPresent(NonBlocking.ConcurrentDictionary<long, int> d, Func<int, long> key, int count)
        {
            for (int i = 1; i <= count; i++)
            {
                Assert.True(d.TryGetValue(key(i), out int v));
                Assert.Equal(i, v);
            }

            Assert.Equal(count, d.Count);
        }

        private static void AssertTableAtMost(object dictionary, int slots)
        {
            // A resize publishes the new table only once its copy is done, so follow it to the newest.
            object table = FindField(dictionary.GetType(), "_table").GetValue(dictionary);
            for (object next; (next = FindField(table.GetType(), "_newTable").GetValue(table)) != null;)
            {
                table = next;
            }

            int length = ((Array)FindField(table.GetType(), "_entries").GetValue(table)).Length;
            Assert.True(length <= slots, $"the table has {length} slots, expected at most {slots}");
        }

        private static FieldInfo FindField(Type type, string name)
        {
            for (; type != null; type = type.BaseType)
            {
                FieldInfo field = type.GetField(name, Any | BindingFlags.DeclaredOnly);
                if (field != null)
                {
                    return field;
                }
            }

            throw new MissingFieldException(name);
        }
    }
}
