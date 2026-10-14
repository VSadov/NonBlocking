// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace NonBlockingTests
{
    public class DelayedCopyTests
    {
        private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;
        private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public static void DelayedCopyAfterSweepDoesNotResurrect(bool resizeAgain)
        {
            RunSchedule("blocker", "target", "old", "new", resizeAgain, sweep: true);
        }

        [Fact]
        public static void DelayedCopyAfterSweepSupportsBoxedKeys()
        {
            RunSchedule(new TestKey(1), new TestKey(2), "old", "new", resizeAgain: false, sweep: true);
        }

        [Fact]
        public static void DelayedCopyAfterSweepSupportsNullValues()
        {
            RunSchedule<string, object>("blocker", "target", null, "new", resizeAgain: false, sweep: true);
        }

        [Fact]
        public static void DelayedCopyAfterSweepSupportsAtomicValues()
        {
            RunSchedule("blocker", "target", 42, 43, resizeAgain: false, sweep: true);
        }

        [Fact]
        public static void DelayedCopyPreservesFreshReinsertion()
        {
            RunSchedule("blocker", "target", "old", "new", resizeAgain: false, sweep: true, reinsert: true);
        }

        [Fact]
        public static void DelayedCopyIntoUnsweptTombstoneRemainsAbsent()
        {
            // Control: the previous destination identity still exists, so even
            // an unguarded copy cannot publish through its non-null tombstone.
            RunSchedule("blocker", "target", "old", "new", resizeAgain: false, sweep: false);
        }

        [Fact]
        public static void RemovalHelpingCompletesSourceBeforeSweep()
        {
            RunSchedule("blocker", "target", "old", "new", resizeAgain: false, sweep: true,
                removeBeforePromotion: true);
        }

        private static void RunSchedule<TKey, TValue>(TKey blocker, TKey target, TValue oldValue, TValue newValue,
            bool resizeAgain, bool sweep, bool reinsert = false, bool removeBeforePromotion = false)
        {
            using var comparer = new PausingComparer<TKey>(blocker, target);
            var d = new NonBlocking.ConcurrentDictionary<TKey, TValue>(1, 8, comparer);
            // Keep removal from arming a GC-driven sweep. The test invokes the
            // real sweep itself after establishing the desired interleaving.
            FindField(d.GetType(), "_sweepRequests").SetValue(d, 1);
            d[blocker] = newValue;
            d[target] = oldValue;

            object source = Table(d);
            int sourceIndex = FindEntryIndex(source, target);
            object destination = Invoke(source, "Resize", false);
            Assert.NotSame(source, destination);

            // The constant hash routes to source index 1 at capacity 8. HelpCopy
            // copies the blocker before the target, so the target's destination
            // probe must compare with the blocker. Pause only this worker there.
            var delayed = Task.Run(() =>
            {
                comparer.ArmForCurrentThread();
                Invoke(source, "HelpCopy", false);
            });

            try
            {
                Assert.True(comparer.Paused.Wait(Timeout), "The delayed copy did not reach destination probing.");

                if (removeBeforePromotion)
                {
                    Assert.Same(source, Table(d));
                    Assert.True(d.TryRemove(target, out TValue removed));
                    Assert.Equal(oldValue, removed);
                    Assert.Same(FindField(source.GetType(), "TOMBPRIME").GetValue(null),
                        EntryField(source, sourceIndex, "value"));
                }

                // Another helper must finish promotion while the first remains
                // paused. This also covers HelpCopy's duplicate/panic path.
                Invoke(source, "HelpCopy", true);
                Assert.Same(destination, Table(d));
                Assert.Same(FindField(source.GetType(), "TOMBPRIME").GetValue(null),
                    EntryField(source, sourceIndex, "value"));
                if (!removeBeforePromotion)
                {
                    Assert.Equal(oldValue, d[target]);
                }

                if (resizeAgain)
                {
                    object next = Invoke(destination, "Resize", false);
                    Invoke(destination, "HelpCopy", true);
                    Assert.Same(next, Table(d));
                    destination = next;
                }

                if (!removeBeforePromotion)
                {
                    Assert.True(d.TryRemove(target, out TValue removed));
                    Assert.Equal(oldValue, removed);
                }
                Assert.False(d.TryGetValue(target, out _));
                int destinationIndex = FindEntryIndex(destination, target);

                if (sweep)
                {
                    Sweep(destination);
                    Assert.Equal(unchecked((int)0xc0000000), EntryField(destination, destinationIndex, "hash"));
                    Assert.Null(EntryField(destination, destinationIndex, "key"));
                    Assert.Same(FindField(destination.GetType(), "TOMBPRIME").GetValue(null),
                        EntryField(destination, destinationIndex, "value"));
                }

                if (reinsert)
                {
                    Assert.True(d.TryAdd(target, newValue));
                }
            }
            finally
            {
                comparer.Resume.Set();
                Assert.True(delayed.Wait(Timeout), "The delayed copy did not finish after being released.");
            }

            Assert.Equal(newValue, d[blocker]);
            if (reinsert)
            {
                Assert.Equal(newValue, d[target]);
                Assert.Equal(2, d.Count);
                Assert.Equal(2, d.ToArray().Length);
                FindField(d.GetType(), "_sweepRequests").SetValue(d, 1);
                Assert.True(d.TryRemove(target, out TValue removed));
                Assert.Equal(newValue, removed);
            }
            else
            {
                // PutSlotCopy does not increment Count, so lookup and enumeration
                // are required to catch a resurrected value with an unchanged count.
                Assert.False(d.TryGetValue(target, out _));
                Assert.False(d.TryRemove(target, out _));
            }

            Assert.Equal(1, d.Count);
            Assert.Equal(blocker, Assert.Single(d.ToArray()).Key);
        }

        private static object Table<TKey, TValue>(NonBlocking.ConcurrentDictionary<TKey, TValue> d)
            => FindField(d.GetType(), "_table").GetValue(d);

        private static object EntryField(object table, int index, string field)
        {
            var entries = (Array)FindField(table.GetType(), "_entries").GetValue(table);
            object entry = entries.GetValue(index);
            return entry.GetType().GetField(field, Any).GetValue(entry);
        }

        private static int FindEntryIndex<TKey>(object table, TKey key)
        {
            var entries = (Array)FindField(table.GetType(), "_entries").GetValue(table);
            for (int i = 0; i < entries.Length; i++)
            {
                object stored = EntryField(table, i, "key");
                if (stored == null)
                {
                    continue;
                }

                // Struct keys use Boxed<TKey> in the generic implementation.
                FieldInfo boxedValue = stored.GetType().GetField("Value", Any);
                object actual = boxedValue == null ? stored : boxedValue.GetValue(stored);
                if (Equals(actual, key))
                {
                    return i;
                }
            }

            throw new InvalidOperationException("The expected key was not found in the table.");
        }

        private static void Sweep(object table)
        {
            Type definition = FindField(table.GetType(), "_entries").DeclaringType;
            Type sweeperType = definition.GetNestedType("Sweeper", Any).MakeGenericType(definition.GetGenericArguments());
            object sweeper = Activator.CreateInstance(sweeperType, nonPublic: true);
            GC.SuppressFinalize(sweeper);
            FindField(sweeperType, "_dict").SetValue(sweeper, table);
            Invoke(sweeper, "Sweep");
        }

        private static object Invoke(object instance, string name, params object[] arguments)
            => FindMethod(instance.GetType(), name).Invoke(instance, arguments);

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

        private static MethodInfo FindMethod(Type type, string name)
        {
            for (; type != null; type = type.BaseType)
            {
                MethodInfo method = type.GetMethod(name, Any | BindingFlags.DeclaredOnly);
                if (method != null)
                {
                    return method;
                }
            }
            throw new MissingMethodException(name);
        }

        private readonly struct TestKey
        {
            // A reference-bearing struct uses boxed key storage and is eligible
            // for automatic sweeping on both library targets.
            private readonly string _id;
            public TestKey(int id) => _id = id.ToString();
        }

        private sealed class PausingComparer<TKey> : IEqualityComparer<TKey>, IDisposable
        {
            private readonly TKey _blocker;
            private readonly TKey _target;
            private int _thread;
            private int _armed;
            public readonly ManualResetEventSlim Paused = new ManualResetEventSlim();
            public readonly ManualResetEventSlim Resume = new ManualResetEventSlim();

            public PausingComparer(TKey blocker, TKey target)
            {
                _blocker = blocker;
                _target = target;
            }

            public void ArmForCurrentThread()
            {
                _thread = Environment.CurrentManagedThreadId;
                Volatile.Write(ref _armed, 1);
            }

            public bool Equals(TKey x, TKey y)
            {
                var equality = EqualityComparer<TKey>.Default;
                bool comparesBlocker = (equality.Equals(x, _target) && equality.Equals(y, _blocker)) ||
                    (equality.Equals(y, _target) && equality.Equals(x, _blocker));
                if (Environment.CurrentManagedThreadId == _thread && comparesBlocker &&
                    Interlocked.CompareExchange(ref _armed, 0, 1) == 1)
                {
                    Paused.Set();
                    if (!Resume.Wait(Timeout))
                    {
                        throw new TimeoutException("The delayed copy was not released.");
                    }
                }
                return equality.Equals(x, y);
            }

            public int GetHashCode(TKey key) => 1;

            public void Dispose()
            {
                Paused.Dispose();
                Resume.Dispose();
            }
        }
    }
}
