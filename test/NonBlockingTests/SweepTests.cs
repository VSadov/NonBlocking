// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading;
using Xunit;

namespace NonBlockingTests
{
    // The sweeper runs after a garbage collection that follows a removal, and retires the removed
    // keys so the table stops referencing them.
    public class SweepTests
    {
        private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;

        // A key removed and added back while an enumeration is part-way through the table must
        // not be yielded a second time. Each round enumerates up to the key, removes it, gives the
        // sweeper several collections to retire its slot, adds it back and finishes enumerating.
        [Fact]
        public static void KeyReaddedDuringEnumerationIsNotEnumeratedTwice()
        {
            const int Keys = 8;
            var duplicates = new List<string>();

            for (int round = 0; round < Keys; round++)
            {
                var d = NewDictionary(Keys);
                string key = "k" + round;
                int yielded = 0;
                using var e = d.GetEnumerator();
                while (e.MoveNext())
                {
                    if (e.Current.Key == key)
                    {
                        yielded++;
                        break;
                    }
                }

                // The table is quiescent, so the first pass must reach the key.
                Assert.Equal(1, yielded);
                Assert.True(d.TryRemove(key, out _));
                WaitForSweep(d, key, attempts: 20);
                d[key] = "again";
                while (e.MoveNext())
                {
                    if (e.Current.Key == key)
                    {
                        yielded++;
                    }
                }

                if (yielded > 1)
                {
                    duplicates.Add(key);
                }
            }

            Assert.True(duplicates.Count == 0,
                $"{duplicates.Count} of {Keys} re-added keys were yielded twice: {string.Join(", ", duplicates)}");
        }

        // Postponing must not stop the sweeper for good: once the enumeration is disposed, or has
        // run to its end, the next collections retire the removed key.
        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public static void SweepRunsOnceEnumerationEnds(bool dispose)
        {
            // Open the enumeration first: a collection between the removal and GetEnumerator could
            // otherwise retire the slot legitimately, before the enumeration exists.
            var d = NewDictionary(16);
            var e = d.GetEnumerator();
            Assert.True(e.MoveNext());
            Assert.True(d.TryRemove("k3", out _));
            Assert.False(WaitForSweep(d, "k3", attempts: 10), "the sweeper retired a slot during an open enumeration");

            if (dispose)
            {
                e.Dispose();
            }
            else
            {
                while (e.MoveNext())
                {
                }
            }

            Assert.True(WaitForSweep(d, "k3", attempts: 100), "the sweeper did not run after the enumeration ended");
        }

        private static NonBlocking.ConcurrentDictionary<string, string> NewDictionary(int keys)
        {
            var d = new NonBlocking.ConcurrentDictionary<string, string>();
            for (int i = 0; i < keys; i++)
            {
                d["k" + i] = "v";
            }

            return d;
        }

        // The sweeper is armed by the removal and runs from a finalizer, so collect until the
        // current table no longer references the key, or give up.
        private static bool WaitForSweep(object dictionary, string key, int attempts)
        {
            for (int i = 0; i < attempts; i++)
            {
                if (!CurrentTableHolds(dictionary, key))
                {
                    return true;
                }

                GC.Collect();
                GC.WaitForPendingFinalizers();
                Thread.Sleep(10);
            }

            return false;
        }

        private static bool CurrentTableHolds(object dictionary, string key)
        {
            object table = FindField(dictionary.GetType(), "_table").GetValue(dictionary);
            var entries = (Array)FindField(table.GetType(), "_entries").GetValue(table);
            for (int i = 0; i < entries.Length; i++)
            {
                object entry = entries.GetValue(i);
                if (Equals(entry.GetType().GetField("key", Any).GetValue(entry), key))
                {
                    return true;
                }
            }

            return false;
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
