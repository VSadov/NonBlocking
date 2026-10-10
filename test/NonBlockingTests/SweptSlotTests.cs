// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections.Generic;
using System.Reflection;
using Xunit;

namespace NonBlockingTests
{
    // The sweeper retires a removed key's slot in steps: it CASes the value from TOMBSTONE to
    // TOMBPRIME, then clears the hash and the key. A thread that reaches the slot between those
    // steps sees TOMBPRIME with no resize in progress. The window is too narrow to hit reliably
    // under load, so these tests build that state directly through reflection.
    public class SweptSlotTests
    {
        private const BindingFlags Any = BindingFlags.Instance | BindingFlags.Static | BindingFlags.Public | BindingFlags.NonPublic;
        private const string Key = "k";

        // A thread copying the key into a new table lost the race: another thread copied it,
        // then the key was removed and the sweeper started retiring the slot.
        [Fact]
        public static void TableCopyIntoSweptSlotDoesNotThrowOrResurrect()
        {
            var d = new NonBlocking.ConcurrentDictionary<string, object>();
            var (table, hash) = SweepSlot(d);

            var copy = FindMethod(table.GetType(), "PutSlotCopy");
            bool copied = (bool)copy.Invoke(table, new object[] { Key, "stale", hash });

            Assert.False(copied);
            Assert.False(d.ContainsKey(Key));
            Assert.Equal(0, d.Count);
        }

        [Fact]
        public static void GetOrAddWithFactoryOnSweptSlotAddsTheKey()
        {
            var d = new NonBlocking.ConcurrentDictionary<string, object>();
            SweepSlot(d);

            Assert.Equal("new", d.GetOrAdd(Key, _ => "new"));
            Assert.Equal("new", d[Key]);
        }

        [Fact]
        public static void OtherOperationsOnSweptSlot()
        {
            var checks = new List<(string Name, Func<NonBlocking.ConcurrentDictionary<string, object>, bool> Ok)>
            {
                ("GetOrAdd(key, value)", d => Equals(d.GetOrAdd(Key, "new"), "new")),
                ("TryAdd", d => d.TryAdd(Key, "new") && Equals(d[Key], "new")),
                ("indexer set", d => { d[Key] = "new"; return Equals(d[Key], "new"); }),
                ("AddOrUpdate", d => Equals(d.AddOrUpdate(Key, "new", (_, _) => "updated"), "new")),
                ("TryUpdate", d => !d.TryUpdate(Key, "new", "old")),
                ("TryGetValue", d => !d.TryGetValue(Key, out _)),
                ("TryRemove", d => !d.TryRemove(Key, out _)),
                ("TryRemove(pair)", d => !d.TryRemove(new KeyValuePair<string, object>(Key, "old"))),
                ("enumerate", d => { foreach (var _ in d) { return false; } return d.Count == 0; }),
            };

            foreach (var (name, ok) in checks)
            {
                var d = new NonBlocking.ConcurrentDictionary<string, object>();
                SweepSlot(d);
                Assert.True(ok(d), name);
            }
        }

        // Adds and removes the key, then leaves its slot as the sweeper does after its CAS:
        // value TOMBPRIME, key and hash still set, no new table.
        private static (object Table, int Hash) SweepSlot(NonBlocking.ConcurrentDictionary<string, object> d)
        {
            // A removal arms the real sweeper, which runs after the next garbage collection and
            // would clear the slot under the test. A pending request keeps SweepCheck from arming it.
            FindField(d.GetType(), "_sweepRequests").SetValue(d, 1);

            d[Key] = "old";
            Assert.True(d.TryRemove(Key, out _));

            object table = FindField(d.GetType(), "_table").GetValue(d);
            var entries = (Array)FindField(table.GetType(), "_entries").GetValue(table);
            object tombprime = FindField(table.GetType(), "TOMBPRIME").GetValue(null);
            Assert.Null(FindField(table.GetType(), "_newTable").GetValue(table));

            for (int i = 0; i < entries.Length; i++)
            {
                // Entry is a struct: change a boxed copy and store it back.
                object entry = entries.GetValue(i);
                Type type = entry.GetType();
                if (Equals(type.GetField("key", Any).GetValue(entry), Key))
                {
                    type.GetField("value", Any).SetValue(entry, tombprime);
                    entries.SetValue(entry, i);
                    return (table, (int)type.GetField("hash", Any).GetValue(entry));
                }
            }

            throw new InvalidOperationException("The removed key's slot was not found.");
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
    }
}
