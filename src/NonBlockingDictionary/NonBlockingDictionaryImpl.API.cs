// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Threading;

namespace NonBlocking
{
    internal  abstract partial class NonBlockingDictionary<TKey, TKeyStore, TValue>
        : NonBlockingDictionary<TKey, TValue>,
        IEnumerable,
        IDictionary
    {
        public override int Count
        {
            get
            {
                return this.GetTableInfo(this._topTable).Size;
            }
        }

        public sealed override void Clear()
        {
            var newTable = CreateNew()._topTable;
            Volatile.Write(ref _topTable, newTable);
        }

        public override void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            if (array == null)
            {
                throw new ArgumentNullException("array");
            }
            if (arrayIndex < 0)
            {
                throw new ArgumentOutOfRangeException("index");
            }

            foreach(var entry in this)
            {
                array[arrayIndex++] = entry;
            }
        }

        public override void CopyTo(DictionaryEntry[] array, int arrayIndex)
        {
            if (array == null)
            {
                throw new ArgumentNullException("array");
            }
            if (arrayIndex < 0)
            {
                throw new ArgumentOutOfRangeException("index");
            }

            foreach (var entry in this)
            {
                array[arrayIndex++] = new DictionaryEntry(entry.Key, entry.Value);
            }
        }

        public override void CopyTo(object[] array, int arrayIndex)
        {
            if (array == null)
            {
                throw new ArgumentNullException("array");
            }
            if (arrayIndex < 0)
            {
                throw new ArgumentOutOfRangeException("index");
            }

            var length = array.Length;
            foreach (var entry in this)
            {
                if ((uint)arrayIndex < (uint)length)
                {
                    array[arrayIndex++] = entry;
                }
                else
                {
                    throw new ArgumentException();
                }
            }
        }

        public override IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new SnapshotKV(this);
        }

        public override ReadOnlyCollection<TKey> Keys
        {
            get
            {
                var keys = new List<TKey>(Count);
                foreach (var kv in this)
                {
                    keys.Add(kv.Key);
                }

                return new ReadOnlyCollection<TKey>(keys);
            }
        }

        public override ReadOnlyCollection<TValue> Values
        {
            get
            {
                var values = new List<TValue>(Count);
                foreach (var kv in this)
                {
                    values.Add(kv.Value);
                }

                return new ReadOnlyCollection<TValue>(values);
            }
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new SnapshotKV(this);
        }

        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            return new SnapshotIDict(this);
        }

        private class Snapshot : IDisposable
        {
            private readonly NonBlockingDictionary<TKey, TKeyStore, TValue> dict;
            private readonly Entry[] _table;
            private int _idx;              
            protected TKey _curKey, _nextK;
            protected object _curValue, _nextV;

            public Snapshot(NonBlockingDictionary<TKey, TKeyStore, TValue> dict)
            {
                // linearization point.
                // if table is quiescent and has no copy in progress,
                // we can simply iterate over its table.
                while (true)
                {
                    this.dict = dict;
                    var table = dict._topTable;
                    var tableInfo = dict.GetTableInfo(table);
                    if (tableInfo._newTable == null)
                    {
                        this._table = table;
                        break;
                    }

                    // there is a copy in progress, finish it and try again
                    tableInfo.HelpCopyImpl(dict, table, copy_all: true);
                }

                // Warm-up the iterator
                MoveNext();
            }

            public bool MoveNext()
            {
                if (_nextV == NULLVALUE)
                {
                    return false;
                }

                _curKey = _nextK;
                _curValue = _nextV;
                _nextV = NULLVALUE;

                while (_idx < _table.Length - 1)
                {  // Scan array
                    var nextEntry = _table[_idx++];

                    if (nextEntry.value != null)
                    {
                        var nextK = dict.keyFromEntry(nextEntry.key);

                        object nextV = dict.tryGetValue(nextK);
                        if (nextV != null)
                        {
                            _nextK = nextK;

                            // PERF: this would be nice to have as a helper, 
                            // but it does not get inlined
                            if (default(TValue) == null && nextV == NULLVALUE)
                            {
                                _nextV = default(TValue);
                            }
                            else
                            {
                                _nextV = (TValue)nextV;
                            }


                            break;
                        }
                    }
                }

                return _curValue != NULLVALUE;
            }

            public void Reset()
            {
                _idx = 0;
            }

            public void Dispose()
            {
            }
        }

        private sealed class SnapshotKV : Snapshot, IEnumerator<KeyValuePair<TKey, TValue>>
        {
            public SnapshotKV(NonBlockingDictionary<TKey, TKeyStore, TValue> dict) : base(dict) { }

            public KeyValuePair<TKey, TValue> Current
            {
                get
                {
                    var curValue = this._curValue;
                    if (curValue == NULLVALUE)
                    {
                        throw new InvalidOperationException();
                    }

                    return new KeyValuePair<TKey, TValue>(this._curKey, (TValue)curValue);
                }
            }

            object IEnumerator.Current => Current;
        }

        private sealed class SnapshotIDict : Snapshot, IDictionaryEnumerator
        {
            public SnapshotIDict(NonBlockingDictionary<TKey, TKeyStore, TValue> dict) : base(dict) { }

            public DictionaryEntry Entry
            {
                get
                {
                    var curValue = this._curValue;
                    if (curValue == NULLVALUE)
                    {
                        throw new InvalidOperationException();
                    }

                    return new DictionaryEntry(this._curKey, (TValue)curValue);
                }
            }

            public object Key
            {
                get
                {
                    return Entry.Key;
                }
            }

            public object Value
            {
                get
                {
                    return Entry.Value;
                }
            }

            object IEnumerator.Current => Entry;
        }
    }
}
