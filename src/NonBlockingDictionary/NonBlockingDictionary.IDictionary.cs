// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;

namespace NonBlocking
{
    internal  abstract partial class NonBlockingDictionary<TKey, TKeyStore, TValue>
        : NonBlockingDictionary<TKey, TValue>, 
        IDictionary<TKey, TValue>,
        IReadOnlyDictionary<TKey, TValue>
    {
        public int Count
        {
            get
            {
                return this.GetTableInfo(this._topTable).size();
            }
        }

        public bool IsReadOnly
        {
            get
            {
                return false;
            }
        }
        
        public bool ContainsKey(TKey key)
        {
            object value;
            return this.tryGetValue(key, out value);
        }

        void ICollection<KeyValuePair<TKey, TValue>>.Add(KeyValuePair<TKey, TValue> item)
        {
            this.Add(item.Key, item.Value);
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Contains(KeyValuePair<TKey, TValue> item)
        {
            object value;
            return this.tryGetValue(item.Key, out value) &&
                value.Equals(item.Value);
        }

        void ICollection<KeyValuePair<TKey, TValue>>.CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        bool ICollection<KeyValuePair<TKey, TValue>>.Remove(KeyValuePair<TKey, TValue> item)
        {
            throw new NotImplementedException();
        }

        IEnumerator<KeyValuePair<TKey, TValue>> IEnumerable<KeyValuePair<TKey, TValue>>.GetEnumerator()
        {
            return new SnapshotKV(this);
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return new SnapshotKV(this);
        }

        public ICollection<TKey> Keys
        {
            get
            {
                var keys = new List<TKey>(Count);
                foreach(var kv in this)
                {
                    keys.Add(kv.Key);
                }

                return keys;
            }
        }

        public ICollection<TValue> Values
        {
            get
            {
                var values = new List<TValue>(Count);
                foreach (var kv in this)
                {
                    values.Add(kv.Value);
                }

                return values;
            }
        }

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys
        {
            get
            {
                return Keys;
            }
        }

        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values
        {
            get
            {
                return Values;
            }
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
                if (_idx != 0 && _nextV == null)
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
                        object nextV;
                        if (dict.tryGetValue(nextK, out nextV))
                        {
                            _nextK = nextK;
                            _nextV = nextV;
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
    }
}
