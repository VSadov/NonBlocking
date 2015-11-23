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
    internal  abstract partial class DictionaryImpl<TKey, TKeyStore, TValue>
        : DictionaryImpl<TKey, TValue>
    {

        internal override IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return new SnapshotKV(this);
        }

        internal override IDictionaryEnumerator GetdIDictEnumerator()
        {
            return new SnapshotIDict(this);
        }

        private class Snapshot : IDisposable
        {
            private readonly DictionaryImpl<TKey, TKeyStore, TValue> dict;
            private readonly Entry[] _table;
            private int _idx;              
            protected TKey _curKey, _nextK;
            protected object _curValue, _nextV;

            public Snapshot(DictionaryImpl<TKey, TKeyStore, TValue> dict)
            {
                // linearization point.
                // if table is quiescent and has no copy in progress,
                // we can simply iterate over its table.
                while (true)
                {
                    this.dict = dict;
                    var table = dict._table;
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

                        object nextV = dict.TryGetValue(nextK);
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
            public SnapshotKV(DictionaryImpl<TKey, TKeyStore, TValue> dict) : base(dict) { }

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
            public SnapshotIDict(DictionaryImpl<TKey, TKeyStore, TValue> dict) : base(dict) { }

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
