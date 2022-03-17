// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

#nullable disable

using System.Runtime.CompilerServices;

namespace NonBlocking
{
    internal abstract partial class DictionaryImpl<TKey, TKeyStore, TValue>
        : DictionaryImpl<TKey, TValue>
    {
        internal override Snapshot GetSnapshot()
        {
            return new SnapshotImpl(this);
        }

        private class SnapshotImpl : Snapshot
        {
            private readonly DictionaryImpl<TKey, TKeyStore, TValue> _table;

            public SnapshotImpl(DictionaryImpl<TKey, TKeyStore, TValue> dict)
            {
                this._table = dict;

                // linearization point.
                // if table is quiescent and has no copy in progress,
                // we can simply iterate over its table.
                while (true)
                {
                    if (_table._newTable == null)
                    {
                        break;
                    }

                    // there is a copy in progress, finish it and try again
                    _table.HelpCopy(copy_all: true);
                    this._table = (DictionaryImpl<TKey, TKeyStore, TValue>)(this._table._topDict._table);
                }
            }

            public override int Count => _table.Count;

            public override bool MoveNext()
            {
                var entries = this._table._entries;
                while (_idx < entries.Length)
                {
                    var nextEntry = entries[_idx++];

                    if (nextEntry.value != null)
                    {
                        var nextKstore = nextEntry.key;
                        if (nextKstore == null)
                        {
                            // slot was deleted.
                            continue;
                        }

                        _curKey = _table.keyFromEntry(nextKstore);
                        object nextV = _table.TryGetValue(_curKey);
                        if (nextV != null)
                        {
                            _curValue = _table.FromObjectValue(nextV);
                            return true;
                        }
                    }
                }

                _curKey = default;
                _curValue = default;
                return false;
            }

            public override void Reset()
            {
                _idx = 0;
                _curKey = default;
                _curValue = default;
            }
        }
    }
}
