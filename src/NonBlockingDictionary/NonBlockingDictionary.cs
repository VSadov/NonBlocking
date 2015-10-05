// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace NonBlocking
{
    public abstract class NonBlockingDictionary<TKey, TValue>
        : NonBlockingDictionary,
        IDictionary<TKey, TValue>,
        IReadOnlyDictionary<TKey, TValue>,
        IDictionary,
        ICollection
    {
        internal NonBlockingDictionary() { }

        // TODO: move to leafs
        internal IEqualityComparer<TKey> keyComparer;

        protected enum ValueMatch
        {
            Any,            // sets new value unconditionally, used by index set
            NullOrDead,     // set value if original value is null or dead, used by Add/TryAdd
            NotNullOrDead,  // set value if original value is alive, used by Remove
            OldValue,       // sets new value if old value matches
        }

        public void Add(TKey key, TValue value)
        {
            if (!TryAdd(key, value))
            {
                throw new ArgumentException("AddingDuplicate");
            }
        }

        public bool TryAdd(TKey key, TValue value)
        {
            object objValue = null;
            return putIfMatch(key, value, ref objValue, ValueMatch.NullOrDead);
        }

        public bool Remove(TKey key)
        {
            object objValue = null;
            var found = putIfMatch(key, TOMBSTONE, ref objValue, ValueMatch.NotNullOrDead);
            Debug.Assert(!(objValue is Prime));

            return found;
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            object objValue = null;
            var found = putIfMatch(key, TOMBSTONE, ref objValue, ValueMatch.NotNullOrDead);

            Debug.Assert(!(objValue is Prime));

            value = found ?
                (TValue)objValue :
                default(TValue);

            return found;
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            object objValue;
            var found = this.tryGetValue(key, out objValue);

            Debug.Assert(!(objValue is Prime));

            value = found ?
                (TValue)objValue :
                default(TValue);

            return found;
        }

        public TValue this[TKey key]
        {
            get
            {
                object objValue;
                var found = this.tryGetValue(key, out objValue);

                Debug.Assert(!(objValue is Prime));

                if (!found)
                {
                    throw new ArgumentException("key not present");
                }

                return (TValue)objValue;
            }
            set
            {
                object objValue = null;
                putIfMatch(key, value, ref objValue, ValueMatch.Any);
            }
        }

        public abstract void Clear();

        protected abstract bool putIfMatch(TKey key, object newVal, ref object oldValue, ValueMatch match);
        protected abstract bool tryGetValue(TKey key, out object value);

        public bool ContainsKey(TKey key)
        {
            TValue value;
            return this.TryGetValue(key, out value);
        }

        public void Add(KeyValuePair<TKey, TValue> item)
        {
            this.Add(item.Key, item.Value);
        }

        public bool Contains(KeyValuePair<TKey, TValue> keyValuePair)
        {
            TValue value;
            return TryGetValue(keyValuePair.Key, out value) && 
                EqualityComparer<TValue>.Default.Equals(value, keyValuePair.Value);
        }

        public bool TryUpdate(TKey key, TValue newValue, TValue comparisonValue)
        {
            object oldValue = (object)comparisonValue ?? NULLVALUE;
            return this.putIfMatch(key, newValue, ref oldValue, ValueMatch.OldValue);
        }

        public TValue GetOrAdd(TKey key, TValue value)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            object oldValue = null;
            if (this.putIfMatch(key, value, ref oldValue, ValueMatch.NullOrDead))
            {
                return value;
            }

            return (TValue)oldValue;
        }

        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            if (valueFactory == null)
            {
                throw new ArgumentNullException("valueFactory");
            }
            TValue value;
            if (this.TryGetValue(key, out value))
            {
                return value;
            }

            value = valueFactory(key);
            object oldValue = null;
            if (this.putIfMatch(key, value, ref oldValue, ValueMatch.NullOrDead))
            {
                return value;
            }

            return (TValue)oldValue;
        }

        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            object oldValue = (object)item.Value ?? NULLVALUE;
            return this.putIfMatch(item.Key, TOMBSTONE, ref oldValue, ValueMatch.OldValue);
        }

        bool IDictionary.IsReadOnly => false;
        bool IDictionary.IsFixedSize => false;
        bool ICollection.IsSynchronized => false;
        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;
        public bool IsEmpty => Count == 0;

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys => Keys;
        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values => Values;
        ICollection<TKey> IDictionary<TKey, TValue>.Keys => Keys;
        ICollection<TValue> IDictionary<TKey, TValue>.Values => Values;
        ICollection IDictionary.Keys => Keys;
        ICollection IDictionary.Values => Values;

        public abstract void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex);
        public abstract void CopyTo(DictionaryEntry[] array, int arrayIndex);
        public abstract void CopyTo(object[] array, int arrayIndex);

        public abstract ReadOnlyCollection<TKey> Keys { get; }
        public abstract ReadOnlyCollection<TValue> Values { get; }
        public abstract int Count { get; }

        public abstract IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator()
        {
            throw new InvalidOperationException("unreachable");
        }

        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            throw new InvalidOperationException("unreachable");
        }

        bool IDictionary.Contains(object key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            return key is TKey && this.ContainsKey((TKey)((object)key));
        }

        void IDictionary.Add(object key, object value)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            if (!(key is TKey))
            {
                throw new ArgumentException();
            }
            TValue value2;
            try
            {
                value2 = (TValue)((object)value);
            }
            catch (InvalidCastException)
            {
                throw new ArgumentException();
            }
            ((IDictionary<TKey, TValue>)this).Add((TKey)((object)key), value2);
        }

        void IDictionary.Remove(object key)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            if (key is TKey)
            {
                TValue tValue;
                this.TryRemove((TKey)((object)key), out tValue);
            }
        }

        void ICollection.CopyTo(Array array, int index)
        {
            var pairs = array as KeyValuePair<TKey, TValue>[];
            if (pairs != null)
            {
                CopyTo(pairs, index);
                return;
            }

            var entries = array as DictionaryEntry[];
            if (entries != null)
            {
                CopyTo(entries, index);
                return;
            }

            var objects = array as object[];
            if (objects != null)
            {
                CopyTo(objects, index);
                return;
            }

            throw new ArgumentNullException("array");
        }

        public object SyncRoot
        {
            get
            {
                throw new NotSupportedException();
            }
        }

        object IDictionary.this[object key]
        {
            get
            {
                if (key == null)
                {
                    throw new ArgumentNullException("key");
                }
                TValue tValue;
                if (key is TKey && this.TryGetValue((TKey)((object)key), out tValue))
                {
                    return tValue;
                }
                return null;
            }
            set
            {
                if (key == null)
                {
                    throw new ArgumentNullException("key");
                }
                if (!(key is TKey))
                {
                    throw new ArgumentException();
                }
                if (!(value is TValue))
                {
                    throw new ArgumentException();
                }
                this[(TKey)((object)key)] = (TValue)((object)value);
            }
        }

        public TValue AddOrUpdate(TKey key, Func<TKey, TValue> addValueFactory, Func<TKey, TValue, TValue> updateValueFactory)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            if (addValueFactory == null)
            {
                throw new ArgumentNullException("addValueFactory");
            }
            if (updateValueFactory == null)
            {
                throw new ArgumentNullException("updateValueFactory");
            }
            TValue tValue2;
            while (true)
            {
                TValue tValue;
                if (this.TryGetValue(key, out tValue))
                {
                    tValue2 = updateValueFactory(key, tValue);
                    if (this.TryUpdate(key, tValue2, tValue))
                    {
                        break;
                    }
                }
                else
                {
                    tValue2 = addValueFactory(key);
                    if (this.TryAdd(key, tValue2))
                    {
                        break;
                    }
                }
            }
            return tValue2;
        }

        public TValue AddOrUpdate(TKey key, TValue addValue, Func<TKey, TValue, TValue> updateValueFactory)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            if (updateValueFactory == null)
            {
                throw new ArgumentNullException("updateValueFactory");
            }
            TValue tValue2;
            while (true)
            {
                TValue tValue;
                if (this.TryGetValue(key, out tValue))
                {
                    tValue2 = updateValueFactory(key, tValue);
                    if (this.TryUpdate(key, tValue2, tValue))
                    {
                        return tValue2;
                    }
                }
                else if (this.TryAdd(key, addValue))
                {
                    return addValue;
                }
            }            
        }
    }
}
