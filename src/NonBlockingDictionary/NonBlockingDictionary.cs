// Copyright (c) Vladimir Sadov. All rights reserved.
//
// This file is distributed under the MIT License. See LICENSE.md for details.

using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;

namespace NonBlocking
{
    public abstract class NonBlockingDictionary
    {
        internal NonBlockingDictionary() { }

        public static NonBlockingDictionary<TKey, TValue> Create<TKey, TValue>(
            int cLevel,
            int size,
            IEqualityComparer<TKey> comparer = null)
        {
            return Create<TKey, TValue>(comparer);
        }

        public static NonBlockingDictionary<TKey, TValue> Create<TKey, TValue>(
            IEqualityComparer<TKey> comparer = null)
        {
            NonBlockingDictionary<TKey, TValue> result;

            if (default(TKey) == null)
            {
                if (typeof(TKey) == typeof(ValueType) ||
                    !(default(TKey) is ValueType))
                {
                    return CreateRefUnsafe<TKey, TValue>(comparer);
                }
            }
            else
            {
                if (typeof(TKey) == typeof(int))
                {
                    if (comparer == null)
                    {
                        return (NonBlockingDictionary<TKey, TValue>)(object)new NonBlockingDictionaryIntNoComparer<TValue>();
                    }

                    result = (NonBlockingDictionary<TKey, TValue>)(object)new NonBlockingDictionaryInt<TValue>();
                    result.keyComparer = comparer;
                    return result;
                }

                if (typeof(TKey) == typeof(long))
                {
                    if (comparer == null)
                    {
                        return (NonBlockingDictionary<TKey, TValue>)(object)new NonBlockingDictionaryLongNoComparer<TValue>();
                    }

                    result = (NonBlockingDictionary<TKey, TValue>)(object)new NonBlockingDictionaryLong<TValue>();
                    result.keyComparer = comparer;
                    return result;
                }
            }

            result = new NonBlockingDictionaryBoxed<TKey, TValue>();
            result.keyComparer = comparer ?? EqualityComparer<TKey>.Default;
            return result;
        }

        internal static NonBlockingDictionary<TKey, TValue> CreateRefUnsafe<TKey, TValue>(IEqualityComparer<TKey> comparer = null)
        {
            return (NonBlockingDictionary<TKey, TValue>)typeof(NonBlockingDictionary).
                GetMethod("CreateRef", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static).
                MakeGenericMethod(new Type[] { typeof(TKey), typeof(TValue) }).
                Invoke(null, new object[] { comparer });

            //var result = (NonBlockingDictionary<TKey, TValue>)
            //    Activator.CreateInstance(typeof(NonBlockingDictionaryRef<,>).MakeGenericType(new Type[] { typeof(TKey), typeof(TValue) }));

            //result.keyComparer = comparer ?? EqualityComparer<TKey>.Default;
            //return result;
        }

        internal static NonBlockingDictionary<TKey, TValue> CreateRef<TKey, TValue>(IEqualityComparer<TKey> comparer = null)
            where TKey : class
        {
            var result = new NonBlockingDictionaryRef<TKey, TKey, TValue>();
            result.keyComparer = comparer ?? EqualityComparer<TKey>.Default;
            return result;
        }

        ///////////////
        // Internal statics that do not need to be generic on TKey/TValue

        internal sealed class Prime
        {
            internal object originalValue;

            public Prime(object originalValue)
            {
                this.originalValue = originalValue;
            }
        }

        internal static readonly object TOMBSTONE = new object();
        internal static readonly Prime TOMBPRIME = new Prime(TOMBSTONE);
        internal static readonly object NULLVALUE = new object();
    }

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
            NullOrDead,     // set value if original value is null or dead, used by Add/TryAdd
            NotNullOrDead,  // set value if original value is alive, used by Remove
            Any,            // sets new value unconditionally, used by index set
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
            object objValue;
            return putIfMatch(key, value, out objValue, ValueMatch.NullOrDead);
        }

        public bool Remove(TKey key)
        {
            object objValue;
            var found = putIfMatch(key, TOMBSTONE, out objValue, ValueMatch.NotNullOrDead);
            Debug.Assert(!(objValue is Prime));

            return found;
        }

        public bool TryRemove(TKey key, out TValue value)
        {
            object objValue;
            var found = putIfMatch(key, TOMBSTONE, out objValue, ValueMatch.NotNullOrDead);

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
                object objValue;
                putIfMatch(key, value, out objValue, ValueMatch.Any);
            }
        }

        public abstract void Clear();

        protected abstract bool putIfMatch(TKey key, object newVal, out object value, ValueMatch match);
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
            //TODO: VS
            throw new NotImplementedException();
        }

        public TValue GetOrAdd(TKey key, TValue value)
        {
            if (key == null)
            {
                throw new ArgumentNullException("key");
            }
            //TODO: VS
            throw new NotImplementedException();
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
            TValue result;
            if (this.TryGetValue(key, out result))
            {
                return result;
            }
            //TODO: VS
            throw new NotImplementedException();
        }

        bool IDictionary.IsReadOnly => false;
        bool IDictionary.IsFixedSize => false;
        bool ICollection.IsSynchronized => false;
        bool ICollection<KeyValuePair<TKey, TValue>>.IsReadOnly => false;
        public bool IsEmpty => Count != 0;

        public abstract bool Remove(KeyValuePair<TKey, TValue> item);
        public abstract void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex);
        public abstract IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator();
        public abstract ICollection<TKey> Keys { get; }
        public abstract ICollection<TValue> Values { get; }
        public abstract int Count { get; }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
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

        IDictionaryEnumerator IDictionary.GetEnumerator()
        {
            throw new NotImplementedException();
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
            throw new NotImplementedException();
        }

        IEnumerable<TKey> IReadOnlyDictionary<TKey, TValue>.Keys
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        IEnumerable<TValue> IReadOnlyDictionary<TKey, TValue>.Values
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        ICollection IDictionary.Keys
        {
            get
            {
                throw new NotImplementedException();
            }
        }

        ICollection IDictionary.Values
        {
            get
            {
                throw new NotImplementedException();
            }
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
                TValue result;
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
