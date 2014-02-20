// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Util {
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;

    using sc = System.Collections;
    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using Dbg = System.Diagnostics.Debug;

    /** <summary>Null-key allowed dictionary.</summary> */
    internal class GridClientNullDictionary<TKey, TVal> : IDictionary<TKey, TVal> {
        /** <summary>Null value.</summary> */
        private static TKey NULL = default(TKey);

        /** <summary>Inner delagate for not-null keys.</summary> */
        private IDictionary<TKey, TVal> inner = new Dictionary<TKey, TVal>();

        /** <summary>Flag to mark this map contains null key.</summary> */
        private bool nullKey;

        /** <summary>Value in the map by the null key.</summary> */
        private TVal nullValue;

        /** <summary>Constructs empty null-key allowed dictionary.</summary> */
        public GridClientNullDictionary() : this(new Dictionary<TKey, TVal>()) {
        }

        /**
         * <summary>
         * Constructs null-key allowed dictionary based on the passed one.</summary>
         *
         * <param name="inner">Map to wrap with this one.</param>
         */
        public GridClientNullDictionary(IDictionary<TKey, TVal> inner) {
            A.Ensure(inner != null, "inner != null");

            this.inner = inner;
        }

        //
        // IDictionary<K, V>
        //

        /** <inheritdoc /> */
        public ICollection<TKey> Keys {
            get {
                var keys = inner.Keys;

                if (nullKey) {
                    keys = new List<TKey>(keys);

                    keys.Add(NULL);
                }

                return keys;
            }
        }

        /** <inheritdoc /> */
        public ICollection<TVal> Values {
            get {
                ICollection<TVal> values = inner.Values;

                if (nullKey) {
                    values = new List<TVal>(values);

                    values.Add(nullValue);
                }

                return values;
            }
        }

        /** <inheritdoc /> */
        public TVal this[TKey key] {
            get {
                if (key != null)
                    return inner[key];

                if (nullKey)
                    return nullValue;

                return default(TVal);
            }
            set {
                if (key != null)
                    inner[key] = value;
                else {
                    nullKey = true;
                    nullValue = value;
                }
            }
        }

        /** <inheritdoc /> */
        public void Add(TKey key, TVal val) {
            if (key != null)
                inner.Add(key, val);
            else {
                if (nullKey)
                    throw new ArgumentException("Element already exists for key: null");

                nullKey = true;
                nullValue = val;
            }
        }

        /** <inheritdoc /> */
        public bool ContainsKey(TKey key) {
            return key == null ? nullKey : inner.ContainsKey(key);
        }

        /** <inheritdoc /> */
        public bool Remove(TKey key) {
            if (key != null)
                return inner.Remove(key);

            if (nullKey) {
                nullKey = false;
                nullValue = default(TVal);

                return true;
            }

            return false;
        }

        /** <inheritdoc /> */
        public bool TryGetValue(TKey key, out TVal val) {
            if (key != null)
                return inner.TryGetValue(key, out val);

            val = nullKey ? nullValue : default(TVal);

            return nullKey;
        }


        //
        // ICollection<KeyValuePair<TKey, TValue>>
        //

        /** <inheritdoc /> */
        public int Count {
            get {
                return inner.Count + (nullKey ? 1 : 0);
            }
        }

        /** <inheritdoc /> */
        public bool IsReadOnly {
            get {
                return inner.IsReadOnly;
            }
        }

        /** <inheritdoc /> */
        public void Add(KeyValuePair<TKey, TVal> item) {
            if (item.Key != null)
                inner.Add(item);
            else {
                nullKey = true;
                nullValue = item.Value;
            }
        }

        /** <inheritdoc /> */
        public void Clear() {
            inner.Clear();

            nullKey = false;
            nullValue = default(TVal);
        }

        /** <inheritdoc /> */
        public bool Contains(KeyValuePair<TKey, TVal> item) {
            if (item.Key != null)
                return inner.Contains(item);

            if (!nullKey)
                return false;

            // Null value and item value equals.
            return Equals(nullValue, item.Value);
        }

        /** <inheritdoc /> */
        public void CopyTo(KeyValuePair<TKey, TVal>[] array, int arrayIndex) {
            if (nullKey)
                array[arrayIndex++] = new KeyValuePair<TKey, TVal>(NULL, nullValue);

            inner.CopyTo(array, arrayIndex);;
        }

        /** <inheritdoc /> */
        public bool Remove(KeyValuePair<TKey, TVal> item) {
            if (item.Key != null)
                return inner.Remove(item);

            if (!nullKey)
                return false;

            // Null value and item value differ.
            if (!Equals(nullValue, item.Value))
                return false;

            nullKey = false;
            nullValue = default(TVal);

            return true;
        }


        //
        // IEnumerable<KeyValuePair<TKey, TValue>>
        //

        /** <inheritdoc /> */
        public IEnumerator<KeyValuePair<TKey, TVal>> GetEnumerator() {
            return nullKey ? new NullEnumerator(inner, nullValue) : inner.GetEnumerator();
        }


        //
        // IEnumerable
        //

        /** <inheritdoc /> */
        sc::IEnumerator sc::IEnumerable.GetEnumerator() {
            return GetEnumerator();
        }

        //
        // Object
        //

        /** <inheritdoc /> */
        public override int GetHashCode() {
            int code = inner.GetHashCode();

            if (nullKey && nullValue != null)
                code = code * 31 + nullValue.GetHashCode();

            return code;
        }

        /** <inheritdoc /> */
        override public bool Equals(object obj) {
            var that = obj as GridClientNullDictionary<TKey, TVal>;

            if (that != null) {
                if (nullKey != that.nullKey)
                    return false;

                if (nullKey && !Equals(nullValue, that.nullValue))
                    return false;

                return inner.Equals(that.inner);
            }

            if (obj is IDictionary<TKey, TVal>) {
                if (nullKey)
                    return false;

                return inner.Equals(obj);
            }

            return false;
        }


        //
        // Private members
        //

        /**
         * <summary>
         * Compare arguments for equals.</summary>
         *
         * <param name="v1">Object #1.</param>
         * <param name="v2">Object #2.</param>
         * <returns><c>true</c> if objects are equal, <c>false</c> otherwise.</returns>
         */
        private static bool Equals(TVal v1, TVal v2) {
            return v1 == null ? v2 == null : v1.Equals(v2);
        }

        /** <summary>Enumerator for the null-key dictionary.</summary> */
        private class NullEnumerator : IEnumerator<KeyValuePair<TKey, TVal>> {
            /** <summary>Wrapped enumerator.</summary> */
            private IEnumerator<KeyValuePair<TKey, TVal>> inner;

            /** <summary>Key-value pair for the null-key, always present.</summary> */
            private KeyValuePair<TKey, TVal> nullValue;

            /** <summary>Initial pointer position.</summary> */
            private int position = -1;

            /**
             * <summary>
             * Constructs enumerator for the null-key dictionary.</summary>
             *
             * <param name="map">Wrapped map.</param>
             * <param name="nullValue">Value for the null key, always present.</param>
             */
            public NullEnumerator(IDictionary<TKey, TVal> map, TVal nullValue) {
                this.inner = map.GetEnumerator();
                this.nullValue = new KeyValuePair<TKey, TVal>(NULL, nullValue);

                this.Reset();
            }

            /** <inheritdoc /> */
            public KeyValuePair<TKey, TVal> Current {
                get {
                    return position == 0 ? nullValue : inner.Current;
                }
            }

            /** <inheritdoc /> */
            object sc::IEnumerator.Current {
                get {
                    return this.Current;
                }
            }

            /** <inheritdoc /> */
            public bool MoveNext() {
                return ++position == 0 || inner.MoveNext();
            }

            /** <inheritdoc /> */
            public void Reset() {
                position = -1;

                inner.Reset();
            }

            /** <inheritdoc /> */
            public void Dispose() {
                if (inner is IDisposable)
                    ((IDisposable)inner).Dispose();

                nullValue = default(KeyValuePair<TKey, TVal>);
            }
        }
    }
}
