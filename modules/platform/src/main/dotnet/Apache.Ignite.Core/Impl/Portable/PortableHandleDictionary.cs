/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Object handle dictionary.
    /// </summary>
    internal class PortableHandleDictionary<K, V>
    {
        /** Initial array sizes. */
        private const int INITIAL_SIZE = 7;

        /** Dictionary. */
        private Dictionary<K, V> dict;

        /** First key. */
        private readonly K key1;

        /** First value. */
        private readonly V val1;

        /** Second key. */
        private K key2;

        /** Second value. */
        private V val2;

        /** Third key. */
        private K key3;

        /** Third value. */
        private V val3;

        /// <summary>
        /// Constructor with initial key-value pair.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        [SuppressMessage("ReSharper", "DoNotCallOverridableMethodsInConstructor")]
        public PortableHandleDictionary(K key, V val)
        {
            Debug.Assert(!Equals(key, EmptyKey));
            
            key1 = key;
            val1 = val;

            key2 = EmptyKey;
            key3 = EmptyKey;
        }

        /// <summary>
        /// Add value to dictionary.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public void Add(K key, V val)
        {
            Debug.Assert(!Equals(key, EmptyKey));

            if (Equals(key2, EmptyKey))
            {
                key2 = key;
                val2 = val;

                return;
            }

            if (Equals(key3, EmptyKey))
            {
                key3 = key;
                val3 = val;

                return;
            }

            if (dict == null)
                dict = new Dictionary<K, V>(INITIAL_SIZE);

            dict[key] = val;
        }

        /// <summary>
        /// Try getting value for the given key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        /// <returns>True if key was found.</returns>
        public bool TryGetValue(K key, out V val)
        {
            Debug.Assert(!Equals(key, EmptyKey));

            if (Equals(key, key1))
            {
                val = val1;

                return true;
            }

            if (Equals(key, key2))
            {
                val = val2;

                return true;
            }

            if (Equals(key, key3))
            {
                val = val3;

                return true;
            }

            if (dict == null)
            {
                val = default(V);

                return false;
            }

            return dict.TryGetValue(key, out val);
        }

        /// <summary>
        /// Merge data from another dictionary without overwrite.
        /// </summary>
        /// <param name="that">Other dictionary.</param>
        public void Merge(PortableHandleDictionary<K, V> that)
        {
            Debug.Assert(that != null, "that == null");
            
            AddIfAbsent(that.key1, that.val1);
            AddIfAbsent(that.key2, that.val2);
            AddIfAbsent(that.key3, that.val3);

            if (that.dict == null)
                return;

            foreach (var pair in that.dict)
                AddIfAbsent(pair.Key, pair.Value);
        }

        /// <summary>
        /// Add key/value pair to the bucket if absent.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        private void AddIfAbsent(K key, V val)
        {
            if (Equals(key, EmptyKey))
                return;

            if (Equals(key, key1) || Equals(key, key2) || Equals(key, key3))
                return;

            if (dict == null || !dict.ContainsKey(key))
                Add(key, val);
        }

        /// <summary>
        /// Gets the empty key.
        /// </summary>
        protected virtual K EmptyKey
        {
            get { return default(K); }
        }
    }
}