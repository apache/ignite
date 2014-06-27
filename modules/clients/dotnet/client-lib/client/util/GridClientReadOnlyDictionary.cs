/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Util
{
    using System;
    using System.Collections.Generic;

    /**
     * <summary>Read only dictionary.</summary>
     */ 
    internal class GridClientReadOnlyDictionary<K, V> : IDictionary<K, V>
    {
        /** Underlying dictionary. */
        private readonly IDictionary<K, V> dict;

        /**
         * <summary>Constructor.</summary>
         * <param name="dict">Dictionary.</param>
         */ 
        public GridClientReadOnlyDictionary(IDictionary<K, V> dict)
        {
            this.dict = dict;
        }

        /** <inheritdoc /> */
        public void Add(K key, V value)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public bool ContainsKey(K key)
        {
            return dict.ContainsKey(key);
        }

        /** <inheritdoc /> */
        public ICollection<K> Keys
        {
            get { return dict.Keys; }
        }

        /** <inheritdoc /> */
        public bool Remove(K key)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public bool TryGetValue(K key, out V value)
        {
            return dict.TryGetValue(key, out value);
        }

        /** <inheritdoc /> */
        public ICollection<V> Values
        {
            get { return dict.Values; }
        }

        /** <inheritdoc /> */
        public V this[K key]
        {
            get
            {
                return dict[key];
            }
            set
            {
                throw new NotSupportedException(); ;
            }
        }

        /** <inheritdoc /> */
        public void Add(KeyValuePair<K, V> item)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public bool Contains(KeyValuePair<K, V> item)
        {
            return dict.Contains(item);
        }

        /** <inheritdoc /> */
        public void CopyTo(KeyValuePair<K, V>[] array, int arrayIndex)
        {
            dict.CopyTo(array, arrayIndex);
        }

        /** <inheritdoc /> */
        public int Count
        {
            get { return dict.Count; }
        }

        /** <inheritdoc /> */
        public bool IsReadOnly
        {
            get { return true; }
        }

        /** <inheritdoc /> */
        public bool Remove(KeyValuePair<K, V> item)
        {
            throw new NotSupportedException();
        }

        /** <inheritdoc /> */
        public IEnumerator<KeyValuePair<K, V>> GetEnumerator()
        {
            return dict.GetEnumerator();
        }

        /** <inheritdoc /> */
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return dict.GetEnumerator();
        }
    }
}
