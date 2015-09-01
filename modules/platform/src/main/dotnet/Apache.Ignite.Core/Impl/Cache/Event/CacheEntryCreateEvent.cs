/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache.Event
{
    using GridGain.Cache;
    using GridGain.Cache.Event;

    /// <summary>
    /// Cache entry create event.
    /// </summary>
    internal class CacheEntryCreateEvent<K, V> : ICacheEntryEvent<K, V>
    {
        /** Key.*/
        private readonly K key;

        /** Value.*/
        private readonly V val;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public CacheEntryCreateEvent(K key, V val)
        {
            this.key = key;
            this.val = val;
        }

        /** <inheritdoc /> */
        public K Key
        {
            get { return key; }
        }

        /** <inheritdoc /> */
        public V Value
        {
            get { return val; }
        }

        /** <inheritdoc /> */
        public V OldValue
        {
            get { return default(V); }
        }

        /** <inheritdoc /> */
        public bool HasOldValue
        {
            get { return false; }
        }

        /** <inheritdoc /> */
        public CacheEntryEventType EventType
        {
            get { return CacheEntryEventType.CREATED; }
        }
    }
}
