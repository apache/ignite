/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Cache.Event
{
    using Apache.Ignite.Core.Cache.Event;

    /// <summary>
    /// Cache entry update event.
    /// </summary>
    internal class CacheEntryUpdateEvent<K, V> : ICacheEntryEvent<K, V>
    {
        /** Key.*/
        private readonly K key;

        /** Value.*/
        private readonly V val;

        /** Old value.*/
        private readonly V oldVal;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="oldVal">Old value.</param>
        /// <param name="val">Value.</param>
        public CacheEntryUpdateEvent(K key, V oldVal, V val)
        {
            this.key = key;
            this.oldVal = oldVal;
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
            get { return oldVal; }
        }

        /** <inheritdoc /> */
        public bool HasOldValue
        {
            get { return true; }
        }

        /** <inheritdoc /> */
        public CacheEntryEventType EventType
        {
            get { return CacheEntryEventType.UPDATED; }
        }
    }
}
