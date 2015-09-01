/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Datastream
{
    /// <summary>
    /// Data streamer entry.
    /// </summary>
    internal class DataStreamerEntry<K, V>
    {
        /** Key. */
        private readonly K key;

        /** Value. */
        private readonly V val;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public DataStreamerEntry(K key, V val)
        {
            this.key = key;
            this.val = val;
        }

        /// <summary>
        /// Key.
        /// </summary>
        public K Key
        {
            get
            {
                return key;
            }
        }

        /// <summary>
        /// Value.
        /// </summary>
        public V Value
        {
            get
            {
                return val;
            }
        }
    }
}
