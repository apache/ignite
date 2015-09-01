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
    /// Remove marker.
    /// </summary>
    internal class DataStreamerRemoveEntry<K>
    {
        /** Key to remove. */
        private readonly K key;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="key">Key.</param>
        public DataStreamerRemoveEntry(K key)
        {
            this.key = key;
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
    }
}
