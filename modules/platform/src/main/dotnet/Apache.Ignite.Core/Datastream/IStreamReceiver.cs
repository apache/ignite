/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Datastream
{
    using System.Collections.Generic;
    using GridGain.Cache;

    /// <summary>
    /// Updates cache with batch of entries. 
    /// Usually it is enough to configure <see cref="IDataStreamer{K,V}.AllowOverwrite" /> property and appropriate 
    /// internal cache receiver will be chosen automatically. But in some cases custom implementation may help 
    /// to achieve better performance.
    /// </summary>
    public interface IStreamReceiver<K, V>
    {
        /// <summary>
        /// Updates cache with batch of entries.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <param name="entries">Entries.</param>
        void Receive(ICache<K, V> cache, ICollection<ICacheEntry<K, V>> entries);
    }
}