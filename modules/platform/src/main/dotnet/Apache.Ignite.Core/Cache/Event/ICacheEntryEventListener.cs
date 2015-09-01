/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache.Event
{
    using System.Collections.Generic;

    /// <summary>
    /// Cache entry event listener.
    /// </summary>
    public interface ICacheEntryEventListener<K, V>
    {
        /// <summary>
        /// Event callback.
        /// </summary>
        /// <param name="evts">Events.</param>
        void OnEvent(IEnumerable<ICacheEntryEvent<K, V>> evts);
    }
}
