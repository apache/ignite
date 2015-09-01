/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache.Store
{
    using System.Collections.Generic;
    
    using GridGain.Cache.Store;

    /// <summary>
    /// Store session implementation.
    /// </summary>
    internal class CacheStoreSession : ICacheStoreSession
    {
        /** Properties. */
        private IDictionary<object, object> props;
        
        /** <inheritdoc /> */

        public string CacheName
        {
            get; internal set;
        }

        /** <inheritdoc /> */
        public IDictionary<object, object> Properties
        {
            get { return props ?? (props = new Dictionary<object, object>(2)); }
        }

        /// <summary>
        /// Clear session state.
        /// </summary>
        public void Clear()
        {
            if (props != null)
                props.Clear();
        }
    }
}
