/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Datastream
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;

    /// <summary>
    /// Convenience adapter to visit every key-value tuple in the stream.
    /// Note that the visitor does not update the cache.
    /// </summary>
    /// <typeparam name="K">The type of the cache key.</typeparam>
    /// <typeparam name="V">The type of the cache value.</typeparam>
    [Serializable]
    public sealed class StreamVisitor<K, V> : IStreamReceiver<K, V>
    {
        /** Visitor action */
        private readonly Action<ICache<K, V>, ICacheEntry<K, V>> action;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamVisitor{K, V}"/> class.
        /// </summary>
        /// <param name="action">The action to be called on each stream entry.</param>
        public StreamVisitor(Action<ICache<K, V>, ICacheEntry<K, V>> action)
        {
            A.NotNull(action, "action");

            this.action = action;
        }

        /** <inheritdoc /> */
        public void Receive(ICache<K, V> cache, ICollection<ICacheEntry<K, V>> entries)
        {
            foreach (var entry in entries)
                action(cache, entry);
        }
    }
}