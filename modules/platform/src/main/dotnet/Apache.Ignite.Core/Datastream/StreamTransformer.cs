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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;
    using AC = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;

    /// <summary>
    /// Convenience adapter to transform update existing values in streaming cache 
    /// based on the previously cached value.
    /// </summary>
    /// <typeparam name="K">The type of the cache key.</typeparam>
    /// <typeparam name="V">The type of the cache value.</typeparam>
    /// <typeparam name="A">The type of the processor argument.</typeparam>
    /// <typeparam name="R">The type of the processor result.</typeparam>
    public sealed class StreamTransformer<K, V, A, R> : IStreamReceiver<K, V>, 
        IPortableWriteAware
    {
        /** Entry processor. */
        private readonly ICacheEntryProcessor<K, V, A, R> proc;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamTransformer{K, V, A, R}"/> class.
        /// </summary>
        /// <param name="proc">Entry processor.</param>
        public StreamTransformer(ICacheEntryProcessor<K, V, A, R> proc)
        {
            AC.NotNull(proc, "proc");

            this.proc = proc;
        }

        /** <inheritdoc /> */
        public void Receive(ICache<K, V> cache, ICollection<ICacheEntry<K, V>> entries)
        {
            var keys = new List<K>(entries.Count);

            foreach (var entry in entries)
                keys.Add(entry.Key);

            cache.InvokeAll(keys, proc, default(A));
        }

        /** <inheritdoc /> */
        void IPortableWriteAware.WritePortable(IPortableWriter writer)
        {
            var w = (PortableWriterImpl)writer;

            w.WriteByte(StreamReceiverHolder.RCV_TRANSFORMER);

            PortableUtils.WritePortableOrSerializable(w, proc);
        }
    }
}