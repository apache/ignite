/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Datastream
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Convenience adapter to transform update existing values in streaming cache 
    /// based on the previously cached value.
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="TV">Value type.</typeparam>
    /// <typeparam name="TA">The type of the processor argument.</typeparam>
    /// <typeparam name="TR">The type of the processor result.</typeparam>
    public sealed class StreamTransformer<TK, TV, TA, TR> : IStreamReceiver<TK, TV>, 
        IPortableWriteAware
    {
        /** Entry processor. */
        private readonly ICacheEntryProcessor<TK, TV, TA, TR> _proc;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamTransformer{K, V, A, R}"/> class.
        /// </summary>
        /// <param name="proc">Entry processor.</param>
        public StreamTransformer(ICacheEntryProcessor<TK, TV, TA, TR> proc)
        {
            IgniteArgumentCheck.NotNull(proc, "proc");

            _proc = proc;
        }

        /** <inheritdoc /> */
        public void Receive(ICache<TK, TV> cache, ICollection<ICacheEntry<TK, TV>> entries)
        {
            var keys = new List<TK>(entries.Count);

            foreach (var entry in entries)
                keys.Add(entry.Key);

            cache.InvokeAll(keys, _proc, default(TA));
        }

        /** <inheritdoc /> */
        void IPortableWriteAware.WritePortable(IPortableWriter writer)
        {
            var w = (PortableWriterImpl)writer;

            w.WriteByte(StreamReceiverHolder.RcvTransformer);

            PortableUtils.WritePortableOrSerializable(w, _proc);
        }
    }
}