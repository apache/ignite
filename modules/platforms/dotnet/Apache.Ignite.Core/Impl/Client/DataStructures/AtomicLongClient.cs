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

namespace Apache.Ignite.Core.Impl.Client.DataStructures
{
    using Apache.Ignite.Core.Client.DataStructures;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Thin client atomic long.
    /// </summary>
    internal sealed class AtomicLongClient : IAtomicLongClient
    {
        /** */
        private const string DefaultDataStructuresCacheGroupName = "default-ds-group";

        /** */
        private const string AtomicsCacheName = "ignite-sys-atomic-cache";

        /** */
        private readonly ClientFailoverSocket _socket;

        /** */
        private readonly int _cacheId;

        /** */
        private readonly string _groupName;

        /// <summary>
        /// Initializes a new instance of <see cref="AtomicLongClient"/> class.
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="name">Name.</param>
        /// <param name="groupName">Group name.</param>
        public AtomicLongClient(ClientFailoverSocket socket, string name, string groupName)
        {
            _socket = socket;
            Name = name;
            _groupName = groupName;

            var cacheName = AtomicsCacheName + "@" + (groupName ?? DefaultDataStructuresCacheGroupName);
            _cacheId = BinaryUtils.GetCacheId(cacheName);
        }

        /** <inheritDoc /> */
        public string Name { get; }

        /** <inheritDoc /> */
        public long Read()
        {
            return _socket.DoOutInOpAffinity(
                ClientOp.AtomicLongValueGet,
                ctx => WriteName(ctx),
                r => r.Reader.ReadLong(),
                _cacheId,
                AffinityKey);
        }

        /** <inheritDoc /> */
        public long Increment()
        {
            return Add(1);
        }

        /** <inheritDoc /> */
        public long Add(long value)
        {
            return _socket.DoOutInOpAffinity(
                ClientOp.AtomicLongValueAddAndGet,
                ctx =>
                {
                    WriteName(ctx);
                    ctx.Writer.WriteLong(value);
                },
                r => r.Reader.ReadLong(),
                _cacheId,
                AffinityKey);
        }

        /** <inheritDoc /> */
        public long Decrement()
        {
            return Add(-1);
        }

        /** <inheritDoc /> */
        public long Exchange(long value)
        {
            return _socket.DoOutInOpAffinity(
                ClientOp.AtomicLongValueGetAndSet,
                ctx =>
                {
                    WriteName(ctx);
                    ctx.Writer.WriteLong(value);
                },
                r => r.Reader.ReadLong(),
                _cacheId,
                AffinityKey);
        }

        /** <inheritDoc /> */
        public long CompareExchange(long value, long comparand)
        {
            return _socket.DoOutInOpAffinity(
                ClientOp.AtomicLongValueCompareAndSetAndGet,
                ctx =>
                {
                    WriteName(ctx);
                    ctx.Writer.WriteLong(comparand);
                    ctx.Writer.WriteLong(value);
                },
                r => r.Reader.ReadLong(),
                _cacheId,
                AffinityKey);
        }

        /** <inheritDoc /> */
        public bool IsClosed()
        {
            return _socket.DoOutInOpAffinity(
                ClientOp.AtomicLongExists,
                ctx => WriteName(ctx),
                r => !r.Reader.ReadBoolean(),
                _cacheId,
                AffinityKey);
        }

        /** <inheritDoc /> */
        public void Close()
        {
            _socket.DoOutInOpAffinity<object, string>(
                ClientOp.AtomicLongRemove,
                ctx => WriteName(ctx),
                null,
                _cacheId,
                AffinityKey);
        }

        /// <summary>
        /// Gets the affinity key.
        /// GridCacheInternalKeyImpl uses name as AffinityKeyMapped.
        /// </summary>
        private string AffinityKey => Name;

        /// <summary>
        /// Writes the name of this data structure.
        /// </summary>
        private void WriteName(ClientRequestContext ctx)
        {
            ctx.Writer.WriteString(Name);
            ctx.Writer.WriteString(_groupName);
        }
    }
}
