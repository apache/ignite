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
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Client.DataStructures;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Client set.
    /// </summary>
    /// <typeparam name="T">Element type.</typeparam>
    internal sealed class IgniteSetClient<T> : IIgniteSetClient<T>
    {
        /** */
        private readonly ClientFailoverSocket _socket;

        /** */
        private readonly int _cacheId;

        /** */
        private readonly object _nameHash;

        /** */
        private int _pageSize = 1024;

        /// <summary>
        /// Initializes a new instance of <see cref="IgniteSetClient{T}"/> class,
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="name">Set name.</param>
        /// <param name="colocated">Colocated flag.</param>
        /// <param name="cacheId">Cache id.</param>
        public IgniteSetClient(ClientFailoverSocket socket, string name, bool colocated, int cacheId)
        {
            Debug.Assert(socket != null);
            Debug.Assert(name != null);

            _socket = socket;
            _cacheId = cacheId;
            _nameHash = BinaryUtils.GetStringHashCode(name);

            Name = name;
            Colocated = colocated;
        }

        /** <inheritdoc /> */
        public IEnumerator<T> GetEnumerator()
        {
            if (Colocated)
            {
                return _socket.DoOutInOpAffinity(
                    ClientOp.SetIteratorStart,
                    ctx => WriteIteratorStart(ctx),
                    ctx => new IgniteSetClientEnumerator<T>(ctx, PageSize),
                    _cacheId,
                    _nameHash);
            }

            return _socket.DoOutInOp(
                ClientOp.SetIteratorStart,
                ctx => WriteIteratorStart(ctx),
                ctx => new IgniteSetClientEnumerator<T>(ctx, PageSize));
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /** <inheritdoc /> */
        void ICollection<T>.Add(T item) => AddIfNotPresent(item);

        /** <inheritdoc /> */
        public void ExceptWith(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void IntersectWith(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool IsProperSubsetOf(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool IsProperSupersetOf(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool IsSubsetOf(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool IsSupersetOf(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool Overlaps(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool SetEquals(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void SymmetricExceptWith(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void UnionWith(IEnumerable<T> other)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        bool ISet<T>.Add(T item) => AddIfNotPresent(item);

        /** <inheritdoc /> */
        public void Clear() => Op<object>(ClientOp.SetClear);

        /** <inheritdoc /> */
        public bool Contains(T item) => SingleKeyOp(ClientOp.SetValueContains, item);

        /** <inheritdoc /> */
        public void CopyTo(T[] array, int arrayIndex)
        {
            foreach (var item in this)
            {
                if (arrayIndex >= array.Length)
                {
                    return;
                }

                array[arrayIndex++] = item;
            }
        }

        /** <inheritdoc /> */
        public bool Remove(T item) => SingleKeyOp(ClientOp.SetValueRemove, item);

        /** <inheritdoc /> */
        public int Count => Op(ClientOp.SetSize, null, r => r.Stream.ReadInt());

        /** <inheritdoc /> */
        public bool IsReadOnly => false;

        /** <inheritdoc /> */
        public string Name { get; }

        /** <inheritdoc /> */
        public bool Colocated { get; }

        /** <inheritdoc /> */
        public int PageSize
        {
            get => _pageSize;
            set => _pageSize = value > 0 ? value : throw new ArgumentOutOfRangeException(nameof(value));
        }

        /** <inheritdoc /> */
        public bool IsClosed => Op(ClientOp.SetExists, null, r => !r.Stream.ReadBool());

        /** <inheritdoc /> */
        public void Close() => Op<object>(ClientOp.SetClose);

        private bool AddIfNotPresent(T item) => SingleKeyOp(ClientOp.SetValueAdd, item);

        private TRes Op<TRes>(
            ClientOp op,
            Action<BinaryWriter> writeAction = null,
            Func<ClientResponseContext, TRes> readFunc = null) =>
            _socket.DoOutInOp(op, ctx =>
            {
                WriteIdentity(ctx.Writer);
                writeAction?.Invoke(ctx.Writer);
            }, readFunc);

        private bool SingleKeyOp(ClientOp op, T item) =>
            _socket.DoOutInOpAffinity(
                op,
                ctx =>
                {
                    var w = ctx.Writer;

                    WriteIdentity(w);
                    w.WriteBoolean(true); // ServerKeepBinary
                    w.WriteObject(item);
                },
                ctx => ctx.Stream.ReadBool(),
                _cacheId,
                GetAffinityKey(item));

        private void WriteIdentity(BinaryWriter w)
        {
            w.WriteString(Name);
            w.WriteInt(_cacheId);
            w.WriteBoolean(Colocated);
        }

        private object GetAffinityKey(T key)
        {
            // See ClientIgniteSetImpl#affinityKey in Java for details.
            return Colocated ? _nameHash : key;
        }

        private void WriteIteratorStart(ClientRequestContext ctx)
        {
            WriteIdentity(ctx.Writer);
            ctx.Writer.WriteInt(_pageSize);
        }
    }
}
