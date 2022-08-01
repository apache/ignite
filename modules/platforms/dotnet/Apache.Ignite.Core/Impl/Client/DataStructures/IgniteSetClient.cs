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
    using System.Collections;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Client.DataStructures;

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

        /// <summary>
        /// Initializes a new instance of <see cref="IgniteSetClient{T}"/> class,
        /// </summary>
        /// <param name="socket">Socket.</param>
        /// <param name="name">Set name.</param>
        /// <param name="colocated">Colocated flag.</param>
        /// <param name="cacheId">Cache id.</param>
        public IgniteSetClient(ClientFailoverSocket socket, string name, bool colocated, int cacheId)
        {
            _socket = socket;
            _cacheId = cacheId;

            Name = name;
            Colocated = colocated;
        }

        /** <inheritdoc /> */
        public IEnumerator<T> GetEnumerator()
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        /** <inheritdoc /> */
        void ICollection<T>.Add(T item)
        {
            _socket.DoOutInOp<object>(ClientOp.SetValueAdd, ctx => ctx.Writer.WriteObject(item), null);
        }

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
        bool ISet<T>.Add(T item)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool Contains(T item)
        {
            return _socket.DoOutInOp(
                ClientOp.SetValueContains,
                ctx => ctx.Writer.WriteObject(item),
                ctx => ctx.Reader.ReadBoolean());
        }

        /** <inheritdoc /> */
        public void CopyTo(T[] array, int arrayIndex)
        {
            throw new System.NotImplementedException();
        }

        /** <inheritdoc /> */
        public bool Remove(T item)
        {
            return _socket.DoOutInOp(
                ClientOp.SetValueRemove,
                ctx => ctx.Writer.WriteObject(item),
                ctx => ctx.Reader.ReadBoolean());
        }

        /** <inheritdoc /> */
        public int Count { get; }

        /** <inheritdoc /> */
        public bool IsReadOnly { get; }

        /** <inheritdoc /> */
        public string Name { get; }

        /** <inheritdoc /> */
        public bool Colocated { get; }

        /** <inheritdoc /> */
        public int PageSize { get; set; }

        /** <inheritdoc /> */
        public bool IsClosed { get; }

        /** <inheritdoc /> */
        public void Close()
        {
            throw new System.NotImplementedException();
        }
    }
}
