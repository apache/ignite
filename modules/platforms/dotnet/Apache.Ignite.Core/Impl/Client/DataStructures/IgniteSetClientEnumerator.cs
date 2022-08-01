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
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Enumerator for <see cref="IgniteSetClient{T}"/>.
    /// </summary>
    internal sealed class IgniteSetClientEnumerator<T> : IEnumerator<T>
    {
        /** */
        private long? _resourceId;

        /** */
        private readonly ClientSocket _socket;

        /** */
        private List<T> _page;

        /** */
        private int _pos = -1;

        /// <summary>
        /// Initializes a new instance of <see cref="IgniteSetClientEnumerator{T}"/> class.
        /// </summary>
        /// <param name="ctx">Response context.</param>
        public IgniteSetClientEnumerator(ClientResponseContext ctx)
        {
            _page = ReadPage(ctx.Reader);

            var hasNext = ctx.Reader.ReadBoolean();

            _resourceId = hasNext ? ctx.Reader.ReadLong() : (long?)null;
            _socket = ctx.Socket;
        }

        public bool MoveNext()
        {
            if (_pos < _page.Count - 1)
            {
                _pos++;
                return true;
            }

            if (_resourceId == null)
            {
                return false;
            }

            _socket.DoOutInOp<object>(ClientOp.SetIteratorGetPage,
                ctx => ctx.Stream.WriteLong(_resourceId.Value),
                ctx =>
                {
                    _page = ReadPage(ctx.Reader);

                    var hasNext = ctx.Reader.ReadBoolean();

                    if (!hasNext)
                    {
                        _resourceId = null;
                    }

                    return null;
                });

            _pos = 0;
            return true;
        }

        public void Reset() => throw new NotSupportedException();

        public T Current
        {
            get
            {
                if (_pos == -1)
                {
                    // TODO: Proper text - see
                    throw new InvalidOperationException("Enumeration has not started. Call MoveNext.");
                }

                if (_pos >= _page.Count)
                {
                    throw new InvalidOperationException("Enumeration already finished.");
                }

                return _page[_pos];
            }
        }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
            if (_resourceId == null)
            {
                return;
            }

            _socket.DoOutInOp<object>(ClientOp.ResourceClose, ctx => ctx.Stream.WriteLong(_resourceId.Value), null);

            _resourceId = null;
            _pos = int.MaxValue;
        }

        private static List<T> ReadPage(IBinaryRawReader r)
        {
            var size = r.ReadInt();
            var res = new List<T>(size);

            for (int i = 0; i < size; i++)
            {
                res.Add(r.ReadObject<T>());
            }

            return res;
        }
    }
}
