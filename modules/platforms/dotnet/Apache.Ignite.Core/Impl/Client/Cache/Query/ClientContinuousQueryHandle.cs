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

namespace Apache.Ignite.Core.Impl.Client.Cache.Query
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Cache.Query.Continuous;

    internal class ClientContinuousQueryHandle<TK, TV> : IContinuousQueryHandle<ICacheEntry<TK, TV>>,
        IContinuousQueryHandleFields
    {
        /** Socket. */
        private readonly ClientSocket _socket;

        /** Keep binary flag. */
        private readonly bool _keepBinary;

        /** Cursor ID. */
        private readonly long _queryId;

        /** Columns (for fields initial query). */
        private readonly IList<string> _columns;

        /** */
        private readonly object _disposeSyncRoot = new object();

        /** */
        private bool _disposed;

        /// <summary>
        /// Initializes a new instance of <see cref="ClientContinuousQueryHandle{TK, TV}"/>.
        /// </summary>
        public ClientContinuousQueryHandle(ClientSocket socket, bool keepBinary, long queryId, IList<string> columns)
        {
            _socket = socket;
            _keepBinary = keepBinary;
            _queryId = queryId;
            _columns = columns;
        }

        /** <inheritdoc /> */
        public IQueryCursor<ICacheEntry<TK, TV>> GetInitialQueryCursor()
        {
            return new ClientQueryCursor<TK, TV>(_socket, _queryId, _keepBinary, null, ClientOp.QueryScanCursorGetPage);
        }

        /** <inheritdoc /> */
        IFieldsQueryCursor IContinuousQueryHandleFields.GetInitialQueryCursor()
        {
            Debug.Assert(_columns != null);

            return new ClientFieldsQueryCursor(_socket, _queryId, _keepBinary, null,
                ClientOp.QuerySqlFieldsCursorGetPage, _columns);
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Releases unmanaged and - optionally - managed resources.
        /// </summary>
        /// <param name="disposing">
        /// <c>true</c> when called from Dispose; <c>false</c> when called from finalizer.
        /// </param>
        protected virtual void Dispose(bool disposing)
        {
            lock (_disposeSyncRoot)
            {
                if (_disposed)
                {
                    return;
                }

                _socket.DoOutInOp<object>(ClientOp.ResourceClose,
                    ctx => ctx.Writer.WriteLong(_queryId), null);

                _socket.RemoveNotificationHandler(_queryId);

                _disposed = true;
            }
        }

        /** <inheritdoc /> */
        ~ClientContinuousQueryHandle()
        {
            Dispose(false);
        }
    }
}
