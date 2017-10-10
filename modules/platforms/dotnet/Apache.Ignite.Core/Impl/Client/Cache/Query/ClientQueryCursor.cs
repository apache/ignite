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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cache.Query;
    using Apache.Ignite.Core.Impl.Client;

    /// <summary>
    /// Client query cursor.
    /// </summary>
    internal class ClientQueryCursor<TK, TV> : QueryCursorBase<ICacheEntry<TK, TV>>
    {
        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Cursor ID. */
        private readonly long _cursorId;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientQueryCursor{TK, TV}" /> class.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        /// <param name="cursorId">The cursor identifier.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="initialBatchStream">Optional stream with initial batch.</param>
        public ClientQueryCursor(IgniteClient ignite, long cursorId, bool keepBinary, 
            IBinaryStream initialBatchStream) 
            : base(ignite.Marshaller, keepBinary, initialBatchStream)
        {
            _ignite = ignite;
            _cursorId = cursorId;
        }

        /** <inheritdoc /> */
        protected override void InitIterator()
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override IList<ICacheEntry<TK, TV>> GetAllInternal()
        {
            return this.ToArray();
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override ICacheEntry<TK, TV> Read(BinaryReader reader)
        {
            return new CacheEntry<TK, TV>(reader.ReadObject<TK>(), reader.ReadObject<TV>());
        }

        /** <inheritdoc /> */
        protected override ICacheEntry<TK, TV>[] GetBatch()
        {
            return _ignite.Socket.DoOutInOp(ClientOp.QueryScanCursorGetPage,
                w => w.WriteLong(_cursorId),
                s => ConvertGetBatch(s));
        }

        /** <inheritdoc /> */
        protected override void Dispose(bool disposing)
        {
            try
            {
                _ignite.Socket.DoOutInOp<object>(ClientOp.ResourceClose, w => w.WriteLong(_cursorId), null);
            }
            finally
            {
                base.Dispose(disposing);
            }
        }
    }
}
