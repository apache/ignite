/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
namespace Apache.Ignite.Core.Impl.Client.Cache.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Cache.Query;

    /// <summary>
    /// Client query cursor base.
    /// </summary>
    internal class ClientQueryCursorBase<T> : QueryCursorBase<T>
    {
        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Cursor ID. */
        private readonly long _cursorId;

        /** Page op code. */
        private readonly ClientOp _getPageOp;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientQueryCursorBase{T}" /> class.
        /// </summary>
        /// <param name="ignite">The ignite.</param>
        /// <param name="cursorId">The cursor identifier.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="initialBatchStream">Optional stream with initial batch.</param>
        /// <param name="getPageOp">The get page op.</param>
        /// <param name="readFunc">Read func.</param>
        public ClientQueryCursorBase(IgniteClient ignite, long cursorId, bool keepBinary, 
            IBinaryStream initialBatchStream, ClientOp getPageOp, Func<BinaryReader, T> readFunc) 
            : base(ignite.Marshaller, keepBinary, readFunc, initialBatchStream)
        {
            _ignite = ignite;
            _cursorId = cursorId;
            _getPageOp = getPageOp;
        }

        /** <inheritdoc /> */
        protected override void InitIterator()
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override IList<T> GetAllInternal()
        {
            return this.ToArray();
        }

        /** <inheritdoc /> */
        protected override T[] GetBatch()
        {
            return _ignite.Socket.DoOutInOp(_getPageOp, w => w.WriteLong(_cursorId), s => ConvertGetBatch(s));
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