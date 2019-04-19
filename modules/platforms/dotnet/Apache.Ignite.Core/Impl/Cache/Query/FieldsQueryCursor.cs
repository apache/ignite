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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Cursor for entry-based queries.
    /// </summary>
    internal class FieldsQueryCursor<T> : PlatformQueryQursorBase<T>
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="keepBinary">Keep poratble flag.</param>
        /// <param name="readerFunc">The reader function.</param>
        public FieldsQueryCursor(IPlatformTargetInternal target, bool keepBinary, 
            Func<IBinaryRawReader, int, T> readerFunc)
            : base(target, keepBinary, r =>
            {
                // Reading and skipping row size in bytes.
                r.ReadInt();

                int cnt = r.ReadInt();

                return readerFunc(r, cnt);

            })
        {
            // No-op.
        }
    }

    /// <summary>
    /// Cursor for entry-based queries.
    /// </summary>
    internal class FieldsQueryCursor : FieldsQueryCursor<IList<object>>, IFieldsQueryCursor
    {
        /** */
        private const int OpGetFieldNames = 7;

        /** */
        private IList<string> _fieldNames;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="keepBinary">Keep poratble flag.</param>
        /// <param name="readerFunc">The reader function.</param>
        public FieldsQueryCursor(IPlatformTargetInternal target, bool keepBinary, 
            Func<IBinaryRawReader, int, IList<object>> readerFunc) : base(target, keepBinary, readerFunc)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public IList<string> FieldNames
        {
            get
            {
                return _fieldNames ??
                       (_fieldNames = new ReadOnlyCollection<string>(
                           Target.OutStream(OpGetFieldNames, reader => reader.ReadStringCollection())));
            }
        }
    }
}
