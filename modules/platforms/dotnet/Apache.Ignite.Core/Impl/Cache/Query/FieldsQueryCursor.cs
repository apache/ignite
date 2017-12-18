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

namespace Apache.Ignite.Core.Impl.Cache.Query
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Query;

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
        private const int OpGetFieldNames = 6;

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
            get { return Target.OutStream(OpGetFieldNames, reader => reader.ReadStringArray()); }
        }
    }
}
