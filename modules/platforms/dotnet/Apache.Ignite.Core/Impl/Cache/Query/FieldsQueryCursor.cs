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
        /// <param name="keepBinary">Keep binary flag.</param>
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
        private const int OpGetFieldsMeta = 8;

        /** */
        private IList<string> _fieldNames;

        /** */
        private IList<IQueryCursorField> _fieldsMeta;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <param name="readerFunc">The reader function.</param>
        public FieldsQueryCursor(IPlatformTargetInternal target, bool keepBinary,
            Func<IBinaryRawReader, int, IList<object>> readerFunc = null)
            : base(target, keepBinary, readerFunc ?? ReadFieldsArrayList)
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

        /** <inheritdoc /> */
        public IList<IQueryCursorField> Fields
        {
            get
            {
                if (_fieldsMeta == null)
                {
                    var metadata = Target.OutStream(
                        OpGetFieldsMeta,
                        reader => reader.ReadCollectionRaw(stream =>
                            new QueryCursorField(stream) as IQueryCursorField));

                    _fieldsMeta = new ReadOnlyCollection<IQueryCursorField>(metadata ?? new List<IQueryCursorField>());
                }

                return _fieldsMeta;
            }
        }

        /// <summary>
        /// Reads the fields array list.
        /// </summary>
        public static List<object> ReadFieldsArrayList(IBinaryRawReader reader, int count)
        {
            var res = new List<object>(count);

            for (var i = 0; i < count; i++)
                res.Add(reader.ReadObject<object>());

            return res;
        }
    }
}
