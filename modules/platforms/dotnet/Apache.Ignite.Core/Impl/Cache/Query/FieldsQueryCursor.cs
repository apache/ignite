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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Cursor for entry-based queries.
    /// </summary>
    internal class FieldsQueryCursor<T> : AbstractQueryCursor<T>
    {
        /** */
        private readonly Func<IBinaryRawReader, int, T> _readerFunc;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaler.</param>
        /// <param name="keepBinary">Keep poratble flag.</param>
        /// <param name="readerFunc">The reader function.</param>
        public FieldsQueryCursor(IUnmanagedTarget target, Marshaller marsh, bool keepBinary, 
            Func<IBinaryRawReader, int, T> readerFunc)
            : base(target, marsh, keepBinary)
        {
            Debug.Assert(readerFunc != null);

            _readerFunc = readerFunc;
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        protected override T Read(BinaryReader reader)
        {
            // Reading and skipping row size in bytes.
            reader.ReadInt();

            int cnt = reader.ReadInt();

            return _readerFunc(reader, cnt);
        }
    }
}
