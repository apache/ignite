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
    using System.Collections;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;

    /// <summary>
    /// Cursor for entry-based queries.
    /// </summary>
    internal class FieldsQueryCursor : AbstractQueryCursor<IList>
    {
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaler.</param>
        /// <param name="keepPortable">Keep poratble flag.</param>
        public FieldsQueryCursor(IUnmanagedTarget target, PortableMarshaller marsh, bool keepPortable)
            : base(target, marsh, keepPortable)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        protected override IList Read(PortableReaderImpl reader)
        {
            int cnt = reader.ReadInt();

            var res = new ArrayList(cnt);

            for (int i = 0; i < cnt; i++)
                res.Add(reader.ReadObject<object>());

            return res;
        }
    }
}
