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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Query cursor field implementation.
    /// </summary>
    internal class QueryCursorField : IQueryCursorField
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryCursorField"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public QueryCursorField(IBinaryRawReader reader)
        {
            Name = reader.ReadString();
            JavaTypeName = reader.ReadString();
            Type = JavaTypes.GetDotNetType(JavaTypeName);
        }

        /** <inheritdoc /> */
        public string Name { get; private set; }

        /** <inheritdoc /> */
        public string JavaTypeName { get; private set; }

        /** <inheritdoc /> */
        public Type Type { get; private set; }
    }
}