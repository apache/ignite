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

// ReSharper disable UnusedMember.Global
namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Represents cache query index configuration.
    /// </summary>
    public class QueryIndex
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex"/> class.
        /// </summary>
        public QueryIndex()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex" /> class.
        /// </summary>
        /// <param name="fieldNames">Names of the fields to index.</param>
        public QueryIndex(params string[] fieldNames) : this(false, fieldNames)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex" /> class.
        /// </summary>
        /// <param name="isDescending">Sort direction.</param>
        /// <param name="fieldNames">Names of the fields to index.</param>
        public QueryIndex(bool isDescending, params string[] fieldNames)
        {
            Fields = fieldNames.Select(f => new QueryIndexField(f, isDescending)).ToArray();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex" /> class.
        /// </summary>
        /// <param name="isDescending">Sort direction.</param>
        /// <param name="indexType">Type of the index.</param>
        /// <param name="fieldNames">Names of the fields to index.</param>
        public QueryIndex(bool isDescending, QueryIndexType indexType, params string[] fieldNames) 
            : this(isDescending, fieldNames)
        {
            IndexType = indexType;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex"/> class.
        /// </summary>
        /// <param name="fields">The fields.</param>
        public QueryIndex(params QueryIndexField[] fields)
        {
            if (fields == null || fields.Length == 0)
                throw new ArgumentException("Query index must have at least one field");

            if (fields.Any(f => f == null))
                throw new ArgumentException("IndexField cannot be null.");

            Fields = fields;
        }

        /// <summary>
        /// Gets or sets the index name.
        /// Will be set automatically if not specified.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the type of the index.
        /// </summary>
        public QueryIndexType IndexType { get; set; }

        /// <summary>
        /// Gets or sets a collection of fields to be indexed.
        /// </summary>
        public ICollection<QueryIndexField> Fields { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal QueryIndex(IBinaryRawReader reader)
        {
            Name = reader.ReadString();
            IndexType = (QueryIndexType) reader.ReadByte();

            var count = reader.ReadInt();
            Fields = count == 0 ? null : Enumerable.Range(0, count).Select(x =>
                new QueryIndexField(reader.ReadString(), reader.ReadBoolean())).ToList();
        }

        /// <summary>
        /// Writes this instance.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteString(Name);
            writer.WriteByte((byte) IndexType);

            if (Fields != null)
            {
                writer.WriteInt(Fields.Count);

                foreach (var field in Fields)
                {
                    writer.WriteString(field.Name);
                    writer.WriteBoolean(field.IsDescending);
                }
            }
            else
                writer.WriteInt(0);
        }
    }
}