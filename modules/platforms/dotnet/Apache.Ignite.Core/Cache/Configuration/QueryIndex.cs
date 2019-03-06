/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

// ReSharper disable UnusedMember.Global
namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Represents cache query index configuration.
    /// </summary>
    public class QueryIndex
    {
        /// <summary>
        /// Default value for <see cref="InlineSize"/>.
        /// </summary>
        public const int DefaultInlineSize = -1;

        /** Inline size. */
        private int _inlineSize = DefaultInlineSize;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex"/> class.
        /// </summary>
        public QueryIndex() : this((string[]) null)
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
            : this(isDescending, QueryIndexType.Sorted, fieldNames)
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex" /> class.
        /// </summary>
        /// <param name="isDescending">Sort direction.</param>
        /// <param name="indexType">Type of the index.</param>
        /// <param name="fieldNames">Names of the fields to index.</param>
        public QueryIndex(bool isDescending, QueryIndexType indexType, params string[] fieldNames) 
        {
            if (fieldNames != null)
            {
                Fields = fieldNames.Select(f => new QueryIndexField(f, isDescending)).ToArray();
            }

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
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<QueryIndexField> Fields { get; set; }

        /// <summary>
        /// Gets index inline size in bytes. When enabled part of indexed value will be placed directly to index pages,
        /// thus minimizing data page accesses and increasing query performance.
        /// <para />
        /// Allowed values:
        /// <ul>
        /// <li><c>-1</c> (default) - determine inline size automatically(see below)</li>
        /// <li><c>0</c> - index inline is disabled(not recommended)</li>
        /// <li>positive value - fixed index inline</li >
        /// </ul>
        /// When set to <c>-1</c>, Ignite will try to detect inline size automatically. It will be no more than
        /// <see cref="CacheConfiguration.SqlIndexMaxInlineSize"/>.
        /// Index inline will be enabled for all fixed-length types,
        ///  but <b>will not be enabled</b> for <see cref="string"/>.
        /// </summary>
        [DefaultValue(DefaultInlineSize)]
        public int InlineSize
        {
            get { return _inlineSize; }
            set { _inlineSize = value; }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryIndex"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal QueryIndex(IBinaryRawReader reader)
        {
            Name = reader.ReadString();
            IndexType = (QueryIndexType) reader.ReadByte();
            InlineSize = reader.ReadInt();

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
            writer.WriteInt(InlineSize);

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