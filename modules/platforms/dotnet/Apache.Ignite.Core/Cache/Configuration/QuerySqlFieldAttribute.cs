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

namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;
    using System.ComponentModel;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Marks field or property for SQL queries.
    /// <para />
    /// Using this attribute is an alternative to <see cref="QueryEntity.Fields"/> in <see cref="CacheConfiguration"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
    public sealed class QuerySqlFieldAttribute : Attribute
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="QuerySqlFieldAttribute"/> class.
        /// </summary>
        public QuerySqlFieldAttribute()
        {
            IndexInlineSize = QueryIndex.DefaultInlineSize;
            Precision = -1;
            Scale = -1;
        }

        /// <summary>
        /// Gets or sets the sql field name.
        /// If not provided, property or field name will be used.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether corresponding field should be indexed.
        /// Just like with databases, field indexing may require additional overhead during updates, 
        /// but makes select operations faster.
        /// </summary>
        public bool IsIndexed { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether index for this field should be descending.
        /// Ignored when <see cref="IsIndexed"/> is <c>false</c>.
        /// </summary>
        public bool IsDescending { get; set; }

        /// <summary>
        /// Gets or sets the collection of index groups this field belongs to. 
        /// Groups are used for compound indexes, 
        /// whenever index should be created on more than one field.
        /// All fields within the same group will belong to the same index.
        /// </summary>
        [SuppressMessage("Microsoft.Performance", "CA1819:PropertiesShouldNotReturnArrays", 
            Justification = "Attribute initializers do not allow collections")]
        public string[] IndexGroups { get; set; }

        /// <summary>
        /// Gets or sets the index inline size, see <see cref="QueryIndex.InlineSize"/>.
        /// </summary>
        [DefaultValue(QueryIndex.DefaultInlineSize)]
        public int IndexInlineSize { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether null values are allowed for this field.
        /// </summary>
        public bool NotNull { get; set; }

        /// <summary>
        /// Gets or sets the default value for the field (has effect when inserting with DML).
        /// </summary>
        public object DefaultValue { get; set; }

        /// <summary>
        /// Gets or sets the precision for the field.
        /// </summary>
        public int Precision { get; set; }

        /// <summary>
        /// Gets or sets the scale for the field.
        /// </summary>
        public int Scale { get; set; }
    }
}
