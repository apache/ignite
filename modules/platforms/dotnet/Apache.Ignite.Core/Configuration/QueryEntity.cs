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

namespace Apache.Ignite.Core.Configuration
{
    using System.Collections.Generic;

    /// <summary>
    /// Query entity is a description of cache entry (composed of key and value) 
    /// in a way of how it must be indexed and can be queried.
    /// </summary>
    public class QueryEntity
    {
        // TODO: KeyType, ValueType, Fields

        /// <summary>
        /// Gets or sets key type name.
        /// </summary>
        public string KeyTypeName { get; set; }

        /// <summary>
        /// Gets or sets value type name.
        /// </summary>
        public string ValueTypeName { get; set; }

        /// <summary>
        /// Gets or sets query fields, a map from field name to type name. 
        /// The order of fields is important as it defines the order of columns returned by the 'select *' queries.
        /// </summary>
        public ICollection<KeyValuePair<string, string>> FieldNames { get; set; }

        /// <summary>
        /// Gets or sets field name aliases: mapping from full name in dot notation to an alias 
        /// that will be used as SQL column name.
        /// Example: {"parent.name" -> "parentName"}.
        /// </summary>
        public IDictionary<string, string> Aliases { get; set; }

        /// <summary>
        /// Gets or sets the query indexes.
        /// </summary>
        public ICollection<QueryIndex> Indexes { get; set; }
    }
}
