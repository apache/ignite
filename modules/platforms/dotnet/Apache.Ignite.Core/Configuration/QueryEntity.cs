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

// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable MemberCanBePrivate.Global
namespace Apache.Ignite.Core.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Query entity is a description of cache entry (composed of key and value) 
    /// in a way of how it must be indexed and can be queried.
    /// </summary>
    public class QueryEntity
    {
        // TODO: KeyType, ValueType, Fields

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryEntity"/> class.
        /// </summary>
        public QueryEntity()
        {
            // No-op.
        }

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

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryEntity"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal QueryEntity(IBinaryRawReader reader)
        {
            KeyTypeName = reader.ReadString();
            ValueTypeName = reader.ReadString();

            FieldNames = Enumerable.Range(0, reader.ReadInt()).Select(x =>
                new KeyValuePair<string, string>(reader.ReadString(), reader.ReadString())).ToList();

            Aliases = Enumerable.Range(0, reader.ReadInt())
                .ToDictionary(x => reader.ReadString(), x => reader.ReadString());

            Indexes = Enumerable.Range(0, reader.ReadInt()).Select(x => new QueryIndex(reader)).ToList();
        }

        /// <summary>
        /// Writes this instance.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteString(KeyTypeName);
            writer.WriteString(ValueTypeName);

            WritePairs(writer, FieldNames);
            WritePairs(writer, Aliases);

            if (Indexes != null)
            {
                writer.WriteInt(Indexes.Count);

                foreach (var index in Indexes)
                {
                    if (index == null)
                        throw new InvalidOperationException("Invalid cache configuration: QueryIndex can't be null.");

                    index.Write(writer);
                }
            }
            else
                writer.WriteInt(0);
        }

        /// <summary>
        /// Writes pairs.
        /// </summary>
        private static void WritePairs(IBinaryRawWriter writer, ICollection<KeyValuePair<string, string>> pairs)
        {
            if (pairs != null)
            {
                writer.WriteInt(pairs.Count);

                foreach (var field in pairs)
                {
                    writer.WriteString(field.Key);
                    writer.WriteString(field.Value);
                }
            }
            else
                writer.WriteInt(0);
        }
    }
}
