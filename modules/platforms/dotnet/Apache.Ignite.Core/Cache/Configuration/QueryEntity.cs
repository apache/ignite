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
namespace Apache.Ignite.Core.Cache.Configuration
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Query entity is a description of cache entry (composed of key and value) 
    /// in a way of how it must be indexed and can be queried.
    /// </summary>
    public sealed class QueryEntity : IQueryEntityInternal, IBinaryRawWriteAware
    {
        /** */
        private Type _keyType;

        /** */
        private Type _valueType;

        /** */
        private string _valueTypeName;

        /** */
        private string _keyTypeName;

        /** */
        private Dictionary<string, string> _aliasMap;

        /** */
        private ICollection<QueryAlias> _aliases;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryEntity"/> class.
        /// </summary>
        public QueryEntity()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryEntity"/> class.
        /// </summary>
        /// <param name="valueType">Type of the cache entry value.</param>
        public QueryEntity(Type valueType)
        {
            ValueType = valueType;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryEntity"/> class.
        /// </summary>
        /// <param name="keyType">Type of the key.</param>
        /// <param name="valueType">Type of the value.</param>
        public QueryEntity(Type keyType, Type valueType)
        {
            KeyType = keyType;
            ValueType = valueType;
        }

        /// <summary>
        /// Gets or sets key Java type name.
        /// </summary>
        public string KeyTypeName
        {
            get { return _keyTypeName; }
            set
            {
                _keyTypeName = value;
                _keyType = null;
            }
        }

        /// <summary>
        /// Gets or sets the type of the key.
        /// <para />
        /// This is a shortcut for <see cref="KeyTypeName"/>. Getter will return null for non-primitive types.
        /// <para />
        /// Setting this property will overwrite <see cref="Fields"/> and <see cref="Indexes"/> according to
        /// <see cref="QuerySqlFieldAttribute"/>, if any.
        /// </summary>
        public Type KeyType
        {
            get { return _keyType ?? JavaTypes.GetDotNetType(KeyTypeName); }
            set
            {
                RescanAttributes(value, _valueType);  // Do this first because it can throw

                KeyTypeName = value == null
                    ? null
                    : (JavaTypes.GetJavaTypeName(value) ?? BinaryUtils.GetSqlTypeName(value));

                _keyType = value;
            }
        }

        /// <summary>
        /// Gets or sets value Java type name.
        /// </summary>
        public string ValueTypeName
        {
            get { return _valueTypeName; }
            set
            {
                _valueTypeName = value;
                _valueType = null;
            }
        }

        /// <summary>
        /// Gets or sets the type of the value.
        /// <para />
        /// This is a shortcut for <see cref="ValueTypeName"/>. Getter will return null for non-primitive types.
        /// <para />
        /// Setting this property will overwrite <see cref="Fields"/> and <see cref="Indexes"/> according to
        /// <see cref="QuerySqlFieldAttribute"/>, if any.
        /// </summary>
        public Type ValueType
        {
            get { return _valueType ?? JavaTypes.GetDotNetType(ValueTypeName); }
            set
            {
                RescanAttributes(_keyType, value);  // Do this first because it can throw

                ValueTypeName = value == null
                    ? null
                    : (JavaTypes.GetJavaTypeName(value) ?? BinaryUtils.GetSqlTypeName(value));

                _valueType = value;
            }
        }

        /// <summary>
        /// Gets or sets the name of the field that is used to denote the entire key.
        /// <para />
        /// By default, entite key can be accessed with a special "_key" field name.
        /// </summary>
        public string KeyFieldName { get; set; }

        /// <summary>
        /// Gets or sets the name of the field that is used to denote the entire value.
        /// <para />
        /// By default, entite value can be accessed with a special "_val" field name.
        /// </summary>
        public string ValueFieldName { get; set; }

        /// <summary>
        /// Gets or sets the name of the SQL table.
        /// When not set, value type name is used.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets query fields, a map from field name to Java type name. 
        /// The order of fields defines the order of columns returned by the 'select *' queries.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<QueryField> Fields { get; set; }

        /// <summary>
        /// Gets or sets field name aliases: mapping from full name in dot notation to an alias 
        /// that will be used as SQL column name.
        /// Example: {"parent.name" -> "parentName"}.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<QueryAlias> Aliases
        {
            get { return _aliases; }
            set
            {
                _aliases = value;
                _aliasMap = null;
            }
        }

        /// <summary>
        /// Gets or sets the query indexes.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<QueryIndex> Indexes { get; set; }

        /// <summary>
        /// Gets the alias by field name, or null when no match found.
        /// This method constructs a dictionary lazily to perform lookups.
        /// </summary>
        string IQueryEntityInternal.GetAlias(string fieldName)
        {
            if (Aliases == null || Aliases.Count == 0)
            {
                return null;
            }

            // PERF: No ToDictionary.
            if (_aliasMap == null)
            {
                _aliasMap = new Dictionary<string, string>(Aliases.Count, StringComparer.Ordinal);

                foreach (var alias in Aliases)
                {
                    _aliasMap[alias.FullName] = alias.Alias;
                }
            }

            string res;
            return _aliasMap.TryGetValue(fieldName, out res) ? res : null;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryEntity"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal QueryEntity(IBinaryRawReader reader)
        {
            KeyTypeName = reader.ReadString();
            ValueTypeName = reader.ReadString();
            TableName = reader.ReadString();
            KeyFieldName = reader.ReadString();
            ValueFieldName = reader.ReadString();

            var count = reader.ReadInt();
            Fields = count == 0
                ? null
                : Enumerable.Range(0, count).Select(x => new QueryField(reader)).ToList();

            count = reader.ReadInt();
            Aliases = count == 0 ? null : Enumerable.Range(0, count)
                .Select(x=> new QueryAlias(reader.ReadString(), reader.ReadString())).ToList();

            count = reader.ReadInt();
            Indexes = count == 0 ? null : Enumerable.Range(0, count).Select(x => new QueryIndex(reader)).ToList();
        }

        /// <summary>
        /// Writes this instance.
        /// </summary>
        void IBinaryRawWriteAware<IBinaryRawWriter>.Write(IBinaryRawWriter writer)
        {
            writer.WriteString(KeyTypeName);
            writer.WriteString(ValueTypeName);
            writer.WriteString(TableName);
            writer.WriteString(KeyFieldName);
            writer.WriteString(ValueFieldName);

            if (Fields != null)
            {
                writer.WriteInt(Fields.Count);

                foreach (var field in Fields)
                {
                    field.Write(writer);
                }
            }
            else
                writer.WriteInt(0);


            if (Aliases != null)
            {
                writer.WriteInt(Aliases.Count);

                foreach (var queryAlias in Aliases)
                {
                    writer.WriteString(queryAlias.FullName);
                    writer.WriteString(queryAlias.Alias);
                }
            }
            else
                writer.WriteInt(0);

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
        /// Validates this instance and outputs information to the log, if necessary.
        /// </summary>
        internal void Validate(ILogger log, string logInfo)
        {
            Debug.Assert(log != null);
            Debug.Assert(logInfo != null);

            logInfo += string.Format(", QueryEntity '{0}:{1}'", _keyTypeName ?? "", _valueTypeName ?? "");

            JavaTypes.LogIndirectMappingWarning(_keyType, log, logInfo);
            JavaTypes.LogIndirectMappingWarning(_valueType, log, logInfo);

            var fields = Fields;
            if (fields != null)
            {
                foreach (var field in fields)
                    field.Validate(log, logInfo);
            }
        }

        /// <summary>
        /// Copies the local properties (properties that are not written in Write method).
        /// </summary>
        internal void CopyLocalProperties(QueryEntity entity)
        {
            Debug.Assert(entity != null);

            if (entity._keyType != null)
            {
                _keyType = entity._keyType;
            }

            if (entity._valueType != null)
            {
                _valueType = entity._valueType;
            }

            if (Fields != null && entity.Fields != null)
            {
                var fields = entity.Fields.Where(x => x != null).ToDictionary(x => "_" + x.Name, x => x);

                foreach (var field in Fields)
                {
                    QueryField src;

                    if (fields.TryGetValue("_" + field.Name, out src))
                    {
                        field.CopyLocalProperties(src);
                    }
                }
            }
        }

        /// <summary>
        /// Rescans the attributes in <see cref="KeyType"/> and <see cref="ValueType"/>.
        /// </summary>
        private void RescanAttributes(Type keyType, Type valType)
        {
            if (keyType == null && valType == null)
                return;

            var fields = new List<QueryField>();
            var indexes = new List<QueryIndexEx>();

            if (keyType != null)
                ScanAttributes(keyType, fields, indexes, null, new HashSet<Type>(), true);

            if (valType != null)
                ScanAttributes(valType, fields, indexes, null, new HashSet<Type>(), false);

            if (fields.Any())
                Fields = fields.OrderBy(x => x.Name).ToList();

            if (indexes.Any())
                Indexes = GetGroupIndexes(indexes).ToArray();
        }

        /// <summary>
        /// Gets the group indexes.
        /// </summary>
        /// <param name="indexes">Ungrouped indexes with their group names.</param>
        /// <returns></returns>
        private static IEnumerable<QueryIndex> GetGroupIndexes(List<QueryIndexEx> indexes)
        {
            return indexes.Where(idx => idx.IndexGroups != null)
                .SelectMany(idx => idx.IndexGroups.Select(g => new {Index = idx, GroupName = g}))
                .GroupBy(x => x.GroupName)
                .Select(g =>
                {
                    var idxs = g.Select(pair => pair.Index).ToArray();

                    var first = idxs.First();

                    return new QueryIndex(idxs.SelectMany(i => i.Fields).ToArray())
                    {
                        IndexType = first.IndexType,
                        Name = first.Name
                    };
                })
                .Concat(indexes.Where(idx => idx.IndexGroups == null));
        }

        /// <summary>
        /// Scans specified type for occurences of <see cref="QuerySqlFieldAttribute" />.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="fields">The fields.</param>
        /// <param name="indexes">The indexes.</param>
        /// <param name="parentPropName">Name of the parent property.</param>
        /// <param name="visitedTypes">The visited types.</param>
        /// <param name="isKey">Whether this is a key type.</param>
        /// <exception cref="System.InvalidOperationException">Recursive Query Field definition detected:  + type</exception>
        private static void ScanAttributes(Type type, List<QueryField> fields, List<QueryIndexEx> indexes, 
            string parentPropName, ISet<Type> visitedTypes, bool isKey)
        {
            Debug.Assert(type != null);
            Debug.Assert(fields != null);
            Debug.Assert(indexes != null);

            if (visitedTypes.Contains(type))
                throw new InvalidOperationException("Recursive Query Field definition detected: " + type);

            visitedTypes.Add(type);

            foreach (var memberInfo in ReflectionUtils.GetFieldsAndProperties(type))
            {
                var customAttributes = memberInfo.Key.GetCustomAttributes(true);

                foreach (var attr in customAttributes.OfType<QuerySqlFieldAttribute>())
                {
                    var columnName = attr.Name ?? memberInfo.Key.Name;

                    // Dot notation is required for nested SQL fields.
                    if (parentPropName != null)
                    {
                        columnName = parentPropName + "." + columnName;
                    }

                    if (attr.IsIndexed)
                    {
                        indexes.Add(new QueryIndexEx(columnName, attr.IsDescending, QueryIndexType.Sorted,
                            attr.IndexGroups)
                        {
                            InlineSize = attr.IndexInlineSize,
                        });
                    }

                    fields.Add(new QueryField(columnName, memberInfo.Value)
                    {
                        IsKeyField = isKey,
                        NotNull = attr.NotNull,
                        DefaultValue = attr.DefaultValue,
                        Precision = attr.Precision,
                        Scale = attr.Scale
                    });

                    ScanAttributes(memberInfo.Value, fields, indexes, columnName, visitedTypes, isKey);
                }

                foreach (var attr in customAttributes.OfType<QueryTextFieldAttribute>())
                {
                    var columnName = attr.Name ?? memberInfo.Key.Name;

                    if (parentPropName != null)
                    {
                        columnName = parentPropName + "." + columnName;
                    }

                    indexes.Add(new QueryIndexEx(columnName, false, QueryIndexType.FullText, null));

                    fields.Add(new QueryField(columnName, memberInfo.Value) {IsKeyField = isKey});

                    ScanAttributes(memberInfo.Value, fields, indexes, columnName, visitedTypes, isKey);
                }
            }

            visitedTypes.Remove(type);
        }

        /// <summary>
        /// Extended index with group names.
        /// </summary>
        private class QueryIndexEx : QueryIndex
        {
            /// <summary>
            /// Initializes a new instance of the <see cref="QueryIndexEx"/> class.
            /// </summary>
            /// <param name="fieldName">Name of the field.</param>
            /// <param name="isDescending">if set to <c>true</c> [is descending].</param>
            /// <param name="indexType">Type of the index.</param>
            /// <param name="groups">The groups.</param>
            public QueryIndexEx(string fieldName, bool isDescending, QueryIndexType indexType, 
                ICollection<string> groups) 
                : base(isDescending, indexType, fieldName)
            {
                IndexGroups = groups;
            }

            /// <summary>
            /// Gets or sets the index groups.
            /// </summary>
            public ICollection<string> IndexGroups { get; set; }
        }
    }
}
