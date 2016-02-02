﻿/*
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
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Query entity is a description of cache entry (composed of key and value) 
    /// in a way of how it must be indexed and can be queried.
    /// </summary>
    public class QueryEntity
    {
        /** */
        private Type _keyType;

        /** */
        private Type _valueType;

        /** */
        private string _valueTypeName;

        /** */
        private string _keyTypeName;

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
                    : (JavaTypes.GetJavaTypeName(value) ?? BinaryUtils.GetTypeName(value));

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
                    : (JavaTypes.GetJavaTypeName(value) ?? BinaryUtils.GetTypeName(value));

                _valueType = value;
            }
        }

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
        public ICollection<QueryAlias> Aliases { get; set; }

        /// <summary>
        /// Gets or sets the query indexes.
        /// </summary>
        [SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public ICollection<QueryIndex> Indexes { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryEntity"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        internal QueryEntity(IBinaryRawReader reader)
        {
            KeyTypeName = reader.ReadString();
            ValueTypeName = reader.ReadString();

            var count = reader.ReadInt();
            Fields = count == 0 ? null : Enumerable.Range(0, count).Select(x =>
                    new QueryField(reader.ReadString(), reader.ReadString())).ToList();

            count = reader.ReadInt();
            Aliases = count == 0 ? null : Enumerable.Range(0, count)
                .Select(x=> new QueryAlias(reader.ReadString(), reader.ReadString())).ToList();

            count = reader.ReadInt();
            Indexes = count == 0 ? null : Enumerable.Range(0, count).Select(x => new QueryIndex(reader)).ToList();
        }

        /// <summary>
        /// Writes this instance.
        /// </summary>
        internal void Write(IBinaryRawWriter writer)
        {
            writer.WriteString(KeyTypeName);
            writer.WriteString(ValueTypeName);

            if (Fields != null)
            {
                writer.WriteInt(Fields.Count);

                foreach (var field in Fields)
                {
                    writer.WriteString(field.Name);
                    writer.WriteString(field.FieldTypeName);
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
        /// Rescans the attributes in <see cref="KeyType"/> and <see cref="ValueType"/>.
        /// </summary>
        private void RescanAttributes(params Type[] types)
        {
            if (types.Length == 0 || types.All(t => t == null))
                return;

            var fields = new List<QueryField>();
            var indexes = new List<QueryIndexEx>();

            foreach (var type in types.Where(t => t != null))
                ScanAttributes(type, fields, indexes, null, new HashSet<Type>());

            if (fields.Any())
                Fields = fields;

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
        /// Scans specified type for occurences of <see cref="QuerySqlFieldAttribute"/>.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="fields">The fields.</param>
        /// <param name="indexes">The indexes.</param>
        /// <param name="parentPropName">Name of the parent property.</param>
        /// <param name="visitedTypes">The visited types.</param>
        private static void ScanAttributes(Type type, List<QueryField> fields, List<QueryIndexEx> indexes, 
            string parentPropName, ISet<Type> visitedTypes)
        {
            Debug.Assert(type != null);
            Debug.Assert(fields != null);
            Debug.Assert(indexes != null);

            if (visitedTypes.Contains(type))
                throw new InvalidOperationException("Recursive Query Field definition detected: " + type);

            visitedTypes.Add(type);

            foreach (var memberInfo in GetFieldsAndProperties(type))
            {
                var customAttributes = memberInfo.Key.GetCustomAttributes(true);

                foreach (var attr in customAttributes.OfType<QuerySqlFieldAttribute>())
                {
                    var columnName = attr.Name ?? memberInfo.Key.Name;

                    // Dot notation is required for nested SQL fields
                    if (parentPropName != null)
                        columnName = parentPropName + "." + columnName;

                    fields.Add(new QueryField(columnName, memberInfo.Value));

                    if (attr.IsIndexed)
                        indexes.Add(new QueryIndexEx(columnName, attr.IsDescending, QueryIndexType.Sorted,
                            attr.IndexGroups));

                    ScanAttributes(memberInfo.Value, fields, indexes, columnName, visitedTypes);
                }

                foreach (var attr in customAttributes.OfType<QueryTextFieldAttribute>())
                {
                    var columnName = attr.Name ?? memberInfo.Key.Name;

                    // No dot notation for FullText index names
                    indexes.Add(new QueryIndexEx(columnName, false, QueryIndexType.FullText, null));

                    if (parentPropName != null)
                        columnName = parentPropName + "." + columnName;

                    fields.Add(new QueryField(columnName, memberInfo.Value));

                    ScanAttributes(memberInfo.Value, fields, indexes, columnName, visitedTypes);
                }
            }

            visitedTypes.Remove(type);
        }

        /// <summary>
        /// Gets the fields and properties.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <returns></returns>
        private static IEnumerable<KeyValuePair<MemberInfo, Type>> GetFieldsAndProperties(Type type)
        {
            Debug.Assert(type != null);

            if (type.IsPrimitive)
                yield break;

            var bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance |
                               BindingFlags.DeclaredOnly;

            while (type != typeof (object) && type != null)
            {
                foreach (var fieldInfo in type.GetFields(bindingFlags))
                    yield return new KeyValuePair<MemberInfo, Type>(fieldInfo, fieldInfo.FieldType);

                foreach (var propertyInfo in type.GetProperties(bindingFlags))
                    yield return new KeyValuePair<MemberInfo, Type>(propertyInfo, propertyInfo.PropertyType);

                type = type.BaseType;
            }
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
