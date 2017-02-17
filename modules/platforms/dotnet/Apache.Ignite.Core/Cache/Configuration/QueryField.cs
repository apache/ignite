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
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Log;

    /// <summary>
    /// Represents a queryable field.
    /// </summary>
    public class QueryField
    {
        /** */
        private Type _type;

        /** */
        private string _fieldTypeName;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryField"/> class.
        /// </summary>
        public QueryField()
        {
            // No-op.
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryField"/> class.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="javaFieldTypeName">Java type name.</param>
        public QueryField(string name, string javaFieldTypeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNullOrEmpty(javaFieldTypeName, "typeName");

            Name = name;
            FieldTypeName = javaFieldTypeName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryField" /> class.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="fieldType">Type.</param>
        public QueryField(string name, Type fieldType)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(fieldType, "type");

            Name = name;
            FieldType = fieldType;
        }

        /// <summary>
        /// Gets or sets the field name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the type of the value.
        /// <para />
        /// This is a shortcut for <see cref="FieldTypeName"/>. Getter will return null for non-primitive types.
        /// </summary>
        public Type FieldType
        {
            get { return _type ?? JavaTypes.GetDotNetType(FieldTypeName); }
            set
            {
                FieldTypeName = value == null
                    ? null
                    : (JavaTypes.GetJavaTypeName(value) ?? BinaryUtils.GetTypeName(value));

                _type = value;
            }
        }

        /// <summary>
        /// Gets the Java type name.
        /// </summary>
        public string FieldTypeName
        {
            get { return _fieldTypeName; }
            set
            {
                _fieldTypeName = value;
                _type = null;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether this field belongs to the cache key.
        /// Proper value here is required for SQL DML queries which create/modify cache keys.
        /// </summary>
        public bool IsKeyField { get; set; }

        /// <summary>
        /// Validates this instance and outputs information to the log, if necessary.
        /// </summary>
        internal void Validate(ILogger log, string logInfo)
        {
            Debug.Assert(log != null);
            Debug.Assert(logInfo != null);

            logInfo += string.Format(", QueryField '{0}'", Name);

            JavaTypes.LogIndirectMappingWarning(_type, log, logInfo);
        }
    }
}