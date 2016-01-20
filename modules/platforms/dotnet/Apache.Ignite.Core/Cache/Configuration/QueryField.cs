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
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Represents a queryable field.
    /// </summary>
    public class QueryField
    {
        /** */
        private Type _type;

        /** */
        private string _typeName;

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryField"/> class.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="javaTypeName">Java type name.</param>
        public QueryField(string name, string javaTypeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNullOrEmpty(javaTypeName, "typeName");

            Name = name;
            TypeName = javaTypeName;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="QueryField" /> class.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="type">Type.</param>
        public QueryField(string name, Type type)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");
            IgniteArgumentCheck.NotNull(type, "type");

            Name = name;
            TypeName = JavaTypes.GetJavaTypeName(type) ?? BinaryUtils.GetTypeName(type);
        }

        /// <summary>
        /// Gets the field name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the type of the value.
        /// <para />
        /// This is a shortcut for <see cref="TypeName"/>. Getter will return null for non-primitive types.
        /// </summary>
        public Type Type
        {
            get { return _type ?? JavaTypes.GetDotNetType(TypeName); }
            set
            {
                _type = value;

                TypeName = value == null
                    ? null
                    : (JavaTypes.GetJavaTypeName(value) ?? BinaryUtils.GetTypeName(value));
            }
        }

        /// <summary>
        /// Gets the Java type name.
        /// </summary>
        public string TypeName
        {
            get { return _typeName; }
            set
            {
                _typeName = value;
                _type = null;
            }
        }
    }
}