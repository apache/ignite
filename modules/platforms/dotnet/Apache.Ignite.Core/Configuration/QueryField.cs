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
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Represents a queryable field.
    /// </summary>
    public class QueryField
    {
        /// <summary>
        /// Gets or sets the field name.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets the Java type name.
        /// </summary>
        public string TypeName { get; set; }

        /// <summary>
        /// Gets or sets the type of the field.
        /// This is a shortcut for <see cref="TypeName"/>. 
        /// Getter will return null for non-primitive types.
        /// </summary>
        public Type Type
        {
            get { return JavaTypes.GetDotNetType(TypeName); }
            set
            {
                TypeName = value == null
                    ? null
                    : (JavaTypes.GetJavaTypeName(value) ?? BinaryUtils.GetTypeName(value));
            }
        }
    }
}