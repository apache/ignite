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

namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Text;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Base binary name mapper implementation.
    /// </summary>
    public class BinaryBasicNameMapper : IBinaryNameMapper
    {
        // TODO: Dedicated fixture.

        /// <summary>
        /// The simple name instance.
        /// </summary>
        internal static readonly BinaryBasicNameMapper SimpleNameInstance = new BinaryBasicNameMapper
        {
            IsSimpleName = true
        };

        /// <summary>
        /// Gets or sets a value indicating whether this instance maps to simple type names.
        /// </summary>
        public bool IsSimpleName { get; set; }

        /// <summary>
        /// Gets the type name.
        /// </summary>
        public string GetTypeName(string name)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "typeName");

            var nameFunc = IsSimpleName
                ? (Func<TypeNameParser, string>) (x => x.GetName())
                : (x => x.GetFullName());

            return BuildTypeName(TypeNameParser.Parse(name), new StringBuilder(), nameFunc).ToString();
        }

        /// <summary>
        /// Gets the field name.
        /// </summary>
        public string GetFieldName(string name)
        {
            return name;
        }

        /// <summary>
        /// Gets the simple type name, without namespace and assembly.
        /// </summary>
        private static string GetSimpleTypeName(string typeName)
        {

        }

        /// <summary>
        /// Gets the full type name, with namespace and assembly, without assembly version.
        /// </summary>
        private static string GetFullTypeName(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return BuildTypeName(TypeNameParser.Parse(typeName), new StringBuilder(), x => x.GetFullName()).ToString();
        }

        /// <summary>
        /// Builds the type name.
        /// </summary>
        private static StringBuilder BuildTypeName(TypeNameParser typeName, StringBuilder sb, 
            Func<TypeNameParser, string> typeNameFunc)
        {
            sb.Append(typeNameFunc(typeName));

            var generics = typeName.Generics;

            if (generics != null)
            {
                sb.Append(typeName.GetGenericHeader()).Append('[');

                foreach (var genArg in generics)
                {
                    sb.Append('[');

                    BuildTypeName(genArg, sb, typeNameFunc);

                    sb.Append(']');
                }

                sb.Append(']');
            }

            sb.Append(typeName.GetArray());

            return sb;
        }
    }
}
