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
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Base binary name mapper implementation.
    /// </summary>
    public class BinaryBasicNameMapper : IBinaryNameMapper
    {
        /// <summary>
        /// The assembly name separator.
        /// </summary>
        private const char AssemblyNameSeparator = ',';

        /// <summary>
        /// The namespace separator.
        /// </summary>
        private const char NamespaceSeparator = '.';

        /// <summary>
        /// The nested class separator.
        /// </summary>
        private const char NestedTypeSeparator = '+';

        /// <summary>
        /// The generic separator.
        /// </summary>
        private const char GenericSeparator = ']';

        /// <summary>
        /// Gets or sets a value indicating whether this instance maps to simple type names.
        /// </summary>
        public bool IsSimpleName { get; set; }

        /// <summary>
        /// Gets the type name.
        /// </summary>
        public string GetTypeName(string name)
        {
            return IsSimpleName ? GetSimpleTypeName(name) : GetFullTypeName(name);
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
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            // Example of assembly-qualified name:
            // Apache.Ignite.Bar+Foo, Apache.Ignite, Version=2.0.0.0, Culture=neutral, PublicKeyToken=69a2bf55bb31d6b3

            var asmPos = typeName.IndexOf(AssemblyNameSeparator);

            if (asmPos < 0)
            {
                asmPos = typeName.Length;
            }

            var nsPos = Math.Max(
                typeName.LastIndexOf(NamespaceSeparator, asmPos - 1),
                typeName.LastIndexOf(NestedTypeSeparator, asmPos - 1));

            if (nsPos < 0)
            {
                nsPos = 0;
            }

            if (nsPos == 0 && asmPos == typeName.Length)
            {
                return typeName;
            }

            return typeName.Substring(nsPos + 1, asmPos - nsPos - 1);
        }

        /// <summary>
        /// Gets the full type name, with namespace and assembly, without assembly version.
        /// </summary>
        private static string GetFullTypeName(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            var asmPos = typeName.IndexOf(AssemblyNameSeparator);

            return asmPos < 0 ? typeName : typeName.Substring(0, asmPos);
        }
    }
}
