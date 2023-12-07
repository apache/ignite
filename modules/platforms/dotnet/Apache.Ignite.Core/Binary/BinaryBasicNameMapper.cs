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
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Text;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Base binary name mapper implementation.
    /// </summary>
    public class BinaryBasicNameMapper : IBinaryNameMapper
    {
        /// <summary>
        /// The simple name instance.
        /// </summary>
        internal static readonly BinaryBasicNameMapper SimpleNameInstance = new BinaryBasicNameMapper
        {
            IsSimpleName = true
        };

        /// <summary>
        /// The full name instance.
        /// </summary>
        internal static readonly BinaryBasicNameMapper FullNameInstance = new BinaryBasicNameMapper();

        /// <summary>
        /// Gets or sets a value indicating whether this instance maps to simple type names.
        /// </summary>
        public bool IsSimpleName { get; set; }

        /// <summary>
        /// Gets or sets the prefix to be added to the full type name.
        /// For example, Java package names usually begin with <c>org.</c> or <c>com.</c>.
        /// <para />
        /// In combination with <see cref="NamespaceToLower"/>, we can map .NET type name <c>Apache.Ignite.Foo</c>
        /// to a corresponding Java type name <c>org.apache.ignite.Foo</c>, conforming to the naming conventions
        /// for both languages.
        /// </summary>
        public string NamespacePrefix { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance converts the namespace part to the lower case.
        /// For example, Java package names are usually lowercase.
        /// <para />
        /// In combination with <see cref="NamespacePrefix"/>, we can map .NET type name <c>Apache.Ignite.Foo</c>
        /// to a corresponding Java type name <c>org.apache.ignite.Foo</c>, conforming to the naming conventions
        /// for both languages.
        /// </summary>
        public bool NamespaceToLower { get; set; }

        /// <summary>
        /// Gets the type name.
        /// </summary>
        public string GetTypeName(string name)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "typeName");

            var parsedName = TypeNameParser.Parse(name);

            if (parsedName.Generics == null)
            {
                // Generics are rare, use simpler logic for the common case.
                return GetTypeName(parsedName) + parsedName.GetArray();
            }

            return BuildTypeName(parsedName, new StringBuilder()).ToString();
        }

        /// <summary>
        /// Gets the field name.
        /// </summary>
        public string GetFieldName(string name)
        {
            return name;
        }

        /// <summary>
        /// Builds the type name.
        /// </summary>
        private StringBuilder BuildTypeName(TypeNameParser typeName, StringBuilder sb)
        {
            sb.Append(GetTypeName(typeName));

            var generics = typeName.Generics;

            if (generics != null && generics.Count > 0)  // Generics are non-null but empty when unbound.
            {
                sb.Append('[');

                var first = true;

                foreach (var genArg in generics)
                {
                    if (first)
                    {
                        first = false;
                    }
                    else
                    {
                        sb.Append(',');
                    }

                    sb.Append('[');

                    BuildTypeName(genArg, sb);

                    sb.Append(']');
                }

                sb.Append(']');
            }

            sb.Append(typeName.GetArray());

            return sb;
        }

        /// <summary>
        /// Gets the type name from the parser.
        /// </summary>
        [SuppressMessage("Microsoft.Globalization", "CA1308:NormalizeStringsToUppercase",
            Justification = "Not applicable, lower case is required.")]
        private string GetTypeName(TypeNameParser name)
        {
            if (IsSimpleName)
                return name.GetName();

            var fullName = NamespaceToLower && name.HasNamespace()
                ? name.GetNamespace().ToLower(CultureInfo.InvariantCulture) + name.GetName()
                : name.GetNameWithNamespace();

            return NamespacePrefix + fullName;
        }
    }
}
