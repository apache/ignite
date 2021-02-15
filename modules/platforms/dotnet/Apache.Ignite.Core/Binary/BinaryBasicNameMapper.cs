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
        /// Prefix to be added as a first part of Java type name.
        /// Java and .NET assumes usage of different naming conventions.
        /// Java package name first part is a domain: com, org, ru, etc. and second part is a company name: acme, company, etc.
        /// .NET naming conventions don't use domain and start with a company name.
        /// </summary>
        public string NamespacePrefix { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance maps names using standard Java naming conventions.
        /// E.g `Com.Company.Class` maps to `com.company.Class`.
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
                string res;
                
                if (IsSimpleName)
                    res = parsedName.GetName();
                else
                {
                    res = NamespaceToLower
                        ? DoForceJavaNamingConventions(parsedName, NamespacePrefix)
                        : parsedName.GetNameWithNamespace();
                }

                var arr = parsedName.GetArray();

                if (arr != null)
                {
                    res += arr;
                }

                return res;
            }

            Func<TypeNameParser, string> nameFunc;

            if (IsSimpleName)
                nameFunc = x => x.GetName();
            else 
            {
                nameFunc = NamespaceToLower 
                    ? (Func<TypeNameParser, string>)(x => DoForceJavaNamingConventions(x, NamespacePrefix))
                    : x => x.GetNameWithNamespace();
            }

            return BuildTypeName(parsedName, new StringBuilder(), nameFunc).ToString();
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
        private static StringBuilder BuildTypeName(TypeNameParser typeName, StringBuilder sb, 
            Func<TypeNameParser, string> typeNameFunc)
        {
            sb.Append(typeNameFunc(typeName));

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

                    BuildTypeName(genArg, sb, typeNameFunc);

                    sb.Append(']');
                }

                sb.Append(']');
            }

            sb.Append(typeName.GetArray());

            return sb;
        }

        /// <summary>
        /// Perform mapping to java naming convention e.g `Com.Company.Class` maps to `com.company.Class`.
        /// </summary>
        /// <param name="parsedName">Type name parser.</param>
        /// <param name="javaDomain">Java domain to be added.</param>
        /// <returns></returns>
        private static string DoForceJavaNamingConventions(TypeNameParser parsedName, string javaDomain)
        {
            var nameSpace = parsedName.GetNamespace();

            if (nameSpace == null)
                return javaDomain + parsedName.GetName();

            return javaDomain + nameSpace.ToLower(CultureInfo.InvariantCulture) + '.' + parsedName.GetName();
        }
    }
}
