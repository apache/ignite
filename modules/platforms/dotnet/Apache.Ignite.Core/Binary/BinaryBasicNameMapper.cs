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
    /// <summary>
    /// Base binary name mapper implementation.
    /// </summary>
    public class BinaryBasicNameMapper : IBinaryNameMapper
    {
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
        public static string GetSimpleTypeName(string typeName)
        {
            // TODO
            return typeName;
        }

        /// <summary>
        /// Gets the full type name, with namespace and assembly, without assembly version.
        /// </summary>
        public static string GetFullTypeName(string typeName)
        {
            // TODO: 
            return typeName;
        }
    }
}
