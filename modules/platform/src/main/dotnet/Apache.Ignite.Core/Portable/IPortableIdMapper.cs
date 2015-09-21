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

namespace Apache.Ignite.Core.Portable
{
    /// <summary>
    /// Maps class name and class field names to integer identifiers.
    /// </summary>
    public interface IPortableIdMapper
    {
        /// <summary>
        /// Gets type ID for the given type.
        /// </summary>
        /// <param name="typeName">Full type name.</param>
        /// <returns>ID of the class or 0 in case hash code is to be used.</returns>
        int TypeId(string typeName);

        /// <summary>
        /// Gets field ID for the given field of the given class.
        /// </summary>
        /// <param name="typeId">Type ID.</param>
        /// <param name="fieldName">Field name.</param>
        /// <returns>ID of the field or null in case hash code is to be used.</returns>
        int FieldId(int typeId, string fieldName);
    }
}
