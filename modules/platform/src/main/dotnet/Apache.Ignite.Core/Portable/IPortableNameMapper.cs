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
    /// Maps type and field names to different names.
    /// </summary>
    public interface IPortableNameMapper
    {
        /// <summary>
        /// Gets the type name.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>Type name.</returns>
        string TypeName(string name);

        /// <summary>
        /// Gets the field name.
        /// </summary>
        /// <param name="name">The name.</param>
        /// <returns>Field name.</returns>
        string FieldName(string name);
    }
}
