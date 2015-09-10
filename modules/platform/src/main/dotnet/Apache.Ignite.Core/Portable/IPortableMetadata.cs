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
    using System.Collections.Generic;

    /// <summary>
    /// Portable type metadata.
    /// </summary>
    public interface IPortableMetadata
    {
        /// <summary>
        /// Gets type name.
        /// </summary>
        /// <returns>Type name.</returns>
        string TypeName
        {
            get;
        }

        /// <summary>
        /// Gets field names for that type.
        /// </summary>
        /// <returns>Field names.</returns>
        ICollection<string> Fields
        {
            get;
        }

        /// <summary>
        /// Gets field type for the given field name.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Field type.</returns>
        string FieldTypeName(string fieldName);

        /// <summary>
        /// Gets optional affinity key field name.
        /// </summary>
        /// <returns>Affinity key field name or null in case it is not provided.</returns>
        string AffinityKeyFieldName
        {
            get;
        }
    }
}
