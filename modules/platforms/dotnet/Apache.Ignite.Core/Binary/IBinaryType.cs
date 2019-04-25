/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Binary
{
    using System.Collections.Generic;

    /// <summary>
    /// Binary type metadata.
    /// </summary>
    public interface IBinaryType
    {
        /// <summary>
        /// Gets type name.
        /// </summary>
        /// <returns>Type name.</returns>
        string TypeName { get; }

        /// <summary>
        /// Gets field names for that type.
        /// </summary>
        /// <returns>Field names.</returns>
        ICollection<string> Fields { get; }

        /// <summary>
        /// Gets field type for the given field name.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Field type.</returns>
        string GetFieldTypeName(string fieldName);

        /// <summary>
        /// Gets optional affinity key field name.
        /// </summary>
        /// <returns>Affinity key field name or null in case it is not provided.</returns>
        string AffinityKeyFieldName { get; }

        /// <summary>
        /// Gets a value indicating whether this type represents an enum.
        /// </summary>
        /// <value>   
        /// <c>true</c> if this instance represents an enum; otherwise, <c>false</c>.
        /// </value>
        bool IsEnum { get; }

        /// <summary>
        /// Gets the type identifier.
        /// </summary>
        int TypeId { get; }

        /// <summary>
        /// Gets the enum values.
        /// Only valid when <see cref="IsEnum"/> is true.
        /// </summary>
        IEnumerable<IBinaryObject> GetEnumValues();
    }
}
