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

    /// <summary>
    /// Wrapper for serialized objects.
    /// </summary>
    public interface IBinaryObject
    {
        /// <summary>
        /// Gets binary object type ID.
        /// </summary>
        /// <value>
        /// Type ID.
        /// </value>
        int TypeId { get; }

        /// <summary>
        /// Gets object metadata.
        /// </summary>
        /// <returns>Metadata.</returns>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate",
            Justification = "Expensive operation.")]
        IBinaryType GetBinaryType();

        /// <summary>
        /// Gets field value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>
        /// Field value.
        /// </returns>
        TF GetField<TF>(string fieldName);

        /// <summary>
        /// Gets fully deserialized instance of binary object.
        /// </summary>
        /// <returns>
        /// Fully deserialized instance of binary object.
        /// </returns>
        T Deserialize<T>();
    }
}
