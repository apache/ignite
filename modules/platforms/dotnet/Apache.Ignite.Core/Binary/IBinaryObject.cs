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
        /// Determines whether the field with specified name exists in this instance.
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <returns>True if there is a field with specified name; false otherwise.</returns>
        bool HasField(string fieldName);

        /// <summary>
        /// Gets fully deserialized instance of binary object.
        /// </summary>
        /// <returns>
        /// Fully deserialized instance of binary object.
        /// </returns>
        T Deserialize<T>();

        /// <summary>
        /// Gets the value of underlying enum in int form.
        /// </summary>
        /// <value>
        /// The value of underlying enum in int form.
        /// </value>
        int EnumValue { get; }

        /// <summary>
        /// Gets the name of the underlying enum value.
        /// </summary>
        /// <value>
        /// The name of the enum value.
        /// </value>
        string EnumName { get; }

        /// <summary>
        /// Creates a new <see cref="IBinaryObjectBuilder"/> based on this object.
        /// <para />
        /// This is equivalent to <see cref="IBinary.GetBuilder(IBinaryObject)"/>.
        /// </summary>
        /// <returns>New <see cref="IBinaryObjectBuilder"/> based on this object.</returns>
        IBinaryObjectBuilder ToBuilder();
    }
}
