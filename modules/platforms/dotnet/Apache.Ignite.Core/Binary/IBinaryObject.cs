/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
