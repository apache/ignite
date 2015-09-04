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
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Portable object builder. Provides ability to build portable objects dynamically
    /// without having class definitions.
    /// <para />
    /// Note that type ID is required in order to build portable object. Usually it is
    /// enough to provide a simple type name and Ignite will generate the type ID
    /// automatically.
    /// </summary>
    public interface IPortableBuilder
    {
        /// <summary>
        /// Get object field value. If value is another portable object, then
        /// builder for this object will be returned. If value is a container
        /// for other objects (array, ICollection, IDictionary), then container
        /// will be returned with primitive types in deserialized form and
        /// portable objects as builders. Any change in builder or collection
        /// returned through this method will be reflected in the resulting
        /// portable object after build.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Field value.</returns>
        T GetField<T>(string fieldName);

        /// <summary>
        /// Set object field value. Value can be of any type including other
        /// <see cref="IPortableObject"/> and other builders.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Field value.</param>
        /// <returns>Current builder instance.</returns>
        IPortableBuilder SetField<T>(string fieldName, T val);

        /// <summary>
        /// Remove object field.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Current builder instance.</returns>
        IPortableBuilder RemoveField(string fieldName);

        /// <summary>
        /// Set explicit hash code. If builder creating object from scratch,
        /// then hash code initially set to 0. If builder is created from
        /// exising portable object, then hash code of that object is used
        /// as initial value.
        /// </summary>
        /// <param name="hashCode">Hash code.</param>
        /// <returns>Current builder instance.</returns>
        [SuppressMessage("Microsoft.Naming", "CA1719:ParameterNamesShouldNotMatchMemberNames", MessageId = "0#")]
        IPortableBuilder HashCode(int hashCode);

        /// <summary>
        /// Build the object.
        /// </summary>
        /// <returns>Resulting portable object.</returns>
        IPortableObject Build();
    }
}
