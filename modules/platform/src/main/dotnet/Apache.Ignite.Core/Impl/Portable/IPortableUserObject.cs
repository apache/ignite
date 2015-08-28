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

namespace Apache.Ignite.Core.Impl.Portable
{
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Extended portable object interface for internal usage.
    /// </summary>
    public interface IPortableUserObject : IPortableObject
    {
        /// <summary>
        /// Raw data of this portable object.
        /// </summary>
        byte[] Data { get; }

        /// <summary>
        /// Offset in data array.
        /// </summary>
        int Offset { get; }

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        PortableMarshaller Marshaller { get; }

        /// <summary>
        /// Get field with builder.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName"></param>
        /// <param name="builder"></param>
        /// <returns></returns>
        T Field<T>(string fieldName, PortableBuilderImpl builder);
    }
}