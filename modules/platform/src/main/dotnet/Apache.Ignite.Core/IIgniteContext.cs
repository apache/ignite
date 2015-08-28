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

namespace Apache.Ignite.Core
{
    using System;
    using Apache.Ignite.Core.Impl.Portable;

    /// <summary>
    /// Ignite extensibility context.
    /// </summary>
    public interface IIgniteContext
    {
        /// <summary>
        /// Converts an exception.
        /// All thrown exceptions should be passed through this method.
        /// Provides custom error logic when overridden in a derived class.
        /// </summary>
        /// <param name="exception">The exception to convert.</param>
        Exception ConvertException(Exception exception);

        /// <summary>
        /// Creates the serializable object holder.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>Object holder.</returns>
        object CreateSerializableObjectHolder(object obj);

        /// <summary>
        /// Unwraps the serializable object holder.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>Wrapped object.</returns>
        object UnwrapSerializableObjectHolder(object obj);

        /// <summary>
        /// Wraps an object during deserialization.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <returns>Unwrapped object.</returns>
        T WrapObjectOnRead<T>(object obj);

        /// <summary>
        /// Wraps an object during serialization.
        /// </summary>
        /// <param name="obj">The object.</param>
        /// <param name="result">The result.</param>
        /// <returns>
        /// True if wrapping happened; false otherwise.
        /// </returns>
        bool WrapObjectOnWrite<T>(T obj, out object result);

        /// <summary>
        /// Creates a portable object.
        /// </summary>
        /// <param name="marshaller">Marshaller.</param>
        /// <param name="offset">Offset.</param>
        /// <param name="bytes">Bytes.</param>
        /// <param name="id">Identifier.</param>
        /// <param name="hash">Hash code.</param>
        /// <returns>Portable user object.</returns>
        IPortableUserObject GetPortableObject(PortableMarshaller marshaller, int offset, byte[] bytes, int id, int hash);
    }
}