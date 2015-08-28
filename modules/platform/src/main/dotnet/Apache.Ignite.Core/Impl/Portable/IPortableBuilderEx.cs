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
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Internal builder interface.
    /// </summary>
    public interface IPortableBuilderEx : IPortableBuilder
    {
        /// <summary>
        /// Create child builder.
        /// </summary>
        /// <param name="obj">Portable object.</param>
        /// <returns>Child builder.</returns>
        IPortableBuilderEx Child(IPortableUserObject obj);

        /// <summary>
        /// Get cache field.
        /// </summary>
        /// <param name="pos">Position.</param>
        /// <param name="val">Value.</param>
        /// <returns><c>true</c> if value is found in cache.</returns>
        bool CachedField<T>(int pos, out T val);

        /// <summary>
        /// Add field to cache test.
        /// </summary>
        /// <param name="pos">Position.</param>
        /// <param name="val">Value.</param>
        void CacheField(int pos, object val);

        /// <summary>
        /// Mutate portable object.
        /// </summary>
        /// <param name="inStream">Input stream with initial object.</param>
        /// <param name="outStream">Output stream.</param>
        /// <param name="desc">Portable type descriptor.</param>
        /// <param name="hashCode">Hash code.</param>
        /// <param name="vals">Values.</param>
        void Mutate(
            PortableHeapStream inStream,
            PortableHeapStream outStream,
            IPortableTypeDescriptor desc,
            int hashCode, 
            IDictionary<string, IPortableBuilderField> vals);

        /// <summary>
        /// Process portable object inverting handles if needed.
        /// </summary>
        /// <param name="outStream">Output stream.</param>
        /// <param name="port">Portable.</param>
        void ProcessPortable(IPortableStream outStream, IPortableUserObject port);

        /// <summary>
        /// Process child builder.
        /// </summary>
        /// <param name="outStream">Output stream.</param>
        /// <param name="builder">Builder.</param>
        void ProcessBuilder(IPortableStream outStream, IPortableBuilderEx builder);
    }
}