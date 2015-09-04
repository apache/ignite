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
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portable serializer for system types.
    /// </summary>
    /// <typeparam name="T">Object type.</typeparam>
    internal class PortableSystemTypeSerializer<T> : IPortableSystemTypeSerializer where T : IPortableWriteAware
    {
        /** Ctor delegate. */
        private readonly Func<PortableReaderImpl, T> _ctor;

        /// <summary>
        /// Initializes a new instance of the <see cref="PortableSystemTypeSerializer{T}"/> class.
        /// </summary>
        /// <param name="ctor">Constructor delegate.</param>
        public PortableSystemTypeSerializer(Func<PortableReaderImpl, T> ctor)
        {
            Debug.Assert(ctor != null);

            _ctor = ctor;
        }

        /** <inheritdoc /> */
        public void WritePortable(object obj, IPortableWriter writer)
        {
            ((T) obj).WritePortable(writer);
        }

        /** <inheritdoc /> */
        public void ReadPortable(object obj, IPortableReader reader)
        {
            throw new NotSupportedException("System serializer does not support ReadPortable.");
        }

        /** <inheritdoc /> */
        public object ReadInstance(PortableReaderImpl reader)
        {
            return _ctor(reader);
        }
    }
}