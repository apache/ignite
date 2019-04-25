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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Diagnostics;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// User-defined serializer wrapper.
    /// </summary>
    internal class UserSerializerProxy : IBinarySerializerInternal
    {
        /** */
        private readonly IBinarySerializer _serializer;

        /// <summary>
        /// Initializes a new instance of the <see cref="UserSerializerProxy"/> class.
        /// </summary>
        /// <param name="serializer">The serializer.</param>
        public UserSerializerProxy(IBinarySerializer serializer)
        {
            Debug.Assert(serializer != null);

            _serializer = serializer;
        }

        /** <inheritdoc /> */
        public void WriteBinary<T>(T obj, BinaryWriter writer)
        {
            _serializer.WriteBinary(obj, writer);
        }

        /** <inheritdoc /> */
        public T ReadBinary<T>(BinaryReader reader, IBinaryTypeDescriptor desc, int pos, Type typeOverride)
        {
            var obj = FormatterServices.GetUninitializedObject(typeOverride ?? desc.Type);

            reader.AddHandle(pos, obj);

            _serializer.ReadBinary(obj, reader);

            return (T) obj;
        }

        /** <inheritdoc /> */
        public bool SupportsHandles
        {
            get { return true; }
        }
    }
}
