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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Object written with Java OptimizedMarshaller.
    /// We just hold it as a byte array.
    /// </summary>
    internal class OptimizedMarshallerObject
    {
        /** */
        private readonly byte[] _data;

        /// <summary>
        /// Initializes a new instance of the <see cref="OptimizedMarshallerObject"/> class.
        /// </summary>
        public OptimizedMarshallerObject(IBinaryStream stream)
        {
            Debug.Assert(stream != null);

            _data = stream.ReadByteArray(stream.ReadInt());
        }

        /// <summary>
        /// Writes to the specified writer.
        /// </summary>
        public void Write(IBinaryStream stream)
        {
            Debug.Assert(stream != null);

            stream.WriteByte(BinaryTypeId.OptimizedMarshaller);
            stream.WriteInt(_data.Length);
            stream.WriteByteArray(_data);
        }
    }
}
