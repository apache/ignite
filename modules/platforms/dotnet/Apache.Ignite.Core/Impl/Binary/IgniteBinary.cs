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
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Binary implementation.
    /// </summary>
    internal class IgniteBinary : IIgniteBinary
    {
        /** Owning grid. */
        private readonly Marshaller _marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        internal IgniteBinary(Marshaller marsh)
        {
            _marsh = marsh;
        }

        /** <inheritDoc /> */
        public T ToBinary<T>(object obj)
        {
            if (obj is IBinaryObject)
                return (T)obj;

            IBinaryStream stream = new BinaryHeapStream(1024);

            // Serialize.
            BinaryWriter writer = _marsh.StartMarshal(stream);

            try
            {
                writer.Write(obj);
            }
            finally
            {
                // Save metadata.
                _marsh.FinishMarshal(writer);
            }

            // Deserialize.
            stream.Seek(0, SeekOrigin.Begin);

            return _marsh.Unmarshal<T>(stream, BinaryMode.ForceBinary);
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder GetBuilder(Type type)
        {
            IgniteArgumentCheck.NotNull(type, "type");

            IBinaryTypeDescriptor desc = _marsh.GetDescriptor(type);

            if (desc == null)
                throw new IgniteException("Type is not portable (add it to PortableConfiguration): " + 
                    type.FullName);

            return Builder0(null, BinaryFromDescriptor(desc), desc);
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder GetBuilder(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            IBinaryTypeDescriptor desc = _marsh.GetDescriptor(typeName);
            
            return Builder0(null, BinaryFromDescriptor(desc), desc);
        }

        /** <inheritDoc /> */
        public IBinaryObjectBuilder GetBuilder(IBinaryObject obj)
        {
            IgniteArgumentCheck.NotNull(obj, "obj");

            BinaryObject obj0 = obj as BinaryObject;

            if (obj0 == null)
                throw new ArgumentException("Unsupported object type: " + obj.GetType());

            IBinaryTypeDescriptor desc = _marsh.GetDescriptor(true, obj0.TypeId);
            
            return Builder0(null, obj0, desc);
        }

        /** <inheritDoc /> */
        public int GetTypeId(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return Marshaller.GetDescriptor(typeName).TypeId;
        }

        /** <inheritDoc /> */
        public ICollection<IBinaryType> GetMetadata()
        {
            return Marshaller.Ignite.ClusterGroup.Metadata();
        }

        /** <inheritDoc /> */
        public IBinaryType GetMetadata(int typeId)
        {
            return Marshaller.GetMetadata(typeId);
        }

        /** <inheritDoc /> */
        public IBinaryType GetMetadata(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return GetMetadata(GetTypeId(typeName));
        }

        /** <inheritDoc /> */
        public IBinaryType GetMetadata(Type type)
        {
            IgniteArgumentCheck.NotNull(type, "type");

            var desc = Marshaller.GetDescriptor(type);

            return desc == null ? null : Marshaller.GetMetadata(desc.TypeId);
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        internal Marshaller Marshaller
        {
            get
            {
                return _marsh;
            }
        }

        /// <summary>
        /// Create empty portable object from descriptor.
        /// </summary>
        /// <param name="desc">Descriptor.</param>
        /// <returns>Empty portable object.</returns>
        private BinaryObject BinaryFromDescriptor(IBinaryTypeDescriptor desc)
        {
            var len = BinaryObjectHeader.Size;

            var hdr = new BinaryObjectHeader(desc.UserType, desc.TypeId, 0, len, 0, len, true, 0);

            var stream = new BinaryHeapStream(len);

            BinaryObjectHeader.Write(hdr, stream, 0);

            return new BinaryObject(_marsh, stream.InternalArray, 0, hdr);
        }

        /// <summary>
        /// Internal builder creation routine.
        /// </summary>
        /// <param name="parent">Parent builder.</param>
        /// <param name="obj">Portable object.</param>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>Builder.</returns>
        private BinaryObjectBuilder Builder0(BinaryObjectBuilder parent, BinaryObject obj, 
            IBinaryTypeDescriptor desc)
        {
            return new BinaryObjectBuilder(this, parent, obj, desc);
        }
    }
}
