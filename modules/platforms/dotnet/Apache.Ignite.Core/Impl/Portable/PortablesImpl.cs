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
    using System.Collections.Generic;
    using System.IO;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Portables implementation.
    /// </summary>
    internal class PortablesImpl : IPortables
    {
        /** Owning grid. */
        private readonly PortableMarshaller _marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        internal PortablesImpl(PortableMarshaller marsh)
        {
            _marsh = marsh;
        }

        /** <inheritDoc /> */
        public T ToPortable<T>(object obj)
        {
            if (obj is IPortableObject)
                return (T)obj;

            IPortableStream stream = new PortableHeapStream(1024);

            // Serialize.
            BinaryWriterImpl writer = _marsh.StartMarshal(stream);

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

            return _marsh.Unmarshal<T>(stream, PortableMode.ForcePortable);
        }

        /** <inheritDoc /> */
        public IPortableBuilder GetBuilder(Type type)
        {
            IgniteArgumentCheck.NotNull(type, "type");

            IBinaryTypeDescriptor desc = _marsh.GetDescriptor(type);

            if (desc == null)
                throw new IgniteException("Type is not portable (add it to PortableConfiguration): " + 
                    type.FullName);

            return Builder0(null, PortableFromDescriptor(desc), desc);
        }

        /** <inheritDoc /> */
        public IPortableBuilder GetBuilder(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            IBinaryTypeDescriptor desc = _marsh.GetDescriptor(typeName);
            
            return Builder0(null, PortableFromDescriptor(desc), desc);
        }

        /** <inheritDoc /> */
        public IPortableBuilder GetBuilder(IPortableObject obj)
        {
            IgniteArgumentCheck.NotNull(obj, "obj");

            PortableUserObject obj0 = obj as PortableUserObject;

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
        public ICollection<IPortableMetadata> GetMetadata()
        {
            return Marshaller.Ignite.ClusterGroup.Metadata();
        }

        /** <inheritDoc /> */
        public IPortableMetadata GetMetadata(int typeId)
        {
            return Marshaller.GetMetadata(typeId);
        }

        /** <inheritDoc /> */
        public IPortableMetadata GetMetadata(string typeName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(typeName, "typeName");

            return GetMetadata(GetTypeId(typeName));
        }

        /** <inheritDoc /> */
        public IPortableMetadata GetMetadata(Type type)
        {
            IgniteArgumentCheck.NotNull(type, "type");

            var desc = Marshaller.GetDescriptor(type);

            return desc == null ? null : Marshaller.GetMetadata(desc.TypeId);
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        internal PortableMarshaller Marshaller
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
        private PortableUserObject PortableFromDescriptor(IBinaryTypeDescriptor desc)
        {
            var len = PortableObjectHeader.Size;

            var hdr = new PortableObjectHeader(desc.UserType, desc.TypeId, 0, len, 0, len, true, 0);

            var stream = new PortableHeapStream(len);

            PortableObjectHeader.Write(hdr, stream, 0);

            return new PortableUserObject(_marsh, stream.InternalArray, 0, hdr);
        }

        /// <summary>
        /// Internal builder creation routine.
        /// </summary>
        /// <param name="parent">Parent builder.</param>
        /// <param name="obj">Portable object.</param>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>Builder.</returns>
        private PortableBuilderImpl Builder0(PortableBuilderImpl parent, PortableUserObject obj, 
            IBinaryTypeDescriptor desc)
        {
            return new PortableBuilderImpl(this, parent, obj, desc);
        }
    }
}
