/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Portable
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;

    /// <summary>
    /// Portables implementation.
    /// </summary>
    internal class PortablesImpl : IPortables
    {
        /** Owning grid. */
        private readonly PortableMarshaller marsh;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="marsh">Marshaller.</param>
        internal PortablesImpl(PortableMarshaller marsh)
        {
            this.marsh = marsh;
        }

        /** <inheritDoc /> */
        public T ToPortable<T>(object obj)
        {
            if (obj is IPortableObject)
                return (T)obj;

            IPortableStream stream = new PortableHeapStream(1024);

            // Serialize.
            PortableWriterImpl writer = marsh.StartMarshal(stream);

            try
            {
                writer.Write(obj);
            }
            finally
            {
                // Save metadata.
                marsh.FinishMarshal(writer);
            }

            // Deserialize.
            stream.Seek(0, SeekOrigin.Begin);

            return marsh.Unmarshal<T>(stream, PortableMode.FORCE_PORTABLE);
        }

        /** <inheritDoc /> */
        public IPortableBuilder Builder(Type type)
        {
            A.NotNull(type, "type");

            IPortableTypeDescriptor desc = marsh.Descriptor(type);

            if (desc == null)
                throw new IgniteException("Type is not portable (add it to PortableConfiguration): " + 
                    type.FullName);

            return Builder0(null, PortableFromDescriptor(desc), desc);
        }

        /** <inheritDoc /> */
        public IPortableBuilder Builder(string typeName)
        {
            A.NotNullOrEmpty(typeName, "typeName");

            IPortableTypeDescriptor desc = marsh.Descriptor(typeName);
            
            return Builder0(null, PortableFromDescriptor(desc), desc);
        }

        /** <inheritDoc /> */
        public IPortableBuilder Builder(IPortableObject obj)
        {
            A.NotNull(obj, "obj");

            PortableUserObject obj0 = obj as PortableUserObject;

            if (obj0 == null)
                throw new ArgumentException("Unsupported object type: " + obj.GetType());

            IPortableTypeDescriptor desc = marsh.Descriptor(true, obj0.TypeId());
            
            return Builder0(null, obj0, desc);
        }

        /** <inheritDoc /> */
        public int GetTypeId(string typeName)
        {
            A.NotNullOrEmpty(typeName, "typeName");

            return Marshaller.Descriptor(typeName).TypeId;
        }

        /** <inheritDoc /> */
        public ICollection<IPortableMetadata> GetMetadata()
        {
            return Marshaller.Grid.ClusterGroup.Metadata();
        }

        /** <inheritDoc /> */
        public IPortableMetadata GetMetadata(int typeId)
        {
            return Marshaller.Metadata(typeId);
        }

        /** <inheritDoc /> */
        public IPortableMetadata GetMetadata(string typeName)
        {
            A.NotNullOrEmpty(typeName, "typeName");

            return GetMetadata(GetTypeId(typeName));
        }

        /** <inheritDoc /> */
        public IPortableMetadata GetMetadata(Type type)
        {
            A.NotNull(type, "type");

            var desc = Marshaller.Descriptor(type);

            return desc == null ? null : Marshaller.Metadata(desc.TypeId);
        }

        /// <summary>
        /// Create child builder.
        /// </summary>
        /// <param name="parent">Parent builder.</param>
        /// <param name="obj">Portable object.</param>
        /// <returns></returns>
        internal PortableBuilderImpl ChildBuilder(PortableBuilderImpl parent, PortableUserObject obj)
        {
            IPortableTypeDescriptor desc = marsh.Descriptor(true, obj.TypeId());

            return Builder0(null, obj, desc);
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        internal PortableMarshaller Marshaller
        {
            get
            {
                return marsh;
            }
        }

        /// <summary>
        /// Create empty portable object from descriptor.
        /// </summary>
        /// <param name="desc">Descriptor.</param>
        /// <returns>Empty portable object.</returns>
        private PortableUserObject PortableFromDescriptor(IPortableTypeDescriptor desc)
        {
            PortableHeapStream stream = new PortableHeapStream(18);

            stream.WriteByte(PortableUtils.HDR_FULL);
            stream.WriteBool(true);
            stream.WriteInt(desc.TypeId);
            stream.WriteInt(0); // Hash.
            stream.WriteInt(PortableUtils.FULL_HDR_LEN); // Length.
            stream.WriteInt(PortableUtils.FULL_HDR_LEN); // Raw data offset.

            return new PortableUserObject(marsh, stream.InternalArray, 0, desc.TypeId, 0);
        }

        /// <summary>
        /// Internal builder creation routine.
        /// </summary>
        /// <param name="parent">Parent builder.</param>
        /// <param name="obj">Portable object.</param>
        /// <param name="desc">Type descriptor.</param>
        /// <returns>Builder.</returns>
        private PortableBuilderImpl Builder0(PortableBuilderImpl parent, PortableUserObject obj, 
            IPortableTypeDescriptor desc)
        {
            return new PortableBuilderImpl(this, parent, obj, desc);
        }
    }
}
