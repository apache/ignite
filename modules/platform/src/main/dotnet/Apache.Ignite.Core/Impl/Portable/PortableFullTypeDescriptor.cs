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
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Full type descriptor.
    /// </summary> 
    public class PortableFullTypeDescriptor : IPortableTypeDescriptor
    {
        /** Type. */
        private readonly Type type;

        /** Type ID. */
        private readonly int typeId;

        /** Type name. */
        private readonly string typeName;

        /** User type flag. */
        private readonly bool userType;

        /** Name converter. */
        private readonly IPortableNameMapper nameConverter;

        /** Mapper. */
        private readonly IPortableIdMapper mapper;

        /** Serializer. */
        private readonly IPortableSerializer serializer;

        /** Metadata enabled flag. */
        private readonly bool metaEnabled;

        /** Whether to cache deserialized value in IPortableObject */
        private readonly bool keepDeserialized;

        /** Affinity field key name. */
        private readonly string affKeyFieldName;

        /** Typed handler. */
        private readonly object typedHandler;

        /** Untyped handler. */
        private readonly PortableSystemWriteDelegate untypedHandler;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="type">Type.</param>
        /// <param name="typeId">Type ID.</param>
        /// <param name="typeName">Type name.</param>
        /// <param name="userType">User type flag.</param>
        /// <param name="nameConverter">Name converter.</param>
        /// <param name="mapper">Mapper.</param>
        /// <param name="serializer">Serializer.</param>
        /// <param name="metaEnabled">Metadata enabled flag.</param>
        /// <param name="keepDeserialized">Whether to cache deserialized value in IPortableObject</param>
        /// <param name="affKeyFieldName">Affinity field key name.</param>
        /// <param name="typedHandler">Typed handler.</param>
        /// <param name="untypedHandler">Untyped handler.</param>
        public PortableFullTypeDescriptor(
            Type type, 
            int typeId, 
            string typeName, 
            bool userType, 
            IPortableNameMapper nameConverter, 
            IPortableIdMapper mapper, 
            IPortableSerializer serializer, 
            bool metaEnabled, 
            bool keepDeserialized, 
            string affKeyFieldName, 
            object typedHandler,
            PortableSystemWriteDelegate untypedHandler)
        {
            this.type = type;
            this.typeId = typeId;
            this.typeName = typeName;
            this.userType = userType;
            this.nameConverter = nameConverter;
            this.mapper = mapper;
            this.serializer = serializer;
            this.metaEnabled = metaEnabled;
            this.keepDeserialized = keepDeserialized;
            this.affKeyFieldName = affKeyFieldName;
            this.typedHandler = typedHandler;
            this.untypedHandler = untypedHandler;
        }

        /// <summary>
        /// Type.
        /// </summary>
        public Type Type
        {
            get { return type; }
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        public int TypeId
        {
            get { return typeId; }
        }

        /// <summary>
        /// Type name.
        /// </summary>
        public string TypeName
        {
            get { return typeName; }
        }

        /// <summary>
        /// User type flag.
        /// </summary>
        public bool UserType
        {
            get { return userType; }
        }

        /// <summary>
        /// Metadata enabled flag.
        /// </summary>
        public bool MetadataEnabled
        {
            get { return metaEnabled; }
        }

        /// <summary>
        /// Whether to cache deserialized value in IPortableObject
        /// </summary>
        public bool KeepDeserialized
        {
            get { return keepDeserialized; }
        }

        /// <summary>
        /// Name converter.
        /// </summary>
        public IPortableNameMapper NameConverter
        {
            get { return nameConverter; }
        }

        /// <summary>
        /// Mapper.
        /// </summary>
        public IPortableIdMapper Mapper
        {
            get { return mapper; }
        }

        /// <summary>
        /// Serializer.
        /// </summary>
        public IPortableSerializer Serializer
        {
            get { return serializer; }
        }

        /// <summary>
        /// Affinity key field name.
        /// </summary>
        public string AffinityKeyFieldName
        {
            get { return affKeyFieldName; }
        }

        /// <summary>
        /// Typed handler.
        /// </summary>
        public object TypedHandler
        {
            get { return typedHandler; }
        }

        /// <summary>
        /// Untyped handler.
        /// </summary>
        public PortableSystemWriteDelegate UntypedHandler
        {
            get { return untypedHandler; }
        }
    }
}
