/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Portable
{
    using System;

    using GridGain.Portable;

    /// <summary>
    /// Type descriptor.
    /// </summary>
    internal interface IPortableTypeDescriptor
    {
        /// <summary>
        /// Type.
        /// </summary>
        Type Type
        {
            get;
        }

        /// <summary>
        /// Type ID.
        /// </summary>
        int TypeId
        {
            get;
        }

        /// <summary>
        /// Type name.
        /// </summary>
        string TypeName
        {
            get;
        }

        /// <summary>
        /// User type flag.
        /// </summary>
        bool UserType
        {
            get;
        }

        /// <summary>
        /// Metadata enabled flag.
        /// </summary>
        bool MetadataEnabled
        {
            get;
        }

        /// <summary>
        /// Whether to cache deserialized value in IPortableObject
        /// </summary>
        bool KeepDeserialized
        {
            get;
        }

        /// <summary>
        /// Name converter.
        /// </summary>
        IPortableNameMapper NameConverter
        {
            get;
        }

        /// <summary>
        /// Mapper.
        /// </summary>
        IPortableIdMapper Mapper
        {
            get;
        }

        /// <summary>
        /// Serializer.
        /// </summary>
        IPortableSerializer Serializer
        {
            get;
        }

        /// <summary>
        /// Affinity key field name.
        /// </summary>
        string AffinityKeyFieldName
        {
            get;
        }

        /// <summary>
        /// Typed handler.
        /// </summary>
        object TypedHandler
        {
            get;
        }

        /// <summary>
        /// Untyped handler.
        /// </summary>
        PortableSystemWriteDelegate UntypedHandler
        {
            get;
        }
    }
}
