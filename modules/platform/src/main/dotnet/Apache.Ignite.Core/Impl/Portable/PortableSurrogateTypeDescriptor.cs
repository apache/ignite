﻿/*
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
    /// Surrogate type descriptor. Used in cases when type if identified by name and is not provided in configuration.
    /// </summary>
    internal class PortableSurrogateTypeDescriptor : IPortableTypeDescriptor
    {
        /** Portable configuration. */
        private readonly PortableConfiguration cfg;

        /** Type ID. */
        private readonly int id;

        /** Type name. */
        private readonly string name;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Portable configuration.</param>
        /// <param name="id">Type ID.</param>
        public PortableSurrogateTypeDescriptor(PortableConfiguration cfg, int id)
        {
            this.cfg = cfg;
            this.id = id;
        }

        /// <summary>
        /// Constrcutor.
        /// </summary>
        /// <param name="cfg">Portable configuration.</param>
        /// <param name="name">Type name.</param>
        public PortableSurrogateTypeDescriptor(PortableConfiguration cfg, string name)
        {
            this.cfg = cfg;
            this.name = name;

            id = PortableUtils.TypeId(name, cfg.DefaultNameMapper, cfg.DefaultIdMapper);
        }

        /** <inheritDoc /> */
        public Type Type
        {
            get { return null; }
        }

        /** <inheritDoc /> */
        public int TypeId
        {
            get { return id; }
        }

        /** <inheritDoc /> */
        public string TypeName
        {
            get { return name; }
        }

        /** <inheritDoc /> */
        public bool UserType
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public bool MetadataEnabled
        {
            get { return cfg.DefaultMetadataEnabled; }
        }

        /** <inheritDoc /> */
        public bool KeepDeserialized
        {
            get { return cfg.DefaultKeepDeserialized; }
        }

        /** <inheritDoc /> */
        public IPortableNameMapper NameConverter
        {
            get { return cfg.DefaultNameMapper; }
        }

        /** <inheritDoc /> */
        public IPortableIdMapper Mapper
        {
            get { return cfg.DefaultIdMapper; }
        }

        /** <inheritDoc /> */
        public IPortableSerializer Serializer
        {
            get { return cfg.DefaultSerializer; }
        }

        /** <inheritDoc /> */
        public string AffinityKeyFieldName
        {
            get { return null; }
        }

        /** <inheritDoc /> */
        public object TypedHandler
        {
            get { return null; }
        }

        /** <inheritDoc /> */
        public PortableSystemWriteDelegate UntypedHandler
        {
            get { return null; }
        }
    }
}
