/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Services
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Collections;
    using GridGain.Impl.Portable;
    using GridGain.Services;

    /// <summary>
    /// Service descriptor.
    /// </summary>
    internal class ServiceDescriptor : IServiceDescriptor
    {
        /** Services. */
        private readonly IServices services;

        /** Service type. */
        private Type type;

        /// <summary>
        /// Initializes a new instance of the <see cref="ServiceDescriptor" /> class.
        /// </summary>
        /// <param name="name">Name.</param>
        /// <param name="reader">Reader.</param>
        /// <param name="services">Services.</param>
        public ServiceDescriptor(string name, PortableReaderImpl reader, IServices services)
        {
            Debug.Assert(reader != null);
            Debug.Assert(services != null);
            Debug.Assert(!string.IsNullOrEmpty(name));

            this.services = services;
            Name = name;

            CacheName = reader.ReadString();
            MaxPerNodeCount = reader.ReadInt();
            TotalCount = reader.ReadInt();
            OriginNodeId = reader.ReadGuid() ?? Guid.Empty;
            AffinityKey = reader.ReadObject<object>();

            var mapSize = reader.ReadInt();
            var snap = new Dictionary<Guid, int>(mapSize);

            for (var i = 0; i < mapSize; i++)
                snap[reader.ReadGuid() ?? Guid.Empty] = reader.ReadInt();

            TopologySnapshot = snap.AsReadOnly();
        }

        /** <inheritdoc /> */
        public string Name { get; private set; }
        
        /** <inheritdoc /> */
        public Type Type
        {
            get
            {
                try
                {
                    return type ?? (type = services.GetServiceProxy<IService>(Name).GetType());
                }
                catch (Exception ex)
                {
                    throw new ServiceInvocationException(
                        "Failed to retrieve service type. It has either been cancelled, or is not a .Net service", ex);
                }
            }
        }

        /** <inheritdoc /> */
        public int TotalCount { get; private set; }

        /** <inheritdoc /> */
        public int MaxPerNodeCount { get; private set; }

        /** <inheritdoc /> */
        public string CacheName { get; private set; }

        /** <inheritdoc /> */
        public object AffinityKey { get; private set; }

        /** <inheritdoc /> */
        public Guid OriginNodeId { get; private set; }

        /** <inheritdoc /> */
        public IDictionary<Guid, int> TopologySnapshot { get; private set; }
    }
}