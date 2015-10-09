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

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Collections;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Service descriptor.
    /// </summary>
    internal class ServiceDescriptor : IServiceDescriptor
    {
        /** Services. */
        private readonly IServices _services;

        /** Service type. */
        private Type _type;

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

            _services = services;
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
                    return _type ?? (_type = _services.GetServiceProxy<IService>(Name).GetType());
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