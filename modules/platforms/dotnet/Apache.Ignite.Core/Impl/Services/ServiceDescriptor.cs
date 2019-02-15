/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Collections;
    using Apache.Ignite.Core.Impl.Common;
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
        public ServiceDescriptor(string name, BinaryReader reader, IServices services)
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
            Platform = (Platform) reader.ReadByte();

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
                        "Failed to retrieve service type. It has either been cancelled, or is not a .NET service", ex);
                }
            }
        }

        /** <inheritdoc /> */
        public Platform Platform { get; private set; }

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