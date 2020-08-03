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

namespace Apache.Ignite.Core.Impl.Client.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cluster;

    /// <summary>
    /// Client cluster node implementation.
    /// </summary>
    internal class ClientClusterNode : IClientClusterNode
    {
        /** Node ID. */
        private readonly Guid _id;

        /** Attributes. */
        private readonly IDictionary<string, object> _attrs;

        /** Addresses. */
        private readonly ICollection<string> _addrs;

        /** Hosts. */
        private readonly ICollection<string> _hosts;

        /** Order. */
        private readonly long _order;

        /** Local flag. */
        private readonly bool _isLocal;

        /** Daemon flag. */
        private readonly bool _isDaemon;

        /** Client flag. */
        private readonly bool _isClient;

        /** Consistent id. */
        private readonly object _consistentId;

        /** Ignite version. */
        private readonly IgniteProductVersion _version;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClientClusterNode"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ClientClusterNode(IBinaryRawReader reader)
        {
            var id = reader.ReadGuid();

            Debug.Assert(id.HasValue);

            _id = id.Value;

            _attrs = ClusterNodeImpl.ReadAttributes(reader);
            _addrs = reader.ReadCollectionAsList<string>().AsReadOnly();
            _hosts = reader.ReadCollectionAsList<string>().AsReadOnly();
            _order = reader.ReadLong();
            _isLocal = reader.ReadBoolean();
            _isDaemon = reader.ReadBoolean();
            _isClient = reader.ReadBoolean();
            _consistentId = reader.ReadObject<object>();
            _version = new IgniteProductVersion(reader);
        }

        /** <inheritDoc /> */
        public Guid Id
        {
            get { return _id; }
        }

        /** <inheritDoc /> */
        public ICollection<string> Addresses
        {
            get { return _addrs; }
        }

        /** <inheritDoc /> */
        public ICollection<string> HostNames
        {
            get { return _hosts; }
        }

        /** <inheritDoc /> */
        public long Order
        {
            get { return _order; }
        }

        /** <inheritDoc /> */
        public bool IsLocal
        {
            get { return _isLocal; }
        }

        /** <inheritDoc /> */
        public bool IsDaemon
        {
            get { return _isDaemon; }
        }

        /** <inheritDoc /> */
        public IgniteProductVersion Version
        {
            get { return _version; }
        }

        /** <inheritDoc /> */
        public object ConsistentId
        {
            get { return _consistentId; }
        }

        /** <inheritDoc /> */
        public IDictionary<string, object> Attributes
        {
            get { return new Dictionary<string, object>(_attrs); }
        }

        /** <inheritDoc /> */
        public bool IsClient
        {
            get { return _isClient; }
        }

        /** <inheritDoc /> */
        public override string ToString()
        {
            return "GridNode [id=" + Id + ']';
        }

        /** <inheritDoc /> */
        public override bool Equals(object obj)
        {
            var node = obj as ClientClusterNode;

            if (node != null)
                return _id.Equals(node._id);

            return false;
        }

        /** <inheritDoc /> */
        public override int GetHashCode()
        {
            // ReSharper disable once NonReadonlyMemberInGetHashCode
            return _id.GetHashCode();
        }
    }
}