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

namespace Apache.Ignite.Core.Impl.Cluster
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Collections;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Cluster node implementation.
    /// </summary>
    internal class ClusterNodeImpl : IClusterNode
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
        private readonly bool _local;

        /** Daemon flag. */
        private readonly bool _daemon;

        /** Metrics. */
        private volatile ClusterMetricsImpl _metrics;
        
        /** Ignite reference. */
        private WeakReference _igniteRef;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterNodeImpl"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ClusterNodeImpl(IPortableRawReader reader)
        {
            _id = reader.ReadGuid() ?? default(Guid);

            _attrs = reader.ReadGenericDictionary<string, object>().AsReadOnly();
            _addrs = reader.ReadGenericCollection<string>().AsReadOnly();
            _hosts = reader.ReadGenericCollection<string>().AsReadOnly();
            _order = reader.ReadLong();
            _local = reader.ReadBoolean();
            _daemon = reader.ReadBoolean();

            _metrics = reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
        }

        /** <inheritDoc /> */
        public Guid Id
        {
            get { return _id; }
        }

        /** <inheritDoc /> */
        public T Attribute<T>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            return (T)_attrs[name];
        }

        /** <inheritDoc /> */
        public bool TryGetAttribute<T>(string name, out T attr)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            object val;

            if (_attrs.TryGetValue(name, out val))
            {
                attr = (T)val;

                return true;
            }
            attr = default(T);

            return false;
        }

        /** <inheritDoc /> */
        public IDictionary<string, object> Attributes()
        {
            return _attrs;
        }

        /** <inheritDoc /> */
        public ICollection<string> Addresses
        {
            get
            {
                return _addrs;
            }
        }

        /** <inheritDoc /> */
        public ICollection<string> HostNames
        {
            get
            {
                return _hosts;
            }
        }

        /** <inheritDoc /> */
        public long Order
        {
            get
            {
                return _order;
            }
        }

        /** <inheritDoc /> */
        public bool IsLocal
        {
            get
            {
                return _local;
            }
        }

        /** <inheritDoc /> */
        public bool IsDaemon
        {
            get
            {
                return _daemon;
            }
        }

        /** <inheritDoc /> */
        public IClusterMetrics Metrics()
        {
            var ignite = (Ignite)_igniteRef.Target;

            if (ignite == null)
                return _metrics;

            ClusterMetricsImpl oldMetrics = _metrics;

            long lastUpdateTime = oldMetrics.LastUpdateTimeRaw;

            ClusterMetricsImpl newMetrics = ignite.ClusterGroup.RefreshClusterNodeMetrics(_id, lastUpdateTime);

            if (newMetrics != null)
            {
                lock (this)
                {
                    if (_metrics.LastUpdateTime < newMetrics.LastUpdateTime)
                        _metrics = newMetrics;
                }

                return newMetrics;
            }

            return oldMetrics;
        }
        
        /** <inheritDoc /> */
        public override string ToString()
        {
            return "GridNode [id=" + Id + ']';
        }

        /** <inheritDoc /> */
        public override bool Equals(object obj)
        {
            ClusterNodeImpl node = obj as ClusterNodeImpl;

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

        /// <summary>
        /// Initializes this instance with a grid.
        /// </summary>
        /// <param name="grid">The grid.</param>
        internal void Init(Ignite grid)
        {
            _igniteRef = new WeakReference(grid);
        }
    }
}
