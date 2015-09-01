/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cluster
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Collections;
    using GridGain.Cluster;
    using GridGain.Portable;

    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;

    /// <summary>
    /// Cluster node implementation.
    /// </summary>
    internal class ClusterNodeImpl : IClusterNode
    {
        /** Node ID. */
        private readonly Guid id;

        /** Attributes. */
        private readonly IDictionary<string, object> attrs;

        /** Addresses. */
        private readonly ICollection<string> addrs;

        /** Hosts. */
        private readonly ICollection<string> hosts;

        /** Order. */
        private readonly long order;

        /** Local flag. */
        private readonly bool local;

        /** Daemon flag. */
        private readonly bool daemon;

        /** Metrics. */
        private volatile ClusterMetricsImpl metrics;
        
        /** Grid. */
        private WeakReference gridRef;

        /// <summary>
        /// Initializes a new instance of the <see cref="ClusterNodeImpl"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ClusterNodeImpl(IPortableRawReader reader)
        {
            id = reader.ReadGuid() ?? default(Guid);

            attrs = reader.ReadGenericDictionary<string, object>().AsReadOnly();
            addrs = reader.ReadGenericCollection<string>().AsReadOnly();
            hosts = reader.ReadGenericCollection<string>().AsReadOnly();
            order = reader.ReadLong();
            local = reader.ReadBoolean();
            daemon = reader.ReadBoolean();

            metrics = reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
        }

        /** <inheritDoc /> */
        public Guid Id
        {
            get { return id; }
        }

        /** <inheritDoc /> */
        public T Attribute<T>(string name)
        {
            A.NotNull(name, "name");

            return (T)attrs[name];
        }

        /** <inheritDoc /> */
        public bool TryGetAttribute<T>(string name, out T attr)
        {
            A.NotNull(name, "name");

            object val;

            if (attrs.TryGetValue(name, out val))
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
            return attrs;
        }

        /** <inheritDoc /> */
        public ICollection<string> Addresses
        {
            get
            {
                return addrs;
            }
        }

        /** <inheritDoc /> */
        public ICollection<string> HostNames
        {
            get
            {
                return hosts;
            }
        }

        /** <inheritDoc /> */
        public long Order
        {
            get
            {
                return order;
            }
        }

        /** <inheritDoc /> */
        public bool IsLocal
        {
            get
            {
                return local;
            }
        }

        /** <inheritDoc /> */
        public bool IsDaemon
        {
            get
            {
                return daemon;
            }
        }

        /** <inheritDoc /> */
        public IClusterMetrics Metrics()
        {
            var grid = (GridImpl)gridRef.Target;

            if (grid == null)
                return metrics;

            ClusterMetricsImpl oldMetrics = metrics;

            long lastUpdateTime = oldMetrics.LastUpdateTimeRaw;

            ClusterMetricsImpl newMetrics = grid.ClusterGroup.RefreshClusterNodeMetrics(id, lastUpdateTime);

            if (newMetrics != null)
            {
                lock (this)
                {
                    if (metrics.LastUpdateTime < newMetrics.LastUpdateTime)
                        metrics = newMetrics;
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
                return id.Equals(node.id);

            return false;
        }

        /** <inheritDoc /> */
        public override int GetHashCode()
        {
            // ReSharper disable once NonReadonlyMemberInGetHashCode
            return id.GetHashCode();
        }

        /// <summary>
        /// Initializes this instance with a grid.
        /// </summary>
        /// <param name="grid">The grid.</param>
        internal void Init(GridImpl grid)
        {
            gridRef = new WeakReference(grid);
        }
    }
}
