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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Grid proxy with fake serialization.
    /// </summary>
    [Serializable]
    internal class GridProxy : IIgnite, IClusterGroupEx, IPortableWriteAware, ICluster
    {
        /** */
        [NonSerialized]
        private readonly IIgnite _grid;

        /// <summary>
        /// Default ctor for marshalling.
        /// </summary>
        public GridProxy()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="grid">Grid.</param>
        public GridProxy(IIgnite grid)
        {
            this._grid = grid;
        }

        /** <inheritdoc /> */
        public string Name
        {
            get
            {
                return _grid.Name;
            }
        }

        /** <inheritdoc /> */
        public ICluster Cluster
        {
            get { return this; }
        }

        /** <inheritdoc /> */
        public IIgnite Grid
        {
            get
            {
                return this;
            }
        }

        /** <inheritdoc /> */
        public IClusterGroup ForLocal()
        {
            return _grid.Cluster.ForLocal();
        }

        /** <inheritdoc /> */
        public ICompute Compute()
        {
            return _grid.Compute();
        }

        /** <inheritdoc /> */
        public ICompute Compute(IClusterGroup clusterGroup)
        {
            return clusterGroup.Compute();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            return _grid.Cluster.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            return _grid.Cluster.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
        {
            return _grid.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(ICollection<Guid> ids)
        {
            return _grid.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(params Guid[] ids)
        {
            return _grid.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            return _grid.Cluster.ForPredicate(p);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            return _grid.Cluster.ForAttribute(name, val);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForCacheNodes(string name)
        {
            return _grid.Cluster.ForCacheNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForDataNodes(string name)
        {
            return _grid.Cluster.ForDataNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForClientNodes(string name)
        {
            return _grid.Cluster.ForClientNodes(name);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRemotes()
        {
            return _grid.Cluster.ForRemotes();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            return _grid.Cluster.ForHost(node);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRandom()
        {
            return _grid.Cluster.ForRandom();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForOldest()
        {
            return _grid.Cluster.ForOldest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForYoungest()
        {
            return _grid.Cluster.ForYoungest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForDotNet()
        {
            return _grid.Cluster.ForDotNet();
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Nodes()
        {
            return _grid.Cluster.Nodes();
        }

        /** <inheritdoc /> */
        public IClusterNode Node(Guid id)
        {
            return _grid.Cluster.Node(id);
        }

        /** <inheritdoc /> */
        public IClusterNode Node()
        {
            return _grid.Cluster.Node();
        }

        /** <inheritdoc /> */
        public IClusterMetrics Metrics()
        {
            return _grid.Cluster.Metrics();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            _grid.Dispose();
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> Cache<TK, TV>(string name)
        {
            return _grid.Cache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(string name)
        {
            return _grid.GetOrCreateCache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(string name)
        {
            return _grid.CreateCache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public IClusterNode LocalNode
        {
            get
            {
                return _grid.Cluster.LocalNode;
            }
        }

        /** <inheritdoc /> */
        public bool PingNode(Guid nodeId)
        {
            return _grid.Cluster.PingNode(nodeId);
        }

        /** <inheritdoc /> */
        public long TopologyVersion
        {
            get { return _grid.Cluster.TopologyVersion; }
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Topology(long ver)
        {
            return _grid.Cluster.Topology(ver);
        }

        /** <inheritdoc /> */
        public void ResetMetrics()
        {
            _grid.Cluster.ResetMetrics();
        }

        /** <inheritdoc /> */
        public IDataStreamer<TK, TV> DataStreamer<TK, TV>(string cacheName)
        {
            return _grid.DataStreamer<TK, TV>(cacheName);
        }

        /** <inheritdoc /> */
        public IPortables Portables()
        {
            return _grid.Portables();
        }

        /** <inheritdoc /> */
        public ICacheAffinity Affinity(string name)
        {
            return _grid.Affinity(name);
        }

        /** <inheritdoc /> */
        public ITransactions Transactions
        {
            get { return _grid.Transactions; }
        }

        /** <inheritdoc /> */
        public IMessaging Message()
        {
            return _grid.Message();
        }

        /** <inheritdoc /> */
        public IMessaging Message(IClusterGroup clusterGroup)
        {
            return _grid.Message(clusterGroup);
        }

        /** <inheritdoc /> */
        public IEvents Events()
        {
            return _grid.Events();
        }

        /** <inheritdoc /> */
        public IEvents Events(IClusterGroup clusterGroup)
        {
            return _grid.Events(clusterGroup);
        }

        /** <inheritdoc /> */
        public IServices Services()
        {
            return _grid.Services();
        }

        /** <inheritdoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            // No-op.
        }

        /// <summary>
        /// Target grid.
        /// </summary>
        internal IIgnite Target
        {
            get
            {
                return _grid;
            }
        }

        /** <inheritdoc /> */
        public IPortableMetadata Metadata(int typeId)
        {
            return ((IClusterGroupEx)_grid).Metadata(typeId);
        }
    }
}
