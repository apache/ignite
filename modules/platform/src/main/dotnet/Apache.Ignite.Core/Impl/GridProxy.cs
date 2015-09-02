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
        private readonly IIgnite grid;

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
            this.grid = grid;
        }

        /** <inheritdoc /> */
        public string Name
        {
            get
            {
                return grid.Name;
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
            return grid.Cluster.ForLocal();
        }

        /** <inheritdoc /> */
        public ICompute Compute()
        {
            return grid.Compute();
        }

        /** <inheritdoc /> */
        public ICompute Compute(IClusterGroup clusterGroup)
        {
            return clusterGroup.Compute();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            return grid.Cluster.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            return grid.Cluster.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
        {
            return grid.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(ICollection<Guid> ids)
        {
            return grid.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(params Guid[] ids)
        {
            return grid.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            return grid.Cluster.ForPredicate(p);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            return grid.Cluster.ForAttribute(name, val);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForCacheNodes(string name)
        {
            return grid.Cluster.ForCacheNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForDataNodes(string name)
        {
            return grid.Cluster.ForDataNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForClientNodes(string name)
        {
            return grid.Cluster.ForClientNodes(name);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRemotes()
        {
            return grid.Cluster.ForRemotes();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            return grid.Cluster.ForHost(node);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRandom()
        {
            return grid.Cluster.ForRandom();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForOldest()
        {
            return grid.Cluster.ForOldest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForYoungest()
        {
            return grid.Cluster.ForYoungest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForDotNet()
        {
            return grid.Cluster.ForDotNet();
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Nodes()
        {
            return grid.Cluster.Nodes();
        }

        /** <inheritdoc /> */
        public IClusterNode Node(Guid id)
        {
            return grid.Cluster.Node(id);
        }

        /** <inheritdoc /> */
        public IClusterNode Node()
        {
            return grid.Cluster.Node();
        }

        /** <inheritdoc /> */
        public IClusterMetrics Metrics()
        {
            return grid.Cluster.Metrics();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            grid.Dispose();
        }

        /** <inheritdoc /> */
        public ICache<K, V> Cache<K, V>(string name)
        {
            return grid.Cache<K, V>(name);
        }

        /** <inheritdoc /> */
        public ICache<K, V> GetOrCreateCache<K, V>(string name)
        {
            return grid.GetOrCreateCache<K, V>(name);
        }

        /** <inheritdoc /> */
        public ICache<K, V> CreateCache<K, V>(string name)
        {
            return grid.CreateCache<K, V>(name);
        }

        /** <inheritdoc /> */
        public IClusterNode LocalNode
        {
            get
            {
                return grid.Cluster.LocalNode;
            }
        }

        /** <inheritdoc /> */
        public bool PingNode(Guid nodeId)
        {
            return grid.Cluster.PingNode(nodeId);
        }

        /** <inheritdoc /> */
        public long TopologyVersion
        {
            get { return grid.Cluster.TopologyVersion; }
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Topology(long ver)
        {
            return grid.Cluster.Topology(ver);
        }

        /** <inheritdoc /> */
        public void ResetMetrics()
        {
            grid.Cluster.ResetMetrics();
        }

        /** <inheritdoc /> */
        public IDataStreamer<K, V> DataStreamer<K, V>(string cacheName)
        {
            return grid.DataStreamer<K, V>(cacheName);
        }

        /** <inheritdoc /> */
        public IPortables Portables()
        {
            return grid.Portables();
        }

        /** <inheritdoc /> */
        public ICacheAffinity Affinity(string name)
        {
            return grid.Affinity(name);
        }

        /** <inheritdoc /> */
        public ITransactions Transactions
        {
            get { return grid.Transactions; }
        }

        /** <inheritdoc /> */
        public IMessaging Message()
        {
            return grid.Message();
        }

        /** <inheritdoc /> */
        public IMessaging Message(IClusterGroup clusterGroup)
        {
            return grid.Message(clusterGroup);
        }

        /** <inheritdoc /> */
        public IEvents Events()
        {
            return grid.Events();
        }

        /** <inheritdoc /> */
        public IEvents Events(IClusterGroup clusterGroup)
        {
            return grid.Events(clusterGroup);
        }

        /** <inheritdoc /> */
        public IServices Services()
        {
            return grid.Services();
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
                return grid;
            }
        }

        /** <inheritdoc /> */
        public IPortableMetadata Metadata(int typeId)
        {
            return ((IClusterGroupEx)grid).Metadata(typeId);
        }
    }
}
