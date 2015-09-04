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
    internal class IgniteProxy : IIgnite, IClusterGroupEx, IPortableWriteAware, ICluster
    {
        /** */
        [NonSerialized]
        private readonly IIgnite _ignite;

        /// <summary>
        /// Default ctor for marshalling.
        /// </summary>
        public IgniteProxy()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Grid.</param>
        public IgniteProxy(IIgnite ignite)
        {
            _ignite = ignite;
        }

        /** <inheritdoc /> */
        public string Name
        {
            get { return _ignite.Name; }
        }

        /** <inheritdoc /> */
        public ICluster Cluster
        {
            get { return this; }
        }

        /** <inheritdoc /> */
        public IIgnite Ignite
        {
            get { return this; }
        }

        /** <inheritdoc /> */
        public IClusterGroup ForLocal()
        {
            return _ignite.Cluster.ForLocal();
        }

        /** <inheritdoc /> */
        public ICompute Compute()
        {
            return _ignite.Compute();
        }

        /** <inheritdoc /> */
        public ICompute Compute(IClusterGroup clusterGroup)
        {
            return clusterGroup.Compute();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            return _ignite.Cluster.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            return _ignite.Cluster.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
        {
            return _ignite.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(ICollection<Guid> ids)
        {
            return _ignite.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(params Guid[] ids)
        {
            return _ignite.Cluster.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            return _ignite.Cluster.ForPredicate(p);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            return _ignite.Cluster.ForAttribute(name, val);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForCacheNodes(string name)
        {
            return _ignite.Cluster.ForCacheNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForDataNodes(string name)
        {
            return _ignite.Cluster.ForDataNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForClientNodes(string name)
        {
            return _ignite.Cluster.ForClientNodes(name);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRemotes()
        {
            return _ignite.Cluster.ForRemotes();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            return _ignite.Cluster.ForHost(node);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRandom()
        {
            return _ignite.Cluster.ForRandom();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForOldest()
        {
            return _ignite.Cluster.ForOldest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForYoungest()
        {
            return _ignite.Cluster.ForYoungest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForDotNet()
        {
            return _ignite.Cluster.ForDotNet();
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Nodes()
        {
            return _ignite.Cluster.Nodes();
        }

        /** <inheritdoc /> */
        public IClusterNode Node(Guid id)
        {
            return _ignite.Cluster.Node(id);
        }

        /** <inheritdoc /> */
        public IClusterNode Node()
        {
            return _ignite.Cluster.Node();
        }

        /** <inheritdoc /> */
        public IClusterMetrics Metrics()
        {
            return _ignite.Cluster.Metrics();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            _ignite.Dispose();
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> Cache<TK, TV>(string name)
        {
            return _ignite.Cache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(string name)
        {
            return _ignite.GetOrCreateCache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(string name)
        {
            return _ignite.CreateCache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public IClusterNode LocalNode
        {
            get
            {
                return _ignite.Cluster.LocalNode;
            }
        }

        /** <inheritdoc /> */
        public bool PingNode(Guid nodeId)
        {
            return _ignite.Cluster.PingNode(nodeId);
        }

        /** <inheritdoc /> */
        public long TopologyVersion
        {
            get { return _ignite.Cluster.TopologyVersion; }
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Topology(long ver)
        {
            return _ignite.Cluster.Topology(ver);
        }

        /** <inheritdoc /> */
        public void ResetMetrics()
        {
            _ignite.Cluster.ResetMetrics();
        }

        /** <inheritdoc /> */
        public IDataStreamer<TK, TV> DataStreamer<TK, TV>(string cacheName)
        {
            return _ignite.DataStreamer<TK, TV>(cacheName);
        }

        /** <inheritdoc /> */
        public IPortables Portables()
        {
            return _ignite.Portables();
        }

        /** <inheritdoc /> */
        public ICacheAffinity Affinity(string name)
        {
            return _ignite.Affinity(name);
        }

        /** <inheritdoc /> */
        public ITransactions Transactions
        {
            get { return _ignite.Transactions; }
        }

        /** <inheritdoc /> */
        public IMessaging Message()
        {
            return _ignite.Message();
        }

        /** <inheritdoc /> */
        public IMessaging Message(IClusterGroup clusterGroup)
        {
            return _ignite.Message(clusterGroup);
        }

        /** <inheritdoc /> */
        public IEvents Events()
        {
            return _ignite.Events();
        }

        /** <inheritdoc /> */
        public IEvents Events(IClusterGroup clusterGroup)
        {
            return _ignite.Events(clusterGroup);
        }

        /** <inheritdoc /> */
        public IServices Services()
        {
            return _ignite.Services();
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
                return _ignite;
            }
        }

        /** <inheritdoc /> */
        public IPortableMetadata Metadata(int typeId)
        {
            return ((IClusterGroupEx)_ignite).Metadata(typeId);
        }
    }
}
