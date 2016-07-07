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
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Grid proxy with fake serialization.
    /// </summary>
    [Serializable]
    internal class IgniteProxy : IIgnite, IClusterGroupEx, IBinaryWriteAware, ICluster
    {
        /** */
        [NonSerialized]
        private readonly Ignite _ignite;

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
        public IgniteProxy(Ignite ignite)
        {
            _ignite = ignite;
        }

        /** <inheritdoc /> */
        public string Name
        {
            get { return _ignite.Name; }
        }

        /** <inheritdoc /> */

        public ICluster GetCluster()
        {
            return this;
        }

        /** <inheritdoc /> */
        public IIgnite Ignite
        {
            get { return this; }
        }

        /** <inheritdoc /> */
        public IClusterGroup ForLocal()
        {
            return _ignite.GetCluster().ForLocal();
        }

        /** <inheritdoc /> */
        public ICompute GetCompute()
        {
            return _ignite.GetCompute();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            return _ignite.GetCluster().ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            return _ignite.GetCluster().ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
        {
            return _ignite.GetCluster().ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(ICollection<Guid> ids)
        {
            return _ignite.GetCluster().ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(params Guid[] ids)
        {
            return _ignite.GetCluster().ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            return _ignite.GetCluster().ForPredicate(p);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            return _ignite.GetCluster().ForAttribute(name, val);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForCacheNodes(string name)
        {
            return _ignite.GetCluster().ForCacheNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForDataNodes(string name)
        {
            return _ignite.GetCluster().ForDataNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForClientNodes(string name)
        {
            return _ignite.GetCluster().ForClientNodes(name);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRemotes()
        {
            return _ignite.GetCluster().ForRemotes();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            return _ignite.GetCluster().ForHost(node);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRandom()
        {
            return _ignite.GetCluster().ForRandom();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForOldest()
        {
            return _ignite.GetCluster().ForOldest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForYoungest()
        {
            return _ignite.GetCluster().ForYoungest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForDotNet()
        {
            return _ignite.GetCluster().ForDotNet();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForServers()
        {
            return _ignite.GetCluster().ForServers();
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> GetNodes()
        {
            return _ignite.GetCluster().GetNodes();
        }

        /** <inheritdoc /> */
        public IClusterNode GetNode(Guid id)
        {
            return _ignite.GetCluster().GetNode(id);
        }

        /** <inheritdoc /> */
        public IClusterNode GetNode()
        {
            return _ignite.GetCluster().GetNode();
        }

        /** <inheritdoc /> */
        public IClusterMetrics GetMetrics()
        {
            return _ignite.GetCluster().GetMetrics();
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly", 
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _ignite.Dispose();
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetCache<TK, TV>(string name)
        {
            return _ignite.GetCache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(string name)
        {
            return _ignite.GetOrCreateCache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration)
        {
            return _ignite.GetOrCreateCache<TK, TV>(configuration);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration, NearCacheConfiguration nearConfiguration)
        {
            return _ignite.GetOrCreateCache<TK, TV>(configuration, nearConfiguration);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(string name)
        {
            return _ignite.CreateCache<TK, TV>(name);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration)
        {
            return _ignite.CreateCache<TK, TV>(configuration);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration, NearCacheConfiguration nearConfiguration)
        {
            return _ignite.CreateCache<TK, TV>(configuration, nearConfiguration);
        }

        /** <inheritdoc /> */
        public void DestroyCache(string name)
        {
            _ignite.DestroyCache(name);
        }

        /** <inheritdoc /> */
        public IClusterNode GetLocalNode()
        {
            return _ignite.GetCluster().GetLocalNode();
        }

        /** <inheritdoc /> */
        public bool PingNode(Guid nodeId)
        {
            return _ignite.GetCluster().PingNode(nodeId);
        }

        /** <inheritdoc /> */
        public long TopologyVersion
        {
            get { return _ignite.GetCluster().TopologyVersion; }
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> GetTopology(long ver)
        {
            return _ignite.GetCluster().GetTopology(ver);
        }

        /** <inheritdoc /> */
        public void ResetMetrics()
        {
            _ignite.GetCluster().ResetMetrics();
        }

        /** <inheritdoc /> */
        public Task<bool> ClientReconnectTask
        {
            get { return _ignite.GetCluster().ClientReconnectTask; }
        }

        /** <inheritdoc /> */
        public IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName)
        {
            return _ignite.GetDataStreamer<TK, TV>(cacheName);
        }

        /** <inheritdoc /> */
        public IBinary GetBinary()
        {
            return _ignite.GetBinary();
        }

        /** <inheritdoc /> */
        public ICacheAffinity GetAffinity(string name)
        {
            return _ignite.GetAffinity(name);
        }

        /** <inheritdoc /> */

        public ITransactions GetTransactions()
        {
            return _ignite.GetTransactions();
        }

        /** <inheritdoc /> */
        public IMessaging GetMessaging()
        {
            return _ignite.GetMessaging();
        }

        /** <inheritdoc /> */
        public IEvents GetEvents()
        {
            return _ignite.GetEvents();
        }

        /** <inheritdoc /> */
        public IServices GetServices()
        {
            return _ignite.GetServices();
        }

        /** <inheritdoc /> */
        public IAtomicLong GetAtomicLong(string name, long initialValue, bool create)
        {
            return _ignite.GetAtomicLong(name, initialValue, create);
        }

        /** <inheritdoc /> */
        public IgniteConfiguration GetConfiguration()
        {
            return _ignite.GetConfiguration();
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration)
        {
            return _ignite.CreateNearCache<TK, TV>(name, configuration);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration)
        {
            return _ignite.GetOrCreateNearCache<TK, TV>(name, configuration);
        }

        /** <inheritdoc /> */
        public ICollection<string> GetCacheNames()
        {
            return _ignite.GetCacheNames();
        }

        /** <inheritdoc /> */
        public event EventHandler Stopping
        {
            add { _ignite.Stopping += value; }
            remove { _ignite.Stopping -= value; }
        }

        /** <inheritdoc /> */
        public event EventHandler Stopped
        {
            add { _ignite.Stopped += value; }
            remove { _ignite.Stopped -= value; }
        }

        /** <inheritdoc /> */
        public event EventHandler ClientDisconnected
        {
            add { _ignite.ClientDisconnected += value; }
            remove { _ignite.ClientDisconnected -= value; }
        }

        /** <inheritdoc /> */
        public event EventHandler<ClientReconnectEventArgs> ClientReconnected
        {
            add { _ignite.ClientReconnected += value; }
            remove { _ignite.ClientReconnected -= value; }
        }

        /** <inheritdoc /> */
        public IAtomicSequence GetAtomicSequence(string name, long initialValue, bool create)
        {
            return _ignite.GetAtomicSequence(name, initialValue, create);
        }

        /** <inheritdoc /> */
        public IAtomicReference<T> GetAtomicReference<T>(string name, T initialValue, bool create)
        {
            return _ignite.GetAtomicReference(name, initialValue, create);
        }

        /** <inheritdoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            // No-op.
        }

        /// <summary>
        /// Target grid.
        /// </summary>
        internal Ignite Target
        {
            get
            {
                return _ignite;
            }
        }

        /** <inheritdoc /> */
        public IBinaryType GetBinaryType(int typeId)
        {
            return _ignite.GetBinaryType(typeId);
        }
    }
}
