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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Transactions;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;

    /// <summary>
    /// Native grid wrapper.
    /// </summary>
    internal class GridImpl : IIgnite, IClusterGroupEx, ICluster
    {
        /** Operation: product. */
        private static readonly int OP_PRODUCT = 1;

        /** Operation: security. */
        private static readonly int OP_SECURITY = 2;

        /** Operation: DR. */
        private static readonly int OP_DR = 3;

        /** */
        private readonly GridConfiguration cfg;
        
        /** Grid name. */
        private readonly string name;

        /** Unmanaged node. */
        private readonly IUnmanagedTarget proc;
        
        /** Marshaller. */
        private readonly PortableMarshaller marsh;

        /** Initial projection. */
        private readonly ClusterGroupImpl prj;

        /** Portables. */
        private readonly PortablesImpl portables;

        /** Cached proxy. */
        private readonly GridProxy proxy;

        /** Lifecycle beans. */
        private readonly IList<LifecycleBeanHolder> lifecycleBeans;

        /** Grid local node. */
        private IClusterNode locNode;

        /** Transactions facade. */
        private readonly TransactionsImpl txs;
        
        /** Callbacks */
        private readonly UnmanagedCallbacks cbs;

        /** Node info cache. */
        private readonly ConcurrentDictionary<Guid, ClusterNodeImpl> nodes = 
            new ConcurrentDictionary<Guid, ClusterNodeImpl>();

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="name">Grid name.</param>
        /// <param name="proc">Interop processor.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="lifecycleBeans">Lifecycle beans.</param>
        /// <param name="cbs">Callbacks.</param>
        public GridImpl(GridConfiguration cfg, string name, IUnmanagedTarget proc, PortableMarshaller marsh, 
            IList<LifecycleBeanHolder> lifecycleBeans, UnmanagedCallbacks cbs)
        {
            this.cfg = cfg;
            this.name = name;
            this.proc = proc;
            this.marsh = marsh;
            this.lifecycleBeans = lifecycleBeans;
            this.cbs = cbs;

            marsh.Grid = this;

            prj = new ClusterGroupImpl(proc, UU.ProcessorProjection(proc), marsh, this, null);

            portables = new PortablesImpl(marsh);

            proxy = new GridProxy(this);

            cbs.Initialize(this);

            txs = new TransactionsImpl(UU.ProcessorTransactions(proc), marsh, LocalNode.Id);
        }

        /// <summary>
        /// On-start routine.
        /// </summary>
        internal void OnStart()
        {
            foreach (LifecycleBeanHolder lifecycleBean in lifecycleBeans)
                lifecycleBean.OnStart(this);
        }

        /// <summary>
        /// Gets grid proxy.
        /// </summary>
        /// <returns>Proxy.</returns>
        public GridProxy Proxy
        {
            get
            {
                return proxy;
            }
        }

        /** <inheritdoc /> */
        public string Name
        {
            get 
            {
                return name;
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
            return prj.ForNodes(LocalNode);
        }

        /** <inheritdoc /> */
        public ICompute Compute()
        {
            return prj.Compute();
        }

        /** <inheritdoc /> */
        public ICompute Compute(IClusterGroup clusterGroup)
        {
            A.NotNull(clusterGroup, "clusterGroup");

            return clusterGroup.Compute();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            return ((IClusterGroup) prj).ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            return prj.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
        {
            return ((IClusterGroup) prj).ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(ICollection<Guid> ids)
        {
            return prj.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(params Guid[] ids)
        {
            return prj.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            A.NotNull(p, "p");

            return prj.ForPredicate(p);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            return prj.ForAttribute(name, val);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForCacheNodes(string name)
        {
            return prj.ForCacheNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForDataNodes(string name)
        {
            return prj.ForDataNodes(name);
        }
        
        /** <inheritdoc /> */
        public IClusterGroup ForClientNodes(string name)
        {
            return prj.ForClientNodes(name);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRemotes()
        {
            return prj.ForRemotes();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            A.NotNull(node, "node");

            return prj.ForHost(node);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRandom()
        {
            return prj.ForRandom();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForOldest()
        {
            return prj.ForOldest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForYoungest()
        {
            return prj.ForYoungest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForDotNet()
        {
            return prj.ForDotNet();
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Nodes()
        {
            return prj.Nodes();
        }

        /** <inheritdoc /> */
        public IClusterNode Node(Guid id)
        {
            return prj.Node(id);
        }

        /** <inheritdoc /> */
        public IClusterNode Node()
        {
            return prj.Node();
        }

        /** <inheritdoc /> */
        public IClusterMetrics Metrics()
        {
            return prj.Metrics();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            GridFactory.Stop(Name, true);
        }

        /// <summary>
        /// Internal stop routine.
        /// </summary>
        /// <param name="cancel">Cancel flag.</param>
        internal unsafe void Stop(bool cancel)
        {
            UU.IgnitionStop(proc.Context, Name, cancel);

            cbs.Cleanup();
        }

        /** <inheritdoc /> */
        public ICache<K, V> Cache<K, V>(string name)
        {
            return Cache<K, V>(UU.ProcessorCache(proc, name));
        }

        /** <inheritdoc /> */
        public ICache<K, V> GetOrCreateCache<K, V>(string name)
        {
            return Cache<K, V>(UU.ProcessorGetOrCreateCache(proc, name));
        }

        /** <inheritdoc /> */
        public ICache<K, V> CreateCache<K, V>(string name)
        {
            return Cache<K, V>(UU.ProcessorCreateCache(proc, name));
        }

        /// <summary>
        /// Gets cache from specified native cache object.
        /// </summary>
        /// <param name="nativeCache">Native cache.</param>
        /// <param name="keepPortable">Portable flag.</param>
        /// <returns>
        /// New instance of cache wrapping specified native cache.
        /// </returns>
        public ICache<K, V> Cache<K, V>(IUnmanagedTarget nativeCache, bool keepPortable = false)
        {
            var cacheImpl = new CacheImpl<K, V>(this, nativeCache, marsh, false, keepPortable, false, false);

            return new CacheProxyImpl<K, V>(cacheImpl);
        }

        /** <inheritdoc /> */
        public IClusterNode LocalNode
        {
            get
            {
                if (locNode == null)
                {
                    foreach (IClusterNode node in Nodes()) {
                        if (node.IsLocal)
                        {
                            locNode = node;

                            break;
                        }
                    }
                }

                return locNode;
            }
        }

        /** <inheritdoc /> */
        public bool PingNode(Guid nodeId)
        {
            return prj.PingNode(nodeId);
        }

        /** <inheritdoc /> */
        public long TopologyVersion
        {
            get { return prj.TopologyVersion; }
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Topology(long ver)
        {
            return prj.Topology(ver);
        }

        /** <inheritdoc /> */
        public void ResetMetrics()
        {
            UU.ProjectionResetMetrics(prj.Target);
        }

        /** <inheritdoc /> */
        public IDataStreamer<K, V> DataStreamer<K, V>(string cacheName)
        {
            return new DataStreamerImpl<K, V>(UU.ProcessorDataStreamer(proc, cacheName, false),
                marsh, cacheName, false);
        }

        /** <inheritdoc /> */
        public IPortables Portables()
        {
            return portables;
        }

        /** <inheritdoc /> */
        public ICacheAffinity Affinity(string cacheName)
        {
            return new CacheAffinityImpl(UU.ProcessorAffinity(proc, cacheName), marsh, false, this);
        }

        /** <inheritdoc /> */
        public ITransactions Transactions
        {
            get { return txs; }
        }

        /** <inheritdoc /> */
        public IMessaging Message()
        {
            return prj.Message();
        }

        /** <inheritdoc /> */
        public IMessaging Message(IClusterGroup clusterGroup)
        {
            A.NotNull(clusterGroup, "clusterGroup");

            return clusterGroup.Message();
        }

        /** <inheritdoc /> */
        public IEvents Events()
        {
            return prj.Events();
        }

        /** <inheritdoc /> */
        public IEvents Events(IClusterGroup clusterGroup)
        {
            if (clusterGroup == null)
                throw new ArgumentNullException("clusterGroup");

            return clusterGroup.Events();
        }

        /** <inheritdoc /> */
        public IServices Services()
        {
            return prj.Services();
        }

        /// <summary>
        /// Gets internal projection.
        /// </summary>
        /// <returns>Projection.</returns>
        internal ClusterGroupImpl ClusterGroup
        {
            get
            {
                return prj;
            }
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        internal PortableMarshaller Marshaller
        {
            get
            {
                return marsh;
            }
        }

        /// <summary>
        /// Configuration.
        /// </summary>
        internal GridConfiguration Configuration
        {
            get
            {
                return cfg;
            }
        }

        /// <summary>
        /// Put metadata to Grid.
        /// </summary>
        /// <param name="metas">Metadata.</param>
        internal void PutMetadata(IDictionary<int, IPortableMetadata> metas) 
        {
            prj.PutMetadata(metas);
        }

        /** <inheritDoc /> */
        public IPortableMetadata Metadata(int typeId)
        {
            return prj.Metadata(typeId);
        }

        /// <summary>
        /// Handle registry.
        /// </summary>
        public HandleRegistry HandleRegistry
        {
            get { return cbs.HandleRegistry; }
        }

        /// <summary>
        /// Updates the node information from stream.
        /// </summary>
        /// <param name="memPtr">Stream ptr.</param>
        public void UpdateNodeInfo(long memPtr)
        {
            var stream = GridManager.Memory.Get(memPtr).Stream();

            IPortableRawReader reader = Marshaller.StartUnmarshal(stream, false);

            var node = new ClusterNodeImpl(reader);

            node.Init(this);

            nodes[node.Id] = node;
        }

        /// <summary>
        /// Gets the node from cache.
        /// </summary>
        /// <param name="id">Node id.</param>
        /// <returns>Cached node.</returns>
        public ClusterNodeImpl GetNode(Guid? id)
        {
            return id == null ? null : nodes[id.Value];
        }

        /// <summary>
        /// Gets the interop processor.
        /// </summary>
        internal IUnmanagedTarget InteropProcessor
        {
            get { return proc; }
        }
    }
}
