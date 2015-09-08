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
    using System.Diagnostics;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Transactions;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Native Ignite wrapper.
    /// </summary>
    internal class Ignite : IIgnite, IClusterGroupEx, ICluster
    {
        /** */
        private readonly IgniteConfiguration _cfg;

        /** Grid name. */
        private readonly string _name;

        /** Unmanaged node. */
        private readonly IUnmanagedTarget _proc;

        /** Marshaller. */
        private readonly PortableMarshaller _marsh;

        /** Initial projection. */
        private readonly ClusterGroupImpl _prj;

        /** Portables. */
        private readonly PortablesImpl _portables;

        /** Cached proxy. */
        private readonly IgniteProxy _proxy;

        /** Lifecycle beans. */
        private readonly IList<LifecycleBeanHolder> _lifecycleBeans;

        /** Local node. */
        private IClusterNode _locNode;

        /** Transactions facade. */
        private readonly Lazy<TransactionsImpl> _transactions;

        /** Callbacks */
        private readonly UnmanagedCallbacks _cbs;

        /** Node info cache. */

        private readonly ConcurrentDictionary<Guid, ClusterNodeImpl> _nodes =
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
        public Ignite(IgniteConfiguration cfg, string name, IUnmanagedTarget proc, PortableMarshaller marsh,
            IList<LifecycleBeanHolder> lifecycleBeans, UnmanagedCallbacks cbs)
        {
            Debug.Assert(cfg != null);
            Debug.Assert(proc != null);
            Debug.Assert(marsh != null);
            Debug.Assert(lifecycleBeans != null);
            Debug.Assert(cbs != null);

            _cfg = cfg;
            _name = name;
            _proc = proc;
            _marsh = marsh;
            _lifecycleBeans = lifecycleBeans;
            _cbs = cbs;

            marsh.Ignite = this;

            _prj = new ClusterGroupImpl(proc, UU.ProcessorProjection(proc), marsh, this, null);

            _portables = new PortablesImpl(marsh);

            _proxy = new IgniteProxy(this);

            cbs.Initialize(this);

            // Grid is not completely started here, can't initialize interop transactions right away.
            _transactions = new Lazy<TransactionsImpl>(
                    () => new TransactionsImpl(UU.ProcessorTransactions(proc), marsh, LocalNode.Id));
        }

        /// <summary>
        /// On-start routine.
        /// </summary>
        internal void OnStart()
        {
            foreach (var lifecycleBean in _lifecycleBeans)
                lifecycleBean.OnStart(this);
        }

        /// <summary>
        /// Gets Ignite proxy.
        /// </summary>
        /// <returns>Proxy.</returns>
        public IgniteProxy Proxy
        {
            get { return _proxy; }
        }

        /** <inheritdoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritdoc /> */
        public ICluster Cluster
        {
            get { return this; }
        }

        /** <inheritdoc /> */
        IIgnite IClusterGroup.Ignite
        {
            get { return this; }
        }

        /** <inheritdoc /> */
        public IClusterGroup ForLocal()
        {
            return _prj.ForNodes(LocalNode);
        }

        /** <inheritdoc /> */
        public ICompute Compute()
        {
            return _prj.Compute();
        }

        /** <inheritdoc /> */
        public ICompute Compute(IClusterGroup clusterGroup)
        {
            IgniteArgumentCheck.NotNull(clusterGroup, "clusterGroup");

            return clusterGroup.Compute();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            return ((IClusterGroup) _prj).ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            return _prj.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
        {
            return ((IClusterGroup) _prj).ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(ICollection<Guid> ids)
        {
            return _prj.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(params Guid[] ids)
        {
            return _prj.ForNodeIds(ids);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            IgniteArgumentCheck.NotNull(p, "p");

            return _prj.ForPredicate(p);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            return _prj.ForAttribute(name, val);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForCacheNodes(string name)
        {
            return _prj.ForCacheNodes(name);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForDataNodes(string name)
        {
            return _prj.ForDataNodes(name);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForClientNodes(string name)
        {
            return _prj.ForClientNodes(name);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRemotes()
        {
            return _prj.ForRemotes();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            IgniteArgumentCheck.NotNull(node, "node");

            return _prj.ForHost(node);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForRandom()
        {
            return _prj.ForRandom();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForOldest()
        {
            return _prj.ForOldest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForYoungest()
        {
            return _prj.ForYoungest();
        }

        /** <inheritdoc /> */
        public IClusterGroup ForDotNet()
        {
            return _prj.ForDotNet();
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Nodes()
        {
            return _prj.Nodes();
        }

        /** <inheritdoc /> */
        public IClusterNode Node(Guid id)
        {
            return _prj.Node(id);
        }

        /** <inheritdoc /> */
        public IClusterNode Node()
        {
            return _prj.Node();
        }

        /** <inheritdoc /> */
        public IClusterMetrics Metrics()
        {
            return _prj.Metrics();
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            Ignition.Stop(Name, true);
        }

        /// <summary>
        /// Internal stop routine.
        /// </summary>
        /// <param name="cancel">Cancel flag.</param>
        internal unsafe void Stop(bool cancel)
        {
            UU.IgnitionStop(_proc.Context, Name, cancel);

            _cbs.Cleanup();

            foreach (var bean in _lifecycleBeans)
                bean.OnLifecycleEvent(LifecycleEventType.AfterNodeStop);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> Cache<TK, TV>(string name)
        {
            return Cache<TK, TV>(UU.ProcessorCache(_proc, name));
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(string name)
        {
            return Cache<TK, TV>(UU.ProcessorGetOrCreateCache(_proc, name));
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(string name)
        {
            return Cache<TK, TV>(UU.ProcessorCreateCache(_proc, name));
        }

        /// <summary>
        /// Gets cache from specified native cache object.
        /// </summary>
        /// <param name="nativeCache">Native cache.</param>
        /// <param name="keepPortable">Portable flag.</param>
        /// <returns>
        /// New instance of cache wrapping specified native cache.
        /// </returns>
        public ICache<TK, TV> Cache<TK, TV>(IUnmanagedTarget nativeCache, bool keepPortable = false)
        {
            var cacheImpl = new CacheImpl<TK, TV>(this, nativeCache, _marsh, false, keepPortable, false, false);

            return new CacheProxyImpl<TK, TV>(cacheImpl);
        }

        /** <inheritdoc /> */
        public IClusterNode LocalNode
        {
            get
            {
                if (_locNode == null)
                {
                    foreach (IClusterNode node in Nodes())
                    {
                        if (node.IsLocal)
                        {
                            _locNode = node;

                            break;
                        }
                    }
                }

                return _locNode;
            }
        }

        /** <inheritdoc /> */
        public bool PingNode(Guid nodeId)
        {
            return _prj.PingNode(nodeId);
        }

        /** <inheritdoc /> */
        public long TopologyVersion
        {
            get { return _prj.TopologyVersion; }
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> Topology(long ver)
        {
            return _prj.Topology(ver);
        }

        /** <inheritdoc /> */
        public void ResetMetrics()
        {
            UU.ProjectionResetMetrics(_prj.Target);
        }

        /** <inheritdoc /> */
        public IDataStreamer<TK, TV> DataStreamer<TK, TV>(string cacheName)
        {
            return new DataStreamerImpl<TK, TV>(UU.ProcessorDataStreamer(_proc, cacheName, false),
                _marsh, cacheName, false);
        }

        /** <inheritdoc /> */
        public IPortables Portables()
        {
            return _portables;
        }

        /** <inheritdoc /> */
        public ICacheAffinity Affinity(string cacheName)
        {
            return new CacheAffinityImpl(UU.ProcessorAffinity(_proc, cacheName), _marsh, false, this);
        }

        /** <inheritdoc /> */
        public ITransactions Transactions
        {
            get { return _transactions.Value; }
        }

        /** <inheritdoc /> */
        public IMessaging Message()
        {
            return _prj.Message();
        }

        /** <inheritdoc /> */
        public IMessaging Message(IClusterGroup clusterGroup)
        {
            IgniteArgumentCheck.NotNull(clusterGroup, "clusterGroup");

            return clusterGroup.Message();
        }

        /** <inheritdoc /> */
        public IEvents Events()
        {
            return _prj.Events();
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
            return _prj.Services();
        }

        /// <summary>
        /// Gets internal projection.
        /// </summary>
        /// <returns>Projection.</returns>
        internal ClusterGroupImpl ClusterGroup
        {
            get { return _prj; }
        }

        /// <summary>
        /// Marshaller.
        /// </summary>
        internal PortableMarshaller Marshaller
        {
            get { return _marsh; }
        }

        /// <summary>
        /// Configuration.
        /// </summary>
        internal IgniteConfiguration Configuration
        {
            get { return _cfg; }
        }

        /// <summary>
        /// Put metadata to Grid.
        /// </summary>
        /// <param name="metas">Metadata.</param>
        internal void PutMetadata(IDictionary<int, IPortableMetadata> metas)
        {
            _prj.PutMetadata(metas);
        }

        /** <inheritDoc /> */
        public IPortableMetadata Metadata(int typeId)
        {
            return _prj.Metadata(typeId);
        }

        /// <summary>
        /// Handle registry.
        /// </summary>
        public HandleRegistry HandleRegistry
        {
            get { return _cbs.HandleRegistry; }
        }

        /// <summary>
        /// Updates the node information from stream.
        /// </summary>
        /// <param name="memPtr">Stream ptr.</param>
        public void UpdateNodeInfo(long memPtr)
        {
            var stream = IgniteManager.Memory.Get(memPtr).Stream();

            IPortableRawReader reader = Marshaller.StartUnmarshal(stream, false);

            var node = new ClusterNodeImpl(reader);

            node.Init(this);

            _nodes[node.Id] = node;
        }

        /// <summary>
        /// Gets the node from cache.
        /// </summary>
        /// <param name="id">Node id.</param>
        /// <returns>Cached node.</returns>
        public ClusterNodeImpl GetNode(Guid? id)
        {
            return id == null ? null : _nodes[id.Value];
        }

        /// <summary>
        /// Gets the interop processor.
        /// </summary>
        internal IUnmanagedTarget InteropProcessor
        {
            get { return _proc; }
        }
    }
}
