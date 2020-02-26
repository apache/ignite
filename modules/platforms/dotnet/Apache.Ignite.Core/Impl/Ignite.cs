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
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Datastream;
    using Apache.Ignite.Core.DataStructures;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Datastream;
    using Apache.Ignite.Core.Impl.DataStructures;
    using Apache.Ignite.Core.Impl.Handle;
    using Apache.Ignite.Core.Impl.Plugin;
    using Apache.Ignite.Core.Impl.Transactions;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Interop;
    using Apache.Ignite.Core.Lifecycle;
    using Apache.Ignite.Core.Log;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.PersistentStore;
    using Apache.Ignite.Core.Services;
    using Apache.Ignite.Core.Transactions;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Native Ignite wrapper.
    /// </summary>
    internal class Ignite : PlatformTargetAdapter, ICluster, IIgniteInternal, IIgnite
    {
        /// <summary>
        /// Operation codes for PlatformProcessorImpl calls.
        /// </summary>
        private enum Op
        {
            GetCache = 1,
            CreateCache = 2,
            GetOrCreateCache = 3,
            CreateCacheFromConfig = 4,
            GetOrCreateCacheFromConfig = 5,
            DestroyCache = 6,
            GetAffinity = 7,
            GetDataStreamer = 8,
            GetTransactions = 9,
            GetClusterGroup = 10,
            GetExtension = 11,
            GetAtomicLong = 12,
            GetAtomicReference = 13,
            GetAtomicSequence = 14,
            GetIgniteConfiguration = 15,
            GetCacheNames = 16,
            CreateNearCache = 17,
            GetOrCreateNearCache = 18,
            LoggerIsLevelEnabled = 19,
            LoggerLog = 20,
            GetBinaryProcessor = 21,
            ReleaseStart = 22,
            AddCacheConfiguration = 23,
            SetBaselineTopologyVersion = 24,
            SetBaselineTopologyNodes = 25,
            GetBaselineTopology = 26,
            DisableWal = 27,
            EnableWal = 28,
            IsWalEnabled = 29,
            SetTxTimeoutOnPartitionMapExchange = 30,
            GetNodeVersion = 31,
            IsBaselineAutoAdjustmentEnabled = 32,
            SetBaselineAutoAdjustmentEnabled = 33,
            GetBaselineAutoAdjustTimeout = 34,
            SetBaselineAutoAdjustTimeout = 35
        }

        /** */
        private readonly IgniteConfiguration _cfg;

        /** Grid name. */
        private readonly string _name;

        /** Unmanaged node. */
        private readonly IPlatformTargetInternal _proc;

        /** Marshaller. */
        private readonly Marshaller _marsh;

        /** Initial projection. */
        private readonly ClusterGroupImpl _prj;

        /** Binary. */
        private readonly Binary.Binary _binary;

        /** Binary processor. */
        private readonly BinaryProcessor _binaryProc;

        /** Lifecycle handlers. */
        private readonly IList<LifecycleHandlerHolder> _lifecycleHandlers;

        /** Local node. */
        private IClusterNode _locNode;

        /** Callbacks */
        private readonly UnmanagedCallbacks _cbs;

        /** Node info cache. */
        private readonly ConcurrentDictionary<Guid, ClusterNodeImpl> _nodes =
            new ConcurrentDictionary<Guid, ClusterNodeImpl>();

        /** Client reconnect task completion source. */
        private volatile TaskCompletionSource<bool> _clientReconnectTaskCompletionSource = 
            new TaskCompletionSource<bool>();

        /** Plugin processor. */
        private readonly PluginProcessor _pluginProcessor;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="cfg">Configuration.</param>
        /// <param name="name">Grid name.</param>
        /// <param name="proc">Interop processor.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="lifecycleHandlers">Lifecycle beans.</param>
        /// <param name="cbs">Callbacks.</param>
        public Ignite(IgniteConfiguration cfg, string name, IPlatformTargetInternal proc, Marshaller marsh,
            IList<LifecycleHandlerHolder> lifecycleHandlers, UnmanagedCallbacks cbs) : base(proc)
        {
            Debug.Assert(cfg != null);
            Debug.Assert(proc != null);
            Debug.Assert(marsh != null);
            Debug.Assert(lifecycleHandlers != null);
            Debug.Assert(cbs != null);

            _cfg = cfg;
            _name = name;
            _proc = proc;
            _marsh = marsh;
            _lifecycleHandlers = lifecycleHandlers;
            _cbs = cbs;

            marsh.Ignite = this;

            _prj = new ClusterGroupImpl(Target.OutObjectInternal((int) Op.GetClusterGroup), null);

            _binary = new Binary.Binary(marsh);

            _binaryProc = new BinaryProcessor(DoOutOpObject((int) Op.GetBinaryProcessor));

            cbs.Initialize(this);

            // Set reconnected task to completed state for convenience.
            _clientReconnectTaskCompletionSource.SetResult(false);

            SetCompactFooter();

            _pluginProcessor = new PluginProcessor(this);
        }

        /// <summary>
        /// Sets the compact footer setting.
        /// </summary>
        private void SetCompactFooter()
        {
            if (!string.IsNullOrEmpty(_cfg.SpringConfigUrl))
            {
                // If there is a Spring config, use setting from Spring,
                // since we ignore .NET config in legacy mode.
                var cfg0 = GetConfiguration().BinaryConfiguration;

                if (cfg0 != null)
                    _marsh.CompactFooter = cfg0.CompactFooter;
            }
        }

        /// <summary>
        /// On-start routine.
        /// </summary>
        internal void OnStart()
        {
            PluginProcessor.OnIgniteStart();

            foreach (var lifecycleBean in _lifecycleHandlers)
                lifecycleBean.OnStart(this);
        }

        /** <inheritdoc /> */
        public string Name
        {
            get { return _name; }
        }

        /** <inheritdoc /> */
        public ICluster GetCluster()
        {
            return this;
        }

        /** <inheritdoc /> */
        IIgnite IClusterGroup.Ignite
        {
            get { return this; }
        }

        /** <inheritdoc /> */
        public IClusterGroup ForLocal()
        {
            return _prj.ForNodes(GetLocalNode());
        }

        /** <inheritdoc cref="IIgnite" /> */
        public ICompute GetCompute()
        {
            return _prj.ForServers().GetCompute();
        }

        /** <inheritdoc /> */
        public IgniteProductVersion GetVersion()
        {
            return Target.OutStream((int) Op.GetNodeVersion, r => new IgniteProductVersion(r));
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            return _prj.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            return _prj.ForNodes(nodes);
        }

        /** <inheritdoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
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
        public IClusterGroup ForDaemons()
        {
            return _prj.ForDaemons();
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
        public IClusterGroup ForServers()
        {
            return _prj.ForServers();
        }

        /** <inheritdoc /> */
        public ICollection<IClusterNode> GetNodes()
        {
            return _prj.GetNodes();
        }

        /** <inheritdoc /> */
        public IClusterNode GetNode(Guid id)
        {
            return _prj.GetNode(id);
        }

        /** <inheritdoc /> */
        public IClusterNode GetNode()
        {
            return _prj.GetNode();
        }

        /** <inheritdoc /> */
        public IClusterMetrics GetMetrics()
        {
            return _prj.GetMetrics();
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        [SuppressMessage("Microsoft.Usage", "CA2213:DisposableFieldsShouldBeDisposed", MessageId = "_proxy",
            Justification = "Proxy does not need to be disposed.")]
        public void Dispose()
        {
            Ignition.Stop(Name, true);
        }

        /// <summary>
        /// Internal stop routine.
        /// </summary>
        /// <param name="cancel">Cancel flag.</param>
        internal void Stop(bool cancel)
        {
            var jniTarget = _proc as PlatformJniTarget;

            if (jniTarget == null)
            {
                throw new IgniteException("Ignition.Stop is not supported in thin client.");
            }

            UU.IgnitionStop(Name, cancel);

            _cbs.Cleanup();
        }

        /// <summary>
        /// Called before node has stopped.
        /// </summary>
        internal void BeforeNodeStop()
        {
            var handler = Stopping;
            if (handler != null)
                handler.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Called after node has stopped.
        /// </summary>
        internal void AfterNodeStop()
        {
            foreach (var bean in _lifecycleHandlers)
                bean.OnLifecycleEvent(LifecycleEventType.AfterNodeStop);

            var handler = Stopped;
            if (handler != null)
                handler.Invoke(this, EventArgs.Empty);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            return GetCache<TK, TV>(DoOutOpObject((int) Op.GetCache, w => w.WriteString(name)));
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            return GetCache<TK, TV>(DoOutOpObject((int) Op.GetOrCreateCache, w => w.WriteString(name)));
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration)
        {
            return GetOrCreateCache<TK, TV>(configuration, null);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration,
            NearCacheConfiguration nearConfiguration)
        {
            return GetOrCreateCache<TK, TV>(configuration, nearConfiguration, Op.GetOrCreateCacheFromConfig);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            var cacheTarget = DoOutOpObject((int) Op.CreateCache, w => w.WriteString(name));

            return GetCache<TK, TV>(cacheTarget);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration)
        {
            return CreateCache<TK, TV>(configuration, null);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateCache<TK, TV>(CacheConfiguration configuration, 
            NearCacheConfiguration nearConfiguration)
        {
            return GetOrCreateCache<TK, TV>(configuration, nearConfiguration, Op.CreateCacheFromConfig);
        }

        /// <summary>
        /// Gets or creates the cache.
        /// </summary>
        private ICache<TK, TV> GetOrCreateCache<TK, TV>(CacheConfiguration configuration, 
            NearCacheConfiguration nearConfiguration, Op op)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");
            IgniteArgumentCheck.NotNull(configuration.Name, "CacheConfiguration.Name");
            configuration.Validate(Logger);

            var cacheTarget = DoOutOpObject((int) op, s =>
            {
                var w = BinaryUtils.Marshaller.StartMarshal(s);

                configuration.Write(w, ClientSocket.CurrentProtocolVersion);

                if (nearConfiguration != null)
                {
                    w.WriteBoolean(true);
                    nearConfiguration.Write(w);
                }
                else
                {
                    w.WriteBoolean(false);
                }
            });

            return GetCache<TK, TV>(cacheTarget);
        }

        /** <inheritdoc /> */
        public void DestroyCache(string name)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            DoOutOp((int) Op.DestroyCache, w => w.WriteString(name));
        }

        /// <summary>
        /// Gets cache from specified native cache object.
        /// </summary>
        /// <param name="nativeCache">Native cache.</param>
        /// <param name="keepBinary">Keep binary flag.</param>
        /// <returns>
        /// New instance of cache wrapping specified native cache.
        /// </returns>
        public static ICache<TK, TV> GetCache<TK, TV>(IPlatformTargetInternal nativeCache, bool keepBinary = false)
        {
            return new CacheImpl<TK, TV>(nativeCache, false, keepBinary, false, false);
        }

        /** <inheritdoc /> */
        public IClusterNode GetLocalNode()
        {
            return _locNode ?? (_locNode =
                       GetNodes().FirstOrDefault(x => x.IsLocal) ??
                       ForDaemons().GetNodes().FirstOrDefault(x => x.IsLocal));
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
        public ICollection<IClusterNode> GetTopology(long ver)
        {
            return _prj.Topology(ver);
        }

        /** <inheritdoc /> */
        public void ResetMetrics()
        {
            _prj.ResetMetrics();
        }

        /** <inheritdoc /> */
        public Task<bool> ClientReconnectTask
        {
            get { return _clientReconnectTaskCompletionSource.Task; }
        }

        /** <inheritdoc /> */
        public IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            return GetDataStreamer<TK, TV>(cacheName, false);
        }

        /// <summary>
        /// Gets the data streamer.
        /// </summary>
        public IDataStreamer<TK, TV> GetDataStreamer<TK, TV>(string cacheName, bool keepBinary)
        {
            var streamerTarget = DoOutOpObject((int) Op.GetDataStreamer, w =>
            {
                w.WriteString(cacheName);
                w.WriteBoolean(keepBinary);
            });

            return new DataStreamerImpl<TK, TV>(streamerTarget, _marsh, cacheName, keepBinary);
        }

        /// <summary>
        /// Gets the public Ignite interface.
        /// </summary>
        public IIgnite GetIgnite()
        {
            return this;
        }

        /** <inheritdoc cref="IIgnite" /> */
        public IBinary GetBinary()
        {
            return _binary;
        }

        /** <inheritdoc /> */
        public ICacheAffinity GetAffinity(string cacheName)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            var aff = DoOutOpObject((int) Op.GetAffinity, w => w.WriteString(cacheName));
            
            return new CacheAffinityImpl(aff, false);
        }

        /** <inheritdoc /> */
        public ITransactions GetTransactions()
        {
            return new TransactionsImpl(this, DoOutOpObject((int) Op.GetTransactions), GetLocalNode().Id);
        }

        /** <inheritdoc cref="IIgnite" /> */
        public IMessaging GetMessaging()
        {
            return _prj.GetMessaging();
        }

        /** <inheritdoc cref="IIgnite" /> */
        public IEvents GetEvents()
        {
            return _prj.GetEvents();
        }

        /** <inheritdoc cref="IIgnite" /> */
        public IServices GetServices()
        {
            return _prj.ForServers().GetServices();
        }

        /** <inheritdoc /> */
        public void EnableStatistics(IEnumerable<string> cacheNames, bool enabled)
        {
            _prj.EnableStatistics(cacheNames, enabled);
        }

        /** <inheritdoc /> */
        public IAtomicLong GetAtomicLong(string name, long initialValue, bool create)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            var nativeLong = DoOutOpObject((int) Op.GetAtomicLong, w =>
            {
                w.WriteString(name);
                w.WriteLong(initialValue);
                w.WriteBoolean(create);
            });

            if (nativeLong == null)
                return null;

            return new AtomicLong(nativeLong, name);
        }

        /** <inheritdoc /> */
        public IAtomicSequence GetAtomicSequence(string name, long initialValue, bool create)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            var nativeSeq = DoOutOpObject((int) Op.GetAtomicSequence, w =>
            {
                w.WriteString(name);
                w.WriteLong(initialValue);
                w.WriteBoolean(create);
            });

            if (nativeSeq == null)
                return null;

            return new AtomicSequence(nativeSeq, name);
        }

        /** <inheritdoc /> */
        public IAtomicReference<T> GetAtomicReference<T>(string name, T initialValue, bool create)
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            var refTarget = DoOutOpObject((int) Op.GetAtomicReference, w =>
            {
                w.WriteString(name);
                w.WriteObject(initialValue);
                w.WriteBoolean(create);
            });

            return refTarget == null ? null : new AtomicReference<T>(refTarget, name);
        }

        /** <inheritdoc /> */
        public IgniteConfiguration GetConfiguration()
        {
            return DoInOp((int) Op.GetIgniteConfiguration,
                s => new IgniteConfiguration(BinaryUtils.Marshaller.StartUnmarshal(s), _cfg,
                    ClientSocket.CurrentProtocolVersion));
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> CreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration)
        {
            return GetOrCreateNearCache0<TK, TV>(name, configuration, Op.CreateNearCache);
        }

        /** <inheritdoc /> */
        public ICache<TK, TV> GetOrCreateNearCache<TK, TV>(string name, NearCacheConfiguration configuration)
        {
            return GetOrCreateNearCache0<TK, TV>(name, configuration, Op.GetOrCreateNearCache);
        }

        /** <inheritdoc /> */
        public ICollection<string> GetCacheNames()
        {
            return Target.OutStream((int) Op.GetCacheNames, r =>
            {
                var res = new string[r.ReadInt()];

                for (var i = 0; i < res.Length; i++)
                    res[i] = r.ReadString();

                return (ICollection<string>) res;
            });
        }

        /** <inheritdoc /> */
        public ILogger Logger
        {
            get { return _cbs.Log; }
        }

        /** <inheritdoc /> */
        public event EventHandler Stopping;

        /** <inheritdoc /> */
        public event EventHandler Stopped;

        /** <inheritdoc /> */
        public event EventHandler ClientDisconnected;

        /** <inheritdoc /> */
        public event EventHandler<ClientReconnectEventArgs> ClientReconnected;

        /** <inheritdoc /> */
        public T GetPlugin<T>(string name) where T : class
        {
            IgniteArgumentCheck.NotNullOrEmpty(name, "name");

            return PluginProcessor.GetProvider(name).GetPlugin<T>();
        }

        /** <inheritdoc /> */
        public void ResetLostPartitions(IEnumerable<string> cacheNames)
        {
            IgniteArgumentCheck.NotNull(cacheNames, "cacheNames");

            _prj.ResetLostPartitions(cacheNames);
        }

        /** <inheritdoc /> */
        public void ResetLostPartitions(params string[] cacheNames)
        {
            ResetLostPartitions((IEnumerable<string>) cacheNames);
        }

        /** <inheritdoc /> */
#pragma warning disable 618
        public ICollection<IMemoryMetrics> GetMemoryMetrics()
        {
            return _prj.GetMemoryMetrics();
        }

        /** <inheritdoc /> */
        public IMemoryMetrics GetMemoryMetrics(string memoryPolicyName)
        {
            IgniteArgumentCheck.NotNullOrEmpty(memoryPolicyName, "memoryPolicyName");

            return _prj.GetMemoryMetrics(memoryPolicyName);
        }
#pragma warning restore 618

        /** <inheritdoc cref="IIgnite" /> */
        public void SetActive(bool isActive)
        {
            _prj.SetActive(isActive);
        }

        /** <inheritdoc cref="IIgnite" /> */
        public bool IsActive()
        {
            return _prj.IsActive();
        }

        /** <inheritdoc /> */
        public void SetBaselineTopology(long topologyVersion)
        {
            DoOutInOp((int) Op.SetBaselineTopologyVersion, topologyVersion);
        }

        /** <inheritdoc /> */
        public void SetBaselineTopology(IEnumerable<IBaselineNode> nodes)
        {
            IgniteArgumentCheck.NotNull(nodes, "nodes");

            DoOutOp((int) Op.SetBaselineTopologyNodes, w =>
            {
                var pos = w.Stream.Position;
                w.WriteInt(0);
                var cnt = 0;

                foreach (var node in nodes)
                {
                    cnt++;
                    BaselineNode.Write(w, node);
                }

                w.Stream.WriteInt(pos, cnt);
            });
        }

        /** <inheritdoc /> */
        public ICollection<IBaselineNode> GetBaselineTopology()
        {
            return DoInOp((int) Op.GetBaselineTopology,
                s => Marshaller.StartUnmarshal(s).ReadCollectionRaw(r => (IBaselineNode) new BaselineNode(r)));
        }

        /** <inheritdoc /> */
        public void DisableWal(string cacheName)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            DoOutOp((int) Op.DisableWal, w => w.WriteString(cacheName));
        }

        /** <inheritdoc /> */
        public void EnableWal(string cacheName)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");
            
            DoOutOp((int) Op.EnableWal, w => w.WriteString(cacheName));
        }

        /** <inheritdoc /> */
        public bool IsWalEnabled(string cacheName)
        {
            IgniteArgumentCheck.NotNull(cacheName, "cacheName");

            return DoOutOp((int) Op.IsWalEnabled, w => w.WriteString(cacheName)) == True;
        }

        /** <inheritdoc /> */
        public void SetTxTimeoutOnPartitionMapExchange(TimeSpan timeout)
        {
            DoOutOp((int) Op.SetTxTimeoutOnPartitionMapExchange, 
                (BinaryWriter w) => w.WriteLong((long) timeout.TotalMilliseconds));
        }

        /** <inheritdoc /> */
        public bool IsBaselineAutoAdjustEnabled()
        {
            return DoOutOp((int) Op.IsBaselineAutoAdjustmentEnabled, s => s.ReadBool()) == True;
        }

        /** <inheritdoc /> */
        public void SetBaselineAutoAdjustEnabledFlag(bool isBaselineAutoAdjustEnabled)
        {
            DoOutOp((int) Op.SetBaselineAutoAdjustmentEnabled, w => w.WriteBoolean(isBaselineAutoAdjustEnabled));
        }

        /** <inheritdoc /> */
        public long GetBaselineAutoAdjustTimeout()
        {
            return DoOutOp((int) Op.GetBaselineAutoAdjustTimeout, s => s.ReadLong());
        }

        /** <inheritdoc /> */
        public void SetBaselineAutoAdjustTimeout(long baselineAutoAdjustTimeout)
        {
            DoOutInOp((int) Op.SetBaselineAutoAdjustTimeout, baselineAutoAdjustTimeout);
        }

        /** <inheritdoc /> */
#pragma warning disable 618
        public IPersistentStoreMetrics GetPersistentStoreMetrics()
        {
            return _prj.GetPersistentStoreMetrics();
        }
#pragma warning restore 618

        /** <inheritdoc /> */
        public ICollection<IDataRegionMetrics> GetDataRegionMetrics()
        {
            return _prj.GetDataRegionMetrics();
        }

        /** <inheritdoc /> */
        public IDataRegionMetrics GetDataRegionMetrics(string memoryPolicyName)
        {
            return _prj.GetDataRegionMetrics(memoryPolicyName);
        }

        /** <inheritdoc /> */
        public IDataStorageMetrics GetDataStorageMetrics()
        {
            return _prj.GetDataStorageMetrics();
        }

        /** <inheritdoc /> */
        public void AddCacheConfiguration(CacheConfiguration configuration)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");

            DoOutOp((int) Op.AddCacheConfiguration,
                s => configuration.Write(BinaryUtils.Marshaller.StartMarshal(s), ClientSocket.CurrentProtocolVersion));
        }

        /// <summary>
        /// Gets or creates near cache.
        /// </summary>
        private ICache<TK, TV> GetOrCreateNearCache0<TK, TV>(string name, NearCacheConfiguration configuration,
            Op op)
        {
            IgniteArgumentCheck.NotNull(configuration, "configuration");

            var cacheTarget = DoOutOpObject((int) op, w =>
            {
                w.WriteString(name);
                configuration.Write(w);
            });

            return GetCache<TK, TV>(cacheTarget);
        }

        /// <summary>
        /// Gets internal projection.
        /// </summary>
        /// <returns>Projection.</returns>
        public ClusterGroupImpl ClusterGroup
        {
            get { return _prj; }
        }

        /// <summary>
        /// Gets the binary processor.
        /// </summary>
        public IBinaryProcessor BinaryProcessor
        {
            get { return _binaryProc; }
        }

        /// <summary>
        /// Configuration.
        /// </summary>
        public IgniteConfiguration Configuration
        {
            get { return _cfg; }
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
            var stream = IgniteManager.Memory.Get(memPtr).GetStream();

            IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

            var node = new ClusterNodeImpl(reader);

            node.Init(this);

            _nodes[node.Id] = node;
        }

        /// <summary>
        /// Returns instance of Ignite Transactions to mark a transaction with a special label.
        /// </summary>
        /// <param name="label"></param>
        /// <returns><see cref="ITransactions"/></returns>
        internal ITransactions GetTransactionsWithLabel(string label)
        {
            Debug.Assert(label != null);
            
            var platformTargetInternal = DoOutOpObject((int) Op.GetTransactions, s =>
            {
                var w = BinaryUtils.Marshaller.StartMarshal(s);
                w.WriteString(label);
            });
            
            return new TransactionsImpl(this, platformTargetInternal, GetLocalNode().Id, label);
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
        internal IPlatformTargetInternal InteropProcessor
        {
            get { return _proc; }
        }

        /// <summary>
        /// Called when local client node has been disconnected from the cluster.
        /// </summary>
        internal void OnClientDisconnected()
        {
            _clientReconnectTaskCompletionSource = new TaskCompletionSource<bool>();

            var handler = ClientDisconnected;
            if (handler != null)
                handler.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Called when local client node has been reconnected to the cluster.
        /// </summary>
        /// <param name="clusterRestarted">Cluster restarted flag.</param>
        internal void OnClientReconnected(bool clusterRestarted)
        {
            _marsh.OnClientReconnected(clusterRestarted);
            
            _clientReconnectTaskCompletionSource.TrySetResult(clusterRestarted);

            var handler = ClientReconnected;
            if (handler != null)
                handler.Invoke(this, new ClientReconnectEventArgs(clusterRestarted));
        }

        /// <summary>
        /// Gets the plugin processor.
        /// </summary>
        public PluginProcessor PluginProcessor
        {
            get { return _pluginProcessor; }
        }

        /// <summary>
        /// Notify processor that it is safe to use.
        /// </summary>
        internal void ProcessorReleaseStart()
        {
            Target.InLongOutLong((int) Op.ReleaseStart, 0);
        }

        /// <summary>
        /// Checks whether log level is enabled in Java logger.
        /// </summary>
        internal bool LoggerIsLevelEnabled(LogLevel logLevel)
        {
            return Target.InLongOutLong((int) Op.LoggerIsLevelEnabled, (long) logLevel) == True;
        }

        /// <summary>
        /// Logs to the Java logger.
        /// </summary>
        internal void LoggerLog(LogLevel level, string msg, string category, string err)
        {
            Target.InStreamOutLong((int) Op.LoggerLog, w =>
            {
                w.WriteInt((int) level);
                w.WriteString(msg);
                w.WriteString(category);
                w.WriteString(err);
            });
        }

        /// <summary>
        /// Gets the platform plugin extension.
        /// </summary>
        internal IPlatformTarget GetExtension(int id)
        {
            return ((IPlatformTarget) Target).InStreamOutObject((int) Op.GetExtension, w => w.WriteInt(id));
        }
    }
}
