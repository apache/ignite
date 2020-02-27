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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cache;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Compute;
    using Apache.Ignite.Core.Impl.Events;
    using Apache.Ignite.Core.Impl.Messaging;
    using Apache.Ignite.Core.Impl.PersistentStore;
    using Apache.Ignite.Core.Impl.Services;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.PersistentStore;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Ignite projection implementation.
    /// </summary>
    internal class ClusterGroupImpl : PlatformTargetAdapter, IClusterGroup
    {
        /** Attribute: platform. */
        private const string AttrPlatform = "org.apache.ignite.platform";

        /** Platform. */
        private const string Platform = "dotnet";

        /** Initial topver; invalid from Java perspective, so update will be triggered when this value is met. */
        private const int TopVerInit = 0;

        /** */
        private const int OpForAttribute = 2;

        /** */
        private const int OpForCache = 3;

        /** */
        private const int OpForClient = 4;

        /** */
        private const int OpForData = 5;

        /** */
        private const int OpForHost = 6;

        /** */
        private const int OpForNodeIds = 7;

        /** */
        private const int OpMetrics = 9;

        /** */
        private const int OpMetricsFiltered = 10;

        /** */
        private const int OpNodeMetrics = 11;

        /** */
        private const int OpNodes = 12;

        /** */
        private const int OpPingNode = 13;

        /** */
        private const int OpTopology = 14;

        /** */
        private const int OpForRemotes = 17;

        /** */
        private const int OpForDaemons = 18;

        /** */
        private const int OpForRandom = 19;
        
        /** */
        private const int OpForOldest = 20;
        
        /** */
        private const int OpForYoungest = 21;
        
        /** */
        private const int OpResetMetrics = 22;
        
        /** */
        private const int OpForServers = 23;
        
        /** */
        private const int OpCacheMetrics = 24;
        
        /** */
        private const int OpResetLostPartitions = 25;

        /** */
        private const int OpMemoryMetrics = 26;

        /** */
        private const int OpMemoryMetricsByName = 27;

        /** */
        private const int OpSetActive = 28;

        /** */
        private const int OpIsActive = 29;

        /** */
        private const int OpGetPersistentStoreMetrics = 30;

        /** */
        private const int OpGetCompute = 31;

        /** */
        private const int OpGetMessaging = 32;

        /** */
        private const int OpGetEvents = 33;

        /** */
        private const int OpGetServices = 34;

        /** */
        private const int OpDataRegionMetrics = 35;

        /** */
        private const int OpDataRegionMetricsByName = 36;

        /** */
        private const int OpDataStorageMetrics = 37;

        /** */
        private const int OpEnableStatistics = 38;

        /** Initial Ignite instance. */
        private readonly IIgniteInternal _ignite;
        
        /** Predicate. */
        private readonly Func<IClusterNode, bool> _pred;

        /** Topology version. */
        private long _topVer = TopVerInit;

        /** Nodes for the given topology version. */
        private volatile IList<IClusterNode> _nodes;

        /** Compute. */
        private readonly Lazy<ICompute> _comp;

        /** Messaging. */
        private readonly Lazy<IMessaging> _msg;

        /** Events. */
        private readonly Lazy<IEvents> _events;

        /** Services. */
        private readonly Lazy<IServices> _services;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="pred">Predicate.</param>
        [SuppressMessage("Microsoft.Performance", "CA1805:DoNotInitializeUnnecessarily")]
        public ClusterGroupImpl(IPlatformTargetInternal target, Func<IClusterNode, bool> pred)
            : base(target)
        {
            _ignite = target.Marshaller.Ignite;
            _pred = pred;

            _comp = new Lazy<ICompute>(() => CreateCompute());
            _msg = new Lazy<IMessaging>(() => CreateMessaging());
            _events = new Lazy<IEvents>(() => CreateEvents());
            _services = new Lazy<IServices>(() => CreateServices());
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get { return _ignite.GetIgnite(); }
        }

        /** <inheritDoc /> */
        public ICompute GetCompute()
        {
            return _comp.Value;
        }

        /// <summary>
        /// Creates the compute.
        /// </summary>
        private ICompute CreateCompute()
        {
            return new Compute(new ComputeImpl(DoOutOpObject(OpGetCompute), this, false));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            IgniteArgumentCheck.NotNull(nodes, "nodes");

            return ForNodeIds0(nodes, node => node.Id);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            IgniteArgumentCheck.NotNull(nodes, "nodes");

            return ForNodeIds0(nodes, node => node.Id);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
        {
            IgniteArgumentCheck.NotNull(ids, "ids");

            return ForNodeIds0(ids, null);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForNodeIds(params Guid[] ids)
        {
            IgniteArgumentCheck.NotNull(ids, "ids");

            return ForNodeIds0(ids, null);
        }

        /// <summary>
        /// Internal routine to get projection for specific node IDs.
        /// </summary>
        /// <param name="items">Items.</param>
        /// <param name="func">Function to transform item to Guid (optional).</param>
        /// <returns></returns>
        private IClusterGroup ForNodeIds0<T>(IEnumerable<T> items, Func<T, Guid> func)
        {
            Debug.Assert(items != null);

            var prj = DoOutOpObject(OpForNodeIds, writer => writer.WriteEnumerable(items, func));
            
            return GetClusterGroup(prj);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            var newPred = _pred == null ? p : node => _pred(node) && p(node);

            return new ClusterGroupImpl(Target, newPred);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            Action<BinaryWriter> action = writer =>
            {
                writer.WriteString(name);
                writer.WriteString(val);
            };
            var prj = DoOutOpObject(OpForAttribute, action);

            return GetClusterGroup(prj);
        }

        /// <summary>
        /// Creates projection with a specified op.
        /// </summary>
        /// <param name="name">Cache name to include into projection.</param>
        /// <param name="op">Operation id.</param>
        /// <returns>
        /// Projection over nodes that have specified cache running.
        /// </returns>
        private IClusterGroup ForCacheNodes(string name, int op)
        {
            var prj = DoOutOpObject(op, writer =>
            {
                writer.WriteString(name);
            });

            return GetClusterGroup(prj);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForCacheNodes(string name)
        {
            return ForCacheNodes(name, OpForCache);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForDataNodes(string name)
        {
            return ForCacheNodes(name, OpForData);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForClientNodes(string name)
        {
            return ForCacheNodes(name, OpForClient);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForRemotes()
        {
            return GetClusterGroup(DoOutOpObject(OpForRemotes));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForDaemons()
        {
            return GetClusterGroup(DoOutOpObject(OpForDaemons));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            IgniteArgumentCheck.NotNull(node, "node");

            var prj = DoOutOpObject(OpForHost, writer =>
            {
                writer.WriteGuid(node.Id);
            });    
                    
            return GetClusterGroup(prj);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForRandom()
        {
            return GetClusterGroup(DoOutOpObject(OpForRandom));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForOldest()
        {
            return GetClusterGroup(DoOutOpObject(OpForOldest));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForYoungest()
        {
            return GetClusterGroup(DoOutOpObject(OpForYoungest));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForServers()
        {
            return GetClusterGroup(DoOutOpObject(OpForServers));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForDotNet()
        {
            return ForAttribute(AttrPlatform, Platform);
        }

        /** <inheritDoc /> */
        public ICollection<IClusterNode> GetNodes()
        {
            return RefreshNodes();
        }

        /** <inheritDoc /> */
        public IClusterNode GetNode(Guid id)
        {
            return GetNodes().FirstOrDefault(node => node.Id == id);
        }

        /** <inheritDoc /> */
        public IClusterNode GetNode()
        {
            return GetNodes().FirstOrDefault();
        }

        /** <inheritDoc /> */
        public IClusterMetrics GetMetrics()
        {
            if (_pred == null)
            {
                return DoInOp(OpMetrics, stream =>
                {
                    IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
                });
            }
            return DoOutInOp(OpMetricsFiltered,
                writer => writer.WriteEnumerable(GetNodes().Select(node => node.Id)),
                stream =>
                {
                    IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
                });
        }

        /** <inheritDoc /> */
        public IMessaging GetMessaging()
        {
            return _msg.Value;
        }

        /// <summary>
        /// Creates the messaging.
        /// </summary>
        private IMessaging CreateMessaging()
        {
            return new Messaging(DoOutOpObject(OpGetMessaging), this);
        }

        /** <inheritDoc /> */
        public IEvents GetEvents()
        {
            return _events.Value;
        }

        /// <summary>
        /// Creates the events.
        /// </summary>
        private IEvents CreateEvents()
        {
            return new Events(DoOutOpObject(OpGetEvents), this);
        }

        /** <inheritDoc /> */
        public IServices GetServices()
        {
            return _services.Value;
        }

        /** <inheritDoc /> */
        public void EnableStatistics(IEnumerable<string> cacheNames, bool enabled)
        {
            IgniteArgumentCheck.NotNull(cacheNames, "cacheNames");

            DoOutOp(OpEnableStatistics, w =>
            {
                w.WriteBoolean(enabled);

                var pos = w.Stream.Position;

                var count = 0;
                w.WriteInt(count);  // Reserve space.

                foreach (var cacheName in cacheNames)
                {
                    w.WriteString(cacheName);
                    count++;
                }

                w.Stream.WriteInt(pos, count);
            });
        }

        /// <summary>
        /// Creates the services.
        /// </summary>
        private IServices CreateServices()
        {
            return new Services(DoOutOpObject(OpGetServices), this, false, false);
        }

        /// <summary>
        /// Pings a remote node.
        /// </summary>
        /// <param name="nodeId">ID of a node to ping.</param>
        /// <returns>True if node for a given ID is alive, false otherwise.</returns>
        internal bool PingNode(Guid nodeId)
        {
            return DoOutOp(OpPingNode, nodeId) == True;
        }

        /// <summary>
        /// Predicate (if any).
        /// </summary>
        public Func<IClusterNode, bool> Predicate
        {
            get { return _pred; }
        }

        /// <summary>
        /// Refresh cluster node metrics.
        /// </summary>
        /// <param name="nodeId">Node</param>
        /// <param name="lastUpdateTime"></param>
        /// <returns></returns>
        internal ClusterMetricsImpl RefreshClusterNodeMetrics(Guid nodeId, long lastUpdateTime)
        {
            return DoOutInOp(OpNodeMetrics, writer =>
                {
                    writer.WriteGuid(nodeId);
                    writer.WriteLong(lastUpdateTime);
                }, stream =>
                {
                    IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
                }
            );
        }

        /// <summary>
        /// Gets a topology by version. Returns null if topology history storage doesn't contain 
        /// specified topology version (history currently keeps the last 1000 snapshots).
        /// </summary>
        /// <param name="version">Topology version.</param>
        /// <returns>Collection of Ignite nodes which represented by specified topology version, 
        /// if it is present in history storage, {@code null} otherwise.</returns>
        /// <exception cref="IgniteException">If underlying SPI implementation does not support 
        /// topology history. Currently only {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}
        /// supports topology history.</exception>
        internal ICollection<IClusterNode> Topology(long version)
        {
            return DoOutInOp(OpTopology, writer => writer.WriteLong(version), 
                input => IgniteUtils.ReadNodes(Marshaller.StartUnmarshal(input)));
        }

        /// <summary>
        /// Topology version.
        /// </summary>
        internal long TopologyVersion
        {
            get
            {
                RefreshNodes();

                return Interlocked.Read(ref _topVer);
            }
        }

        /// <summary>
        /// Update topology.
        /// </summary>
        /// <param name="newTopVer">New topology version.</param>
        /// <param name="newNodes">New nodes.</param>
        internal void UpdateTopology(long newTopVer, List<IClusterNode> newNodes)
        {
            lock (this)
            {
                // If another thread already advanced topology version further, we still
                // can safely return currently received nodes, but we will not assign them.
                if (_topVer < newTopVer)
                {
                    Interlocked.Exchange(ref _topVer, newTopVer);

                    _nodes = newNodes.AsReadOnly();
                }
            }
        }

        /// <summary>
        /// Get current nodes without refreshing the topology.
        /// </summary>
        /// <returns>Current nodes.</returns>
        internal IList<IClusterNode> NodesNoRefresh()
        {
            return _nodes;
        }

        /// <summary>
        /// Resets the metrics.
        /// </summary>
        public void ResetMetrics()
        {
            DoOutInOp(OpResetMetrics);
        }

        /// <summary>
        /// Resets the lost partitions.
        /// </summary>
        public void ResetLostPartitions(IEnumerable<string> cacheNames)
        {
            IgniteArgumentCheck.NotNull(cacheNames, "cacheNames");

            DoOutOp(OpResetLostPartitions, w =>
            {
                var pos = w.Stream.Position;

                var count = 0;
                w.WriteInt(count);  // Reserve space.

                foreach (var cacheName in cacheNames)
                {
                    w.WriteString(cacheName);
                    count++;
                }

                w.Stream.WriteInt(pos, count);
            });
        }

        /// <summary>
        /// Gets the cache metrics within this cluster group.
        /// </summary>
        /// <param name="cacheName">Name of the cache.</param>
        /// <returns>Metrics.</returns>
        public ICacheMetrics GetCacheMetrics(string cacheName)
        {
            return DoOutInOp(OpCacheMetrics, w => w.WriteString(cacheName), stream =>
            {
                IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return new CacheMetricsImpl(reader);
            });
        }

        /// <summary>
        /// Gets the memory metrics.
        /// </summary>
#pragma warning disable 618
        public ICollection<IMemoryMetrics> GetMemoryMetrics()
        {
            return DoInOp(OpMemoryMetrics, stream =>
            {
                IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                var cnt = reader.ReadInt();

                var res = new List<IMemoryMetrics>(cnt);

                for (int i = 0; i < cnt; i++)
                {
                    res.Add(new MemoryMetrics(reader));
                }

                return res;
            });
        }

        /// <summary>
        /// Gets the memory metrics.
        /// </summary>
        public IMemoryMetrics GetMemoryMetrics(string memoryPolicyName)
        {
            return DoOutInOp(OpMemoryMetricsByName, w => w.WriteString(memoryPolicyName),
                stream => stream.ReadBool() ? new MemoryMetrics(Marshaller.StartUnmarshal(stream, false)) : null);
        }
#pragma warning restore 618

        /// <summary>
        /// Gets the data region metrics.
        /// </summary>
        public ICollection<IDataRegionMetrics> GetDataRegionMetrics()
        {
            return DoInOp(OpDataRegionMetrics, stream =>
            {
                IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                var cnt = reader.ReadInt();

                var res = new List<IDataRegionMetrics>(cnt);

                for (int i = 0; i < cnt; i++)
                {
                    res.Add(new DataRegionMetrics(reader));
                }

                return res;
            });
        }

        /// <summary>
        /// Gets the data region metrics.
        /// </summary>
        public IDataRegionMetrics GetDataRegionMetrics(string memoryPolicyName)
        {
            return DoOutInOp(OpDataRegionMetricsByName, w => w.WriteString(memoryPolicyName),
                stream => stream.ReadBool() ? new DataRegionMetrics(Marshaller.StartUnmarshal(stream, false)) : null);
        }

        /// <summary>
        /// Gets the data storage metrics.
        /// </summary>
        public IDataStorageMetrics GetDataStorageMetrics()
        {
            return DoInOp(OpDataStorageMetrics, stream =>
                new DataStorageMetrics(Marshaller.StartUnmarshal(stream, false)));
        }

        /// <summary>
        /// Changes Ignite grid state to active or inactive.
        /// </summary>
        public void SetActive(bool isActive)
        {
            DoOutInOp(OpSetActive, isActive ? True : False);
        }

        /// <summary>
        /// Determines whether this grid is in active state.
        /// </summary>
        /// <returns>
        ///   <c>true</c> if the grid is active; otherwise, <c>false</c>.
        /// </returns>
        public bool IsActive()
        {
            return DoOutInOp(OpIsActive) == True;
        }

        /// <summary>
        /// Gets the persistent store metrics.
        /// </summary>
#pragma warning disable 618
        public IPersistentStoreMetrics GetPersistentStoreMetrics()
        {
            return DoInOp(OpGetPersistentStoreMetrics, stream =>
                new PersistentStoreMetrics(Marshaller.StartUnmarshal(stream, false)));
        }
#pragma warning restore 618

        /// <summary>
        /// Creates new Cluster Group from given native projection.
        /// </summary>
        /// <param name="prj">Native projection.</param>
        /// <returns>New cluster group.</returns>
        private IClusterGroup GetClusterGroup(IPlatformTargetInternal prj)
        {
            return new ClusterGroupImpl(prj, _pred);
        }

        /// <summary>
        /// Refresh projection nodes.
        /// </summary>
        /// <returns>Nodes.</returns>
        private IList<IClusterNode> RefreshNodes()
        {
            long oldTopVer = Interlocked.Read(ref _topVer);

            var res = Target.InStreamOutStream(OpNodes, writer =>
            {
                writer.WriteLong(oldTopVer);
            }, reader =>
            {
                if (reader.ReadBoolean())
                {
                    // Topology has been updated.
                    long newTopVer = reader.ReadLong();
                    var newNodes = IgniteUtils.ReadNodes((BinaryReader) reader, _pred);

                    return Tuple.Create(newTopVer, newNodes);
                }

                return null;
            });

            if (res != null)
            {
                UpdateTopology(res.Item1, res.Item2);

                return res.Item2;
            }

            // No topology changes.
            Debug.Assert(_nodes != null, "At least one topology update should have occurred.");

            return _nodes;
        }
    }
}
