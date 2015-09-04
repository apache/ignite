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
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Events;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Compute;
    using Apache.Ignite.Core.Impl.Events;
    using Apache.Ignite.Core.Impl.Messaging;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.Metadata;
    using Apache.Ignite.Core.Impl.Services;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Messaging;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Services;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Ignite projection implementation.
    /// </summary>
    internal class ClusterGroupImpl : PlatformTarget, IClusterGroupEx
    {
        /** Attribute: platform. */
        private const string AttrPlatform = "org.apache.ignite.platform";

        /** Platform. */
        private const string Platform = "dotnet";

        /** Initial topver; invalid from Java perspective, so update will be triggered when this value is met. */
        private const int TopVerInit = 0;

        /** */
        private const int OpAllMetadata = 1;

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
        private const int OpMetadata = 8;

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

        /** Initial Ignite instance. */
        private readonly Ignite _ignite;
        
        /** Predicate. */
        private readonly Func<IClusterNode, bool> _pred;

        /** Topology version. */
        [SuppressMessage("Microsoft.Performance", "CA1805:DoNotInitializeUnnecessarily")]
        private long _topVer = TopVerInit;

        /** Nodes for the given topology version. */
        private volatile IList<IClusterNode> _nodes;

        /** Processor. */
        private readonly IUnmanagedTarget _proc;

        /** Compute. */
        private readonly Lazy<Compute> _comp;

        /** Messaging. */
        private readonly Lazy<Messaging> _msg;

        /** Events. */
        private readonly Lazy<Events> _events;

        /** Services. */
        private readonly Lazy<IServices> _services;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proc">Processor.</param>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="ignite">Grid.</param>
        /// <param name="pred">Predicate.</param>
        public ClusterGroupImpl(IUnmanagedTarget proc, IUnmanagedTarget target, PortableMarshaller marsh,
            Ignite ignite, Func<IClusterNode, bool> pred)
            : base(target, marsh)
        {
            _proc = proc;
            _ignite = ignite;
            _pred = pred;

            _comp = new Lazy<Compute>(() => 
                new Compute(new ComputeImpl(UU.ProcessorCompute(proc, target), marsh, this, false)));

            _msg = new Lazy<Messaging>(() => new Messaging(UU.ProcessorMessage(proc, target), marsh, this));

            _events = new Lazy<Events>(() => new Events(UU.ProcessorEvents(proc, target), marsh, this));

            _services = new Lazy<IServices>(() => 
                new Services(UU.ProcessorServices(proc, target), marsh, this, false, false));
        }

        /** <inheritDoc /> */
        public IIgnite Ignite
        {
            get { return _ignite; }
        }

        /** <inheritDoc /> */
        public ICompute Compute()
        {
            return _comp.Value;
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

            IUnmanagedTarget prj = DoProjetionOutOp(OpForNodeIds, writer =>
            {
                WriteEnumerable(writer, items, func);
            });
            
            return GetClusterGroup(prj);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            var newPred = _pred == null ? p : node => _pred(node) && p(node);

            return new ClusterGroupImpl(_proc, Target, Marshaller, _ignite, newPred);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            IgniteArgumentCheck.NotNull(name, "name");

            IUnmanagedTarget prj = DoProjetionOutOp(OpForAttribute, writer =>
            {
                writer.WriteString(name);
                writer.WriteString(val);
            });

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
            IUnmanagedTarget prj = DoProjetionOutOp(op, writer =>
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
            return GetClusterGroup(UU.ProjectionForRemotes(Target));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            IgniteArgumentCheck.NotNull(node, "node");

            IUnmanagedTarget prj = DoProjetionOutOp(OpForHost, writer =>
            {
                writer.WriteGuid(node.Id);
            });    
                    
            return GetClusterGroup(prj);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForRandom()
        {
            return GetClusterGroup(UU.ProjectionForRandom(Target));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForOldest()
        {
            return GetClusterGroup(UU.ProjectionForOldest(Target));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForYoungest()
        {
            return GetClusterGroup(UU.ProjectionForYoungest(Target));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForDotNet()
        {
            return ForAttribute(AttrPlatform, Platform);
        }

        /** <inheritDoc /> */
        public ICollection<IClusterNode> Nodes()
        {
            return RefreshNodes();
        }

        /** <inheritDoc /> */
        public IClusterNode Node(Guid id)
        {
            return Nodes().FirstOrDefault(node => node.Id == id);
        }

        /** <inheritDoc /> */
        public IClusterNode Node()
        {
            return Nodes().FirstOrDefault();
        }

        /** <inheritDoc /> */
        public IClusterMetrics Metrics()
        {
            if (_pred == null)
            {
                return DoInOp(OpMetrics, stream =>
                {
                    IPortableRawReader reader = Marshaller.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
                });
            }
            return DoOutInOp(OpMetricsFiltered, writer =>
            {
                WriteEnumerable(writer, Nodes().Select(node => node.Id));
            }, stream =>
            {
                IPortableRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
            });
        }

        /** <inheritDoc /> */
        public IMessaging Message()
        {
            return _msg.Value;
        }

        /** <inheritDoc /> */
        public IEvents Events()
        {
            return _events.Value;
        }

        /** <inheritDoc /> */
        public IServices Services()
        {
            return _services.Value;
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
                    IPortableRawReader reader = Marshaller.StartUnmarshal(stream, false);

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
        /// Creates new Cluster Group from given native projection.
        /// </summary>
        /// <param name="prj">Native projection.</param>
        /// <returns>New cluster group.</returns>
        private IClusterGroup GetClusterGroup(IUnmanagedTarget prj)
        {
            return new ClusterGroupImpl(_proc, prj, Marshaller, _ignite, _pred);
        }

        /// <summary>
        /// Refresh projection nodes.
        /// </summary>
        /// <returns>Nodes.</returns>
        private IList<IClusterNode> RefreshNodes()
        {
            long oldTopVer = Interlocked.Read(ref _topVer);

            List<IClusterNode> newNodes = null;

            DoOutInOp(OpNodes, writer =>
            {
                writer.WriteLong(oldTopVer);
            }, input =>
            {
                PortableReaderImpl reader = Marshaller.StartUnmarshal(input);

                if (reader.ReadBoolean())
                {
                    // Topology has been updated.
                    long newTopVer = reader.ReadLong();

                    newNodes = IgniteUtils.ReadNodes(reader, _pred);

                    UpdateTopology(newTopVer, newNodes);
                }
            });

            if (newNodes != null)
                return newNodes;
            
            // No topology changes.
            Debug.Assert(_nodes != null, "At least one topology update should have occurred.");

            return _nodes;
        }
        
        /// <summary>
        /// Perform synchronous out operation returning value.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action.</param>
        /// <returns>Native projection.</returns>
        private IUnmanagedTarget DoProjetionOutOp(int type, Action<PortableWriterImpl> action)
        {
            using (var stream = IgniteManager.Memory.Allocate().Stream())
            {
                var writer = Marshaller.StartMarshal(stream);

                action(writer);

                FinishMarshal(writer);

                return UU.ProjectionOutOpRet(Target, type, stream.SynchronizeOutput());
            }
        }
        
        /** <inheritDoc /> */
        public IPortableMetadata Metadata(int typeId)
        {
            return DoOutInOp<IPortableMetadata>(OpMetadata, 
                writer =>
                {
                    writer.WriteInt(typeId);
                },
                stream =>
                {
                    PortableReaderImpl reader = Marshaller.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new PortableMetadataImpl(reader) : null;
                }
            );
        }

        /// <summary>
        /// Gets metadata for all known types.
        /// </summary>
        public List<IPortableMetadata> Metadata()
        {
            return DoInOp(OpAllMetadata, s =>
            {
                var reader = Marshaller.StartUnmarshal(s);

                var size = reader.ReadInt();

                var res = new List<IPortableMetadata>(size);

                for (var i = 0; i < size; i++)
                    res.Add(reader.ReadBoolean() ? new PortableMetadataImpl(reader) : null);

                return res;
            });
        }
    }
}
