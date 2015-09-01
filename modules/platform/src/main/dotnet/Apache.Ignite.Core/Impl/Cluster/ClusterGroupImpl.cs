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
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Compute;
    using GridGain.Events;
    using GridGain.Impl.Compute;
    using GridGain.Impl.Events;
    using GridGain.Impl.Messaging;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Services;
    using GridGain.Impl.Unmanaged;
    using GridGain.Portable;
    using GridGain.Services;

    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;
    using A = Apache.Ignite.Core.Impl.Common.GridArgumentCheck;
    using U = GridGain.Impl.GridUtils;

    /// <summary>
    /// Grid projection implementation.
    /// </summary>
    internal class ClusterGroupImpl : GridTarget, IClusterGroupEx
    {
        /** Attribute: platform. */
        private const string ATTR_PLATFORM = "org.gridgain.interop.platform";

        /** Platform. */
        private const string PLATFORM = "dotnet";

        /** Initial topver; invalid from Java perspective, so update will be triggered when this value is met. */
        private const int TOP_VER_INIT = 0;

        /** */
        private const int OP_ALL_METADATA = 1;

        /** */
        private const int OP_FOR_ATTRIBUTE = 2;

        /** */
        private const int OP_FOR_CACHE = 3;

        /** */
        private const int OP_FOR_CLIENT = 4;

        /** */
        private const int OP_FOR_DATA = 5;

        /** */
        private const int OP_FOR_HOST = 6;

        /** */
        private const int OP_FOR_NODE_IDS = 7;

        /** */
        private const int OP_METADATA = 8;

        /** */
        private const int OP_METRICS = 9;

        /** */
        private const int OP_METRICS_FILTERED = 10;

        /** */
        private const int OP_NODE_METRICS = 11;

        /** */
        private const int OP_NODES = 12;

        /** */
        private const int OP_PING_NODE = 13;

        /** */
        private const int OP_TOPOLOGY = 14;

        /** Initial grid instance. */
        private readonly GridImpl grid;
        
        /** Predicate. */
        private readonly Func<IClusterNode, bool> pred;

        /** Topology version. */
        [SuppressMessage("Microsoft.Performance", "CA1805:DoNotInitializeUnnecessarily")]
        private long topVer = TOP_VER_INIT;

        /** Nodes for the given topology version. */
        private volatile IList<IClusterNode> nodes;

        /** Processor. */
        private readonly IUnmanagedTarget proc;

        /** Compute. */
        private readonly Lazy<Compute> comp;

        /** Messaging. */
        private readonly Lazy<Messaging> msg;

        /** Events. */
        private readonly Lazy<Events> events;

        /** Services. */
        private readonly Lazy<IServices> services;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="proc">Processor.</param>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="grid">Grid.</param>
        /// <param name="pred">Predicate.</param>
        public ClusterGroupImpl(IUnmanagedTarget proc, IUnmanagedTarget target, PortableMarshaller marsh,
            GridImpl grid, Func<IClusterNode, bool> pred)
            : base(target, marsh)
        {
            this.proc = proc;
            this.grid = grid;
            this.pred = pred;

            comp = new Lazy<Compute>(() => 
                new Compute(new ComputeImpl(UU.ProcessorCompute(proc, target), marsh, this, false)));

            msg = new Lazy<Messaging>(() => new Messaging(UU.ProcessorMessage(proc, target), marsh, this));

            events = new Lazy<Events>(() => new Events(UU.ProcessorEvents(proc, target), marsh, this));

            services = new Lazy<IServices>(() => 
                new Services(UU.ProcessorServices(proc, target), marsh, this, false, false));
        }

        /** <inheritDoc /> */
        public IIgnite Grid
        {
            get { return grid; }
        }

        /** <inheritDoc /> */
        public ICompute Compute()
        {
            return comp.Value;
        }

        /** <inheritDoc /> */
        public IClusterGroup ForNodes(IEnumerable<IClusterNode> nodes)
        {
            A.NotNull(nodes, "nodes");

            return ForNodeIds0(nodes, node => node.Id);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForNodes(params IClusterNode[] nodes)
        {
            A.NotNull(nodes, "nodes");

            return ForNodeIds0(nodes, node => node.Id);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForNodeIds(IEnumerable<Guid> ids)
        {
            A.NotNull(ids, "ids");

            return ForNodeIds0(ids, null);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForNodeIds(params Guid[] ids)
        {
            A.NotNull(ids, "ids");

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

            IUnmanagedTarget prj = DoProjetionOutOp(OP_FOR_NODE_IDS, writer =>
            {
                WriteEnumerable(writer, items, func);
            });
            
            return GetClusterGroup(prj);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForPredicate(Func<IClusterNode, bool> p)
        {
            var newPred = pred == null ? p : node => pred(node) && p(node);

            return new ClusterGroupImpl(proc, target, marsh, grid, newPred);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForAttribute(string name, string val)
        {
            A.NotNull(name, "name");

            IUnmanagedTarget prj = DoProjetionOutOp(OP_FOR_ATTRIBUTE, writer =>
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
            return ForCacheNodes(name, OP_FOR_CACHE);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForDataNodes(string name)
        {
            return ForCacheNodes(name, OP_FOR_DATA);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForClientNodes(string name)
        {
            return ForCacheNodes(name, OP_FOR_CLIENT);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForRemotes()
        {
            return GetClusterGroup(UU.ProjectionForRemotes(target));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForHost(IClusterNode node)
        {
            A.NotNull(node, "node");

            IUnmanagedTarget prj = DoProjetionOutOp(OP_FOR_HOST, writer =>
            {
                writer.WriteGuid(node.Id);
            });    
                    
            return GetClusterGroup(prj);
        }

        /** <inheritDoc /> */
        public IClusterGroup ForRandom()
        {
            return GetClusterGroup(UU.ProjectionForRandom(target));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForOldest()
        {
            return GetClusterGroup(UU.ProjectionForOldest(target));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForYoungest()
        {
            return GetClusterGroup(UU.ProjectionForYoungest(target));
        }

        /** <inheritDoc /> */
        public IClusterGroup ForDotNet()
        {
            return ForAttribute(ATTR_PLATFORM, PLATFORM);
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
            if (pred == null)
            {
                return DoInOp(OP_METRICS, stream =>
                {
                    IPortableRawReader reader = marsh.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
                });
            }
            else
            {
                return DoOutInOp(OP_METRICS_FILTERED, writer =>
                {
                    WriteEnumerable(writer, Nodes().Select(node => node.Id));
                }, stream =>
                {
                    IPortableRawReader reader = marsh.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
                });
            }
        }

        /** <inheritDoc /> */
        public IMessaging Message()
        {
            return msg.Value;
        }

        /** <inheritDoc /> */
        public IEvents Events()
        {
            return events.Value;
        }

        /** <inheritDoc /> */
        public IServices Services()
        {
            return services.Value;
        }

        /// <summary>
        /// Pings a remote node.
        /// </summary>
        /// <param name="nodeId">ID of a node to ping.</param>
        /// <returns>True if node for a given ID is alive, false otherwise.</returns>
        internal bool PingNode(Guid nodeId)
        {
            return DoOutOp(OP_PING_NODE, nodeId) == TRUE;
        }

        /// <summary>
        /// Predicate (if any).
        /// </summary>
        public Func<IClusterNode, bool> Predicate
        {
            get { return pred; }
        }

        /// <summary>
        /// Refresh cluster node metrics.
        /// </summary>
        /// <param name="nodeId">Node</param>
        /// <param name="lastUpdateTime"></param>
        /// <returns></returns>
        internal ClusterMetricsImpl RefreshClusterNodeMetrics(Guid nodeId, long lastUpdateTime)
        {
            return DoOutInOp(OP_NODE_METRICS, writer =>
                {
                    writer.WriteGuid(nodeId);
                    writer.WriteLong(lastUpdateTime);
                }, stream =>
                {
                    IPortableRawReader reader = marsh.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new ClusterMetricsImpl(reader) : null;
                }
            );
        }

        /// <summary>
        /// Gets a topology by version. Returns null if topology history storage doesn't contain 
        /// specified topology version (history currently keeps the last 1000 snapshots).
        /// </summary>
        /// <param name="version">Topology version.</param>
        /// <returns>Collection of grid nodes which represented by specified topology version, 
        /// if it is present in history storage, {@code null} otherwise.</returns>
        /// <exception cref="IgniteException">If underlying SPI implementation does not support 
        /// topology history. Currently only {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}
        /// supports topology history.</exception>
        internal ICollection<IClusterNode> Topology(long version)
        {
            return DoOutInOp(OP_TOPOLOGY, writer => writer.WriteLong(version), 
                input => U.ReadNodes(marsh.StartUnmarshal(input)));
        }

        /// <summary>
        /// Topology version.
        /// </summary>
        internal long TopologyVersion
        {
            get
            {
                RefreshNodes();

                return Interlocked.Read(ref topVer);
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
                if (topVer < newTopVer)
                {
                    Interlocked.Exchange(ref topVer, newTopVer);

                    nodes = newNodes.AsReadOnly();
                }
            }
        }

        /// <summary>
        /// Get current nodes without refreshing the topology.
        /// </summary>
        /// <returns>Current nodes.</returns>
        internal IList<IClusterNode> NodesNoRefresh()
        {
            return nodes;
        }

        /// <summary>
        /// Creates new Grid Cluster Group from given native projection.
        /// </summary>
        /// <param name="prj">Native projection.</param>
        /// <returns>New cluster group.</returns>
        private IClusterGroup GetClusterGroup(IUnmanagedTarget prj)
        {
            return new ClusterGroupImpl(proc, prj, marsh, grid, pred);
        }

        /// <summary>
        /// Refresh projection nodes.
        /// </summary>
        /// <returns>Nodes.</returns>
        private IList<IClusterNode> RefreshNodes()
        {
            long oldTopVer = Interlocked.Read(ref topVer);

            List<IClusterNode> newNodes = null;

            DoOutInOp(OP_NODES, writer =>
            {
                writer.WriteLong(oldTopVer);
            }, input =>
            {
                PortableReaderImpl reader = marsh.StartUnmarshal(input);

                if (reader.ReadBoolean())
                {
                    // Topology has been updated.
                    long newTopVer = reader.ReadLong();

                    newNodes = U.ReadNodes(reader, pred);

                    UpdateTopology(newTopVer, newNodes);
                }
            });

            if (newNodes != null)
                return newNodes;
            
            // No topology changes.
            Debug.Assert(nodes != null, "At least one topology update should have occurred.");

            return nodes;
        }
        
        /// <summary>
        /// Perform synchronous out operation returning value.
        /// </summary>
        /// <param name="type">Operation type.</param>
        /// <param name="action">Action.</param>
        /// <returns>Native projection.</returns>
        private IUnmanagedTarget DoProjetionOutOp(int type, Action<PortableWriterImpl> action)
        {
            using (var stream = GridManager.Memory.Allocate().Stream())
            {
                var writer = marsh.StartMarshal(stream);

                action(writer);

                FinishMarshal(writer);

                return UU.ProjectionOutOpRet(target, type, stream.SynchronizeOutput());
            }
        }
        
        /** <inheritDoc /> */
        public IPortableMetadata Metadata(int typeId)
        {
            return DoOutInOp<IPortableMetadata>(OP_METADATA, 
                writer =>
                {
                    writer.WriteInt(typeId);
                },
                stream =>
                {
                    PortableReaderImpl reader = marsh.StartUnmarshal(stream, false);

                    return reader.ReadBoolean() ? new PortableMetadataImpl(reader) : null;
                }
            );
        }

        /// <summary>
        /// Gets metadata for all known types.
        /// </summary>
        public List<IPortableMetadata> Metadata()
        {
            return DoInOp(OP_ALL_METADATA, s =>
            {
                var reader = marsh.StartUnmarshal(s);

                var size = reader.ReadInt();

                var res = new List<IPortableMetadata>(size);

                for (var i = 0; i < size; i++)
                    res.Add(reader.ReadBoolean() ? new PortableMetadataImpl(reader) : null);

                return res;
            });
        }
    }
}
