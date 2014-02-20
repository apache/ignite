// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;

    using System.Collections.Generic;
    using System.Collections.Concurrent;

    using GridGain.Client.Util;
    using GridGain.Client.Hasher;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    /**
     * <summary>
     * Affinity function for partitioned cache. This function supports the following configuration:
     * <ul>
     * <li>
     *      <c>backupFilter</c> - Optional filter for back up nodes. If provided, then only
     *      nodes that pass this filter will be selected as backup nodes and only nodes that
     *      don't pass this filter will be selected as primary nodes. If not provided, then
     *      primary and backup nodes will be selected out of all nodes available for this cache.
     *      <para/>
     *      NOTE: In situations where there are no primary nodes at all, i.e. no nodes for which backup
     *      filter returns <c>false</c>, first backup node for the key will be considered primary.
     * </li>
     * </ul>
     * </summary>
     */
    public class GridClientPartitionedAffinity : IGridClientDataAffinity, IGridClientTopologyListener {
        /** <summary>Default number of partitions.</summary> */
        public const int DefaultPartitionsCount = 10000;

        /** <summary>Node hash.</summary> */
        private readonly GridClientConsistentHash<NodeInfo> nodeHash;

        /** <summary>Added nodes info.</summary> */
        private readonly IDictionary<Guid, NodeInfo> addedNodes = new ConcurrentDictionary<Guid, NodeInfo>();

        /** <summary>Empty constructor with all defaults.</summary> */
        public GridClientPartitionedAffinity()
            : this(DefaultPartitionsCount, null) {
        }

        /**
         * <summary>
         * Initializes optional counts for replicas and backups.
         * <para/>
         * Note that <c>excludeNeighbors</c> parameter is ignored if <c>backupFilter</c> is set.</summary>
         *
         * <param name="parts">Total number of partitions.</param>
         * <param name="backupsFilter">Optional back up filter for nodes. If provided, then primary nodes</param>
         *      will be selected from all nodes outside of this filter, and backups will be selected
         *      from all nodes inside it.
         */
        public GridClientPartitionedAffinity(int parts, Predicate<Guid> backupsFilter) {
            Partitions = parts;
            BackupsFilter = backupsFilter;

            HashIdResolver = (node) => node.ConsistentId;
            nodeHash = new GridClientConsistentHash<NodeInfo>(null, null);
        }

        /**
         * <summary>
         * Total number of key partitions. To ensure that all partitions are
         * equally distributed across all nodes, please make sure that this
         * number is significantly larger than a number of nodes. Also, partition
         * size should be relatively small. Try to avoid having partitions with more
         * than quarter million keys.
         * <para/>
         * Note that for fully replicated caches this method should always
         * return <c>1</c>.</summary>
         */
        public int Partitions {
            get;
            set;
        }

        /**
         * <summary>
         * Optional backup filter. If not <c>null</c>, then primary nodes will be
         * selected from all nodes outside of this filter, and backups will be selected
         * from all nodes inside it.
         * <para/>
         * Note that <c>excludeNeighbors</c> parameter is ignored if <c>backupFilter</c> is set.</summary>
         */
        public Predicate<Guid> BackupsFilter {
            get;
            set;
        }

        /** 
         * <summary>
         * Gets hash ID resolver for nodes. This resolver is used to provide
         * alternate hash ID, other than node ID.
         * <para/>
         * Node IDs constantly change when nodes get restarted, which causes them to
         * be placed on different locations in the hash ring, and hence causing
         * repartitioning. Providing an alternate hash ID, which survives node restarts,
         * puts node on the same location on the hash ring, hence minimizing required
         * repartitioning.</summary>
         */
        public Func<IGridClientNode, Object> HashIdResolver {
            get;
            set;
        }

        /** <inheritdoc /> */
        public TNode Node<TNode>(Object key, ICollection<TNode> nodes) where TNode : IGridClientNode {
            if (nodes == null || nodes.Count == 0)
                return default(TNode);

            if (nodes.Count == 1) {
                foreach (TNode n in nodes)
                    return n;
            }

            // Store nodes in map for fast lookup.
            IDictionary<NodeInfo, TNode> lookup = new Dictionary<NodeInfo, TNode>(nodes.Count);
            foreach (TNode n in nodes)
                lookup.Add(ResolveNodeInfo(n), n);

            NodeInfo nodeInfo;
            int part = Partition(key);

            if (BackupsFilter == null) {
                nodeInfo = nodeHash.Node(part, lookup.Keys);
            }
            else {
                // Select from primary nodes.
                nodeInfo = nodeHash.Node(part, LookupFilter<TNode>(lookup, BackupsFilter, false));

                if (nodeInfo == null)
                    // Select from backup nodes.
                    nodeInfo = nodeHash.Node(part, LookupFilter<TNode>(lookup, BackupsFilter, true));
            }

            TNode node;

            lookup.TryGetValue(nodeInfo, out node);

            return node;
        }

        /** <inheritdoc /> */
        private int Partition(Object key) {
            var hashCode = GridClientJavaHelper.GetJavaHashCode(key);

            return Math.Abs(hashCode % Partitions);
        }

        /** <inheritdoc /> */
        public void OnNodeAdded(IGridClientNode node) {
            // No-op.
        }

        /** <inheritdoc /> */
        public void OnNodeRemoved(IGridClientNode node) {
            // Cleans up node, removed from the topology.

            Guid nodeId = node.Id;
            NodeInfo nodeInfo;
            
            if (!addedNodes.TryGetValue(nodeId, out nodeInfo))
                return;

            if (!addedNodes.Remove(nodeId))
                return;

            nodeHash.RemoveNode(nodeInfo);
        }

        /** 
         * <summary>
         * Resolve node info for specified node.
         * Add node to hash circle if this is the first node invocation.</summary>
         *
         * <param name="node">Node to get info for.</param>
         * <returns>Node info.</returns>
         */
        private NodeInfo ResolveNodeInfo(IGridClientNode node) {
            Guid nodeId = node.Id;
            NodeInfo nodeInfo;
            
            if (addedNodes.TryGetValue(nodeId, out nodeInfo))
                return nodeInfo;

            nodeInfo = new NodeInfo(nodeId, HashIdResolver == null ? nodeId : HashIdResolver(node));

            addedNodes[nodeId] = nodeInfo;
            nodeHash.AddNode(nodeInfo, node.ReplicaCount);

            return nodeInfo;
        }

        /** <summary>Lookup filter.</summary> */
        private static Predicate<NodeInfo> LookupFilter<TNode>(IDictionary<NodeInfo, TNode> lookup, Predicate<Guid> backuped, bool inBackup) {
            Dbg.Assert(backuped != null, "backuped != null");

            return delegate(NodeInfo info) {
                return lookup.ContainsKey(info) && backuped(info.NodeId) == inBackup;
            };
        }

        /** <summary>Node info.</summary> */
        private class NodeInfo : IComparable<NodeInfo>, IGridClientConsistentHashObject {
            /** Node Id. */
            public Guid NodeId {
                get;
                private set;
            }

            /** Hash Id. */
            public Object HashId {
                get;
                private set;
            }

            /**
             * @param nodeId Node Id.
             * @param hashId Hash Id.
             */
            public NodeInfo(Guid nodeId, Object hashId) {
                Dbg.Assert(nodeId != null, "nodeId != null");
                Dbg.Assert(hashId != null, "hashId != null");

                this.NodeId = nodeId;
                this.HashId = hashId;
            }

            /** {@inheritDoc} */
            override public int GetHashCode() {
                // This code will be used to place Node on ring,
                // so apply java-style hashing as on server.
                return GridClientJavaHelper.GetJavaHashCode(HashId);
            }

            /** {@inheritDoc} */
            override public bool Equals(Object obj) {
                if (!(obj is NodeInfo))
                    return false;

                NodeInfo that = (NodeInfo)obj;

                return that.NodeId.Equals(NodeId) && that.HashId.Equals(HashId);
            }

            /** {@inheritDoc} */
            public int CompareTo(NodeInfo other) {
                return NodeId.CompareTo(other.NodeId);
            }

            /** {@inheritDoc} */
            override public String ToString() {
                return typeof(NodeInfo).Name +
                    " [nodeId=" + NodeId +
                    ", hashId=" + HashId + ']';
            }
        }
    }
}
