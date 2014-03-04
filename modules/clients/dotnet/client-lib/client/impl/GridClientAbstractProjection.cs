/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Linq;
    using System.Collections.Generic;
    using GridGain.Client;
    using GridGain.Client.Balancer;
    using GridGain.Client.Util;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using N = GridGain.Client.IGridClientNode;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    /** <summary>Class contains common connection-error handling logic.</summary> */
    internal abstract class GridClientAbstractProjection<T> where T : GridClientAbstractProjection<T> {
        /** <summary>Count of reconnect retries before exception is thrown.</summary> */
        private const int RetryCount = 3;

        /** <summary>List of nodes included in this projection. If null, all nodes in topology are included.</summary> */
        protected readonly IList<N> _nodes;

        /** <summary>Node filter to be applied for this projection.</summary> */
        protected readonly Predicate<N> _filter;

        /** <summary>Node filter to be applied for this projection.</summary> */
        protected readonly IGridClientLoadBalancer _balancer;

        /** <summary>Projection configuration.</summary> */
        protected readonly IGridClientProjectionConfig cfg;

        /**
         * <summary>
         * Creates projection with specified client.</summary>
         *
         * <param name="cfg">Progjection configuration.</param>
         * <param name="nodes">Collections of nodes included in this projection.</param>
         * <param name="filter">Node filter to be applied.</param>
         * <param name="balancer">Balancer to use.</param>
         */
        protected internal GridClientAbstractProjection(IGridClientProjectionConfig cfg, IEnumerable<N> nodes,
            Predicate<N> filter, IGridClientLoadBalancer balancer) {
            A.NotNull(cfg, "projection config");

            this.cfg = cfg;
            this._nodes = nodes == null ? null : new List<N>(nodes);
            this._filter = filter;
            this._balancer = balancer;
        }

        /**
         * <summary>
         * This method executes request to a communication layer and handles connection error, if it occurs.
         * In case of communication exception client instance is notified and new instance of client is created.
         * If none of the grid servers can be reached, an exception is thrown.</summary>
         *
         * <param name="c">Closure to be executed.</param>
         * <returns>Closure result.</returns>
         */
        protected IGridClientFuture<TRes> WithReconnectHandling<TRes>(Func<IGridClientConnection, Guid, IGridClientFuture<TRes>> c) {
            try {
                N node = null;

                bool changeNode = false;

                GridClientConnectionManager connMgr = cfg.ConnectionManager;

                for (int i = 0; i < RetryCount; i++) {
                    if (node == null || changeNode)
                        node = BalancedNode();

                    IGridClientConnection conn = null;

                    try {
                        conn = connMgr.connection(node);

                        return c(conn, node.Id);
                    }
                    catch (GridClientConnectionIdleClosedException e) {
                        connMgr.onFacadeFailed(node, conn, e);

                        // It's ok, just reconnect to the same node.
                        changeNode = false;
                    }
                    catch (GridClientConnectionResetException e) {
                        connMgr.onFacadeFailed(node, conn, e);

                        changeNode = true;
                    }
                }

                throw new GridClientServerUnreachableException("Failed to communicate with grid nodes " +
                    "(maximum count of retries reached).");
            }
            catch (GridClientException e) {
                return new GridClientFinishedFuture<TRes>(() => {
                    throw e;
                });
            }
        }

        /**
         * <summary>
         * This method executes request to a communication layer and handles connection error, if it occurs. Server
         * is picked up according to the projection affinity and key given. Connection will be made with the node
         * on which key is cached. In case of communication exception client instance is notified and new instance
         * of client is created. If none of servers can be reached, an exception is thrown.</summary>
         *
         * <param name="c">Closure to be executed.</param>
         * <param name="cacheName">Cache name for which mapped node will be calculated.</param>
         * <param name="affKey">Affinity key.</param>
         * <returns>Closure result.</returns>
         */
        protected IGridClientFuture<TRes> WithReconnectHandling<TRes>(Func<IGridClientConnection, Guid, IGridClientFuture<TRes>> c, String cacheName, Object affKey) {
            IGridClientDataAffinity affinity = cfg.Affinity(cacheName);

            // If pinned (fixed-nodes) or no affinity provided use balancer.
            if (_nodes != null || affinity == null)
                return WithReconnectHandling(c);

            try {
                IList<N> prjNodes = ProjectionNodes();

                if (prjNodes.Count == 0)
                    throw new GridClientServerUnreachableException("Failed to get affinity node" +
                        " (no nodes in topology were accepted by the filter): " + _filter);

                N node = affinity.Node(affKey, prjNodes);

                GridClientConnectionManager connMgr = cfg.ConnectionManager;

                for (int i = 0; i < RetryCount; i++) {
                    IGridClientConnection conn = null;

                    try {
                        conn = connMgr.connection(node);

                        return c(conn, node.Id);
                    }
                    catch (GridClientConnectionIdleClosedException e) {
                        connMgr.onFacadeFailed(node, conn, e);
                    }
                    catch (GridClientConnectionResetException e) {
                        connMgr.onFacadeFailed(node, conn, e);

                        if (!CheckNodeAlive(node.Id))
                            throw new GridClientServerUnreachableException("Failed to communicate with mapped grid node" +
                                " for given affinity key (node left the grid)" +
                                " [nodeId=" + node.Id + ", affKey=" + affKey + ']');
                    }
                }

                throw new GridClientServerUnreachableException("Failed to communicate with mapped grid node for given affinity " +
                    "key (did node left the grid?) [nodeId=" + node.Id + ", affKey=" + affKey + ']');
            }
            catch (GridClientException e) {
                return new GridClientFinishedFuture<TRes>(() => {
                    throw e;
                });
            }
        }

        /**
         * <summary>
         * Tries to refresh node on every possible connection in topology.</summary>
         *
         * <param name="nodeId">Node id to check.</param>
         * <returns><c>True</c> if response was received, <c>null</c> if either <c>null</c> response received
         * or no nodes can be contacted at all.</returns>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        protected bool CheckNodeAlive(Guid nodeId) {
            GridClientConnectionManager connMgr = cfg.ConnectionManager;
            GridClientTopology top = cfg.Topology;

            // Try to get node information on any of the connections possible.
            foreach (N node in top.Nodes()) {
                try {
                    // Do not try to connect to the same node.
                    if (node.Id.Equals(nodeId))
                        continue;

                    // Get connection to node DIFFER from the requested nodeId.
                    IGridClientConnection conn = connMgr.connection(node);

                    try {
                        // Get node via DIFFERENT grid node to ensure the grid knows about node with nodeId (it is alive).
                        N target = conn.Node(nodeId, false, false, node.Id).Result;

                        // If requested node not found...
                        if (target == null)
                            top.NodeFailed(nodeId);

                        return target != null;
                    }
                    catch (GridClientConnectionResetException e) {
                        connMgr.onFacadeFailed(node, conn, e);
                    }
                    catch (GridClientClosedException) {
                        throw;
                    }
                    catch (GridClientException e) {
                        // Future failed, so connection exception have already been handled.
                        Dbg.WriteLine("Could not receive node by its id [nodeId={0}, e={1}]", nodeId, e);
                    }
                }
                catch (GridClientServerUnreachableException e) {
                    // Try next node.
                    Dbg.WriteLine("Could not receive node by its id [nodeId={0}, e={1}]", nodeId, e);
                }
            }

            return false;
        }

        /**
         * <summary>
         * Gets most recently refreshed topology. If this compute instance is a projection,
         * then only nodes that satisfy projection criteria will be returned.</summary>
         *
         * <returns>Most recently refreshed topology.</returns>
         */
        protected IList<N> ProjectionNodes() {
            IList<N> prjNodes;

            if (_nodes == null) {
                // Dynamic projection, ask topology for current snapshot.
                prjNodes = cfg.Topology.Nodes();

                if (_filter != null)
                    prjNodes = U.ApplyFilter(prjNodes, _filter);
            }
            else
                prjNodes = _nodes;

            return prjNodes;
        }

        /**
         * <summary>
         * Return balanced node for current projection.</summary>
         *
         * <returns>Balanced node.</returns>
         * <exception cref="GridClientServerUnreachableException">If topology is empty.</exception>
         */
        private N BalancedNode() {
            ICollection<N> prjNodes = ProjectionNodes();

            if (prjNodes.Count == 0) {
                throw new GridClientServerUnreachableException("Failed to get balanced node (no nodes in topology were " +
                    "accepted by the filter): " + _filter);
            }

            if (prjNodes.Count == 1)
                return prjNodes.First();

            return _balancer.BalancedNode(prjNodes);
        }

        /**
         * <summary>
         * Creates a sub-projection for current projection.</summary>
         *
         * <param name="nodes">Collection of nodes that sub-projection will be restricted to. If <c>null</c>,</param>
         *      created projection is dynamic and will take nodes from topology.
         * <param name="filter">Filter to be applied to nodes in projection.</param>
         * <param name="balancer">Balancer to use in projection.</param>
         * <returns>Created projection.</returns>
         * <exception cref="GridClientException">
         * If resulting projection is empty. Note that this exception may only be thrown on
         * case of static projections, i.e. where collection of nodes is not null.</exception>
         */
        protected T CreateProjection(ICollection<N> nodes, Predicate<N> filter, IGridClientLoadBalancer balancer) {
            if (nodes != null && nodes.Count == 0)
                throw new GridClientException("Failed to create projection: given nodes collection is empty.");

            if (filter != null && this._filter != null)
                filter = U.And<N>(this._filter, filter);
            else if (filter == null)
                filter = this._filter;

            ICollection<N> subset = Intersection(this._nodes, nodes);

            if (subset != null && subset.Count == 0)
                throw new GridClientException("Failed to create projection (given node set does not overlap with " +
                    "existing node set) [prjNodes=" + this._nodes + ", nodes=" + nodes);

            if (filter != null && subset != null) {
                subset = U.ApplyFilter(subset, filter);

                if (subset != null && subset.Count == 0)
                    throw new GridClientException("Failed to create projection (none of the nodes in projection node " +
                        "set passed the filter) [prjNodes=" + subset + ", filter=" + filter + ']');
            }

            if (balancer == null)
                balancer = this._balancer;

            return CreateProjectionImpl(nodes, filter, balancer);
        }

        /**
         * <summary>
         * Subclasses must implement this method and return concrete implementation of projection needed.</summary>
         *
         * <param name="nodes">Nodes that are included in projection.</param>
         * <param name="filter">Filter to be applied.</param>
         * <param name="balancer">Balancer to be used.</param>
         * <returns>Created projection.</returns>
         */
        protected abstract T CreateProjectionImpl(IEnumerable<N> nodes, Predicate<N> filter, IGridClientLoadBalancer balancer);

        /**
         * <summary>
         * Calculates intersection of two collections. Returned collection always a new collection.</summary>
         *
         * <param name="first">First collection to intersect.</param>
         * <param name="second">Second collection to intersect.</param>
         * <returns>Intersection or <c>null</c> if both collections are <c>null</c>.</returns>
         */
        private static ICollection<N> Intersection(IEnumerable<N> first, IEnumerable<N> second) {
            if (first == null && second == null)
                return null;

            if (first != null && second != null) {
                ISet<N> set = new HashSet<N>(second);
                IList<N> res = new List<N>();

                foreach (N node in first)
                    if (set.Contains(node))
                        res.Add(node);

                return res;
            }
            else
                return new List<N>(first != null ? first : second);
        }
    }
}
