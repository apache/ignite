// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Collections.Generic;
    using GridGain.Client;
    using GridGain.Client.Balancer;
    using GridGain.Client.Util;

    using N = GridGain.Client.IGridClientNode;
    using U = GridGain.Client.Util.GridClientUtils;

    /** <summary>Compute projection implementation.</summary> */

    internal class GridClientComputeImpl : GridClientAbstractProjection<GridClientComputeImpl>, IGridClientCompute {
        /**
         * <summary>
         * Creates a new compute projection.</summary>
         *
         * <param name="cfg">Projection configuration.</param>
         * <param name="nodes">Nodes to be included in this projection.</param>
         * <param name="nodeFilter">Node filter to be used for this projection.</param>
         * <param name="balancer">Balancer to be used in this projection.</param>
         */
        internal GridClientComputeImpl(IGridClientProjectionConfig cfg, IEnumerable<N> nodes,
            Predicate<N> nodeFilter, IGridClientLoadBalancer balancer)
            : base(cfg, nodes, nodeFilter, balancer) {
        }

        /** <inheritdoc /> */
        public IGridClientLoadBalancer Balancer {
            get {
                return _balancer;
            }
        }

        /** <inheritdoc /> */
        public IGridClientCompute Projection(N node) {
            return CreateProjection(U.List(node), null, null);
        }

        /** <inheritdoc /> */
        public IGridClientCompute Projection(Predicate<N> filter) {
            return CreateProjection(null, filter, null);
        }

        /** <inheritdoc /> */
        public IGridClientCompute Projection(ICollection<N> nodes) {
            return CreateProjection(nodes, null, null);
        }

        /** <inheritdoc /> */
        public IGridClientCompute Projection(Predicate<N> filter, IGridClientLoadBalancer balancer) {
            return CreateProjection(null, filter, balancer);
        }

        /** <inheritdoc /> */
        public IGridClientCompute Projection(ICollection<N> nodes, IGridClientLoadBalancer balancer) {
            return CreateProjection(nodes, null, balancer);
        }

        /** <inheritdoc /> */
        public T Execute<T>(String taskName, Object taskArg) {
            return this.ExecuteAsync<T>(taskName, taskArg).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<T> ExecuteAsync<T>(String taskName, Object taskArg) {
            return WithReconnectHandling((conn, nodeId) => conn.Execute<T>(taskName, taskArg, nodeId));
        }

        /** <inheritdoc /> */
        public T AffinityExecute<T>(String taskName, String cacheName, Object affKey, Object taskArg) {
            return this.AffinityExecuteAsync<T>(taskName, cacheName, affKey, taskArg).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<T> AffinityExecuteAsync<T>(String taskName, String cacheName, Object affKey, Object taskArg) {
            return WithReconnectHandling((conn, nodeId) => conn.Execute<T>(taskName, taskArg, nodeId), cacheName, affKey);
        }

        /** <inheritdoc /> */
        public N Node(Guid id) {
            return cfg.Topology.Node(id);
        }

        /**
         * <summary>
         * Gets most recently refreshed topology. If this compute instance is a projection,
         * then only nodes that satisfy projection criteria will be returned.</summary>
         *
         * <returns>Most recently refreshed topology.</returns>
         */
        public IList<N> Nodes() {
            return ProjectionNodes();
        }

        /** <inheritdoc /> */
        public IList<N> Nodes(ICollection<Guid> ids) {
            return cfg.Topology.Nodes(ids);
        }

        /** <inheritdoc /> */
        public IList<N> Nodes(Predicate<N> filter) {
            return U.ApplyFilter(ProjectionNodes(), filter);
        }

        /** <inheritdoc /> */
        public N RefreshNode(Guid id, bool includeAttrs, bool includeMetrics) {
            return RefreshNodeAsync(id, includeAttrs, includeMetrics).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<N> RefreshNodeAsync(Guid id, bool includeAttrs, bool includeMetrics) {
            return WithReconnectHandling((conn, nodeId) => conn.Node(id, includeAttrs, includeMetrics, nodeId));
        }

        /** <inheritdoc /> */
        public N RefreshNode(String ip, bool includeAttrs, bool includeMetrics) {
            return RefreshNodeAsync(ip, includeAttrs, includeMetrics).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<N> RefreshNodeAsync(String ip, bool includeAttrs, bool includeMetrics) {
            return WithReconnectHandling((conn, nodeId) => conn.Node(ip, includeAttrs, includeMetrics, nodeId));
        }

        /** <inheritdoc /> */
        public IList<N> RefreshTopology(bool includeAttrs, bool includeMetrics) {
            return RefreshTopologyAsync(includeAttrs, includeMetrics).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<IList<N>> RefreshTopologyAsync(bool includeAttrs, bool includeMetrics) {
            return WithReconnectHandling((conn, nodeId) => conn.Topology(includeAttrs, false, nodeId));
        }

        /** <inheritdoc /> */
        public IList<String> Log(int lineFrom, int lineTo) {
            return LogAsync(lineFrom, lineTo).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<IList<String>> LogAsync(int lineFrom, int lineTo) {
            return WithReconnectHandling((conn, nodeId) => conn.Log(null, lineFrom, lineTo, nodeId));
        }

        /** <inheritdoc /> */
        public IList<String> Log(String path, int lineFrom, int lineTo) {
            return LogAsync(path, lineFrom, lineTo).Result;
        }

        /** <inheritdoc /> */
        public IGridClientFuture<IList<String>> LogAsync(String path, int lineFrom, int lineTo) {
            return WithReconnectHandling((conn, nodeId) => conn.Log(path, lineFrom, lineTo, nodeId));
        }

        /** <inheritdoc /> */
        override protected GridClientComputeImpl CreateProjectionImpl(IEnumerable<N> nodes,
            Predicate<N> filter, IGridClientLoadBalancer balancer) {
            return new GridClientComputeImpl(cfg, nodes, filter, balancer);
        }
    }
}
