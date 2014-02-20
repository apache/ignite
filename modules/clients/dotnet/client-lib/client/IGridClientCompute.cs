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
    using GridGain.Client.Balancer;

    /**
     * <summary>
     * A compute projection of grid client. Contains various methods for task execution,
     * full and partial (per node) topology refresh and log downloading.</summary>
     */
    public interface IGridClientCompute {
        /**
         * <summary>
         * Creates a projection that will communicate only with specified remote node.
         * <para/>
         * If current projection is dynamic projection, then this method will check is passed node is in topology.
         * If any filters were specified in current topology, this method will check if passed node is accepted by
         * the filter. If current projection was restricted to any subset of nodes, this method will check if
         * passed node is in that subset. If any of the checks fails an exception will be thrown.</summary>
         *
         * <param name="node">Single node to which this projection will be restricted.</param>
         * <returns>Resulting static projection that is bound to a given node.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientCompute Projection(IGridClientNode node);

        /**
         * <summary>
         * Creates a projection that will communicate only with nodes that are accepted by the passed filter.
         * <para/>
         * If current projection is dynamic projection, then filter will be applied to the most relevant
         * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
         * then resulting projection will only be restricted to nodes that were in parent projection and were
         * accepted by the passed filter. If any of the checks fails an exception will be thrown.</summary>
         *
         * <param name="filter">Filter that will select nodes for projection.</param>
         * <returns>Resulting projection (static or dynamic, depending in parent projection type).</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientCompute Projection(Predicate<IGridClientNode> filter);

        /**
         * <summary>
         * Creates a projection that will communicate only with specified remote nodes. For any particular call
         * a node to communicate will be selected with balancer of this projection.
         * <para/>
         * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
         * If any filters were specified in current topology, this method will check if passed nodes are accepted by
         * the filter. If current projection was restricted to any subset of nodes, this method will check if
         * passed nodes are in that subset (i.e. calculate the intersection of two collections).
         * If any of the checks fails an exception will be thrown.</summary>
         *
         * <param name="nodes">Collection of nodes to which this projection will be restricted.</param>
         * <returns>Resulting static projection that is bound to a given nodes.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientCompute Projection(ICollection<IGridClientNode> nodes);

        /**
         * <summary>
         * Creates a projection that will communicate only with nodes that are accepted by the passed filter. The
         * balancer passed will override default balancer specified in configuration.
         * <para/>
         * If current projection is dynamic projection, then filter will be applied to the most relevant
         * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
         * then resulting projection will only be restricted to nodes that were in parent projection and were
         * accepted by the passed filter. If any of the checks fails an exception will be thrown.</summary>
         *
         * <param name="filter">Filter that will select nodes for projection.</param>
         * <param name="balancer">Balancer that will select balanced node in resulting projection.</param>
         * <returns>Resulting projection (static or dynamic, depending in parent projection type).</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientCompute Projection(Predicate<IGridClientNode> filter, IGridClientLoadBalancer balancer);

        /**
         * <summary>
         * Creates a projection that will communicate only with specified remote nodes. For any particular call
         * a node to communicate will be selected with passed balancer..
         * <para/>
         * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
         * If any filters were specified in current topology, this method will check if passed nodes are accepted by
         * the filter. If current projection was restricted to any subset of nodes, this method will check if
         * passed nodes are in that subset (i.e. calculate the intersection of two collections).
         * If any of the checks fails an exception will be thrown.</summary>
         *
         * <param name="nodes">Collection of nodes to which this projection will be restricted.</param>
         * <param name="balancer">Balancer that will select nodes in resulting projection.</param>
         * <returns>Resulting static projection that is bound to a given nodes.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientCompute Projection(ICollection<IGridClientNode> nodes, IGridClientLoadBalancer balancer);

        /**
         * <summary>
         * Gets balancer used by projection.</summary>
         *
         * <returns>Instance of <see cref="IGridClientLoadBalancer"/>.</returns>
         */
        IGridClientLoadBalancer Balancer {
            get;
        }

        /**
         * <summary>
         * Executes task.</summary>
         *
         * <param name="taskName">Task name or task class name.</param>
         * <param name="taskArg">Optional task argument.</param>
         * <returns>Task execution result.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        T Execute<T>(String taskName, Object taskArg);

        /**
         * <summary>
         * Asynchronously executes task.</summary>
         *
         * <param name="taskName">Task name or task class name.</param>
         * <param name="taskArg">Optional task argument.</param>
         * <returns>Future.</returns>
         */
        IGridClientFuture<T> ExecuteAsync<T>(String taskName, Object taskArg);

        /**
         * <summary>
         * Executes task using cache affinity key for routing. This way the task will start executing
         * exactly on the node where this affinity key is cached.</summary>
         *
         * <param name="taskName">Task name or task class name.</param>
         * <param name="cacheName">Name of the cache on which affinity should be calculated.</param>
         * <param name="affKey">Affinity key.</param>
         * <param name="taskArg">Optional task argument.</param>
         * <returns>Task execution result.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        T AffinityExecute<T>(String taskName, String cacheName, Object affKey, Object taskArg);

        /**
         * <summary>
         * Asynchronously executes task using cache affinity key for routing. This way
         * the task will start executing exactly on the node where this affinity key is cached.</summary>
         *
         * <param name="taskName">Task name or task class name.</param>
         * <param name="cacheName">Name of the cache on which affinity should be calculated.</param>
         * <param name="affKey">Affinity key.</param>
         * <param name="taskArg">Optional task argument.</param>
         * <returns>Future.</returns>
         */
        IGridClientFuture<T> AffinityExecuteAsync<T>(String taskName, String cacheName, Object affKey,
            Object taskArg);

        /**
         * <summary>
         * Gets most recently refreshed topology. If this compute instance is a projection,
         * then only nodes that satisfy projection criteria will be returned.</summary>
         *
         * <returns>Most recently refreshed topology.</returns>
         */
        IList<IGridClientNode> Nodes();

        /**
         * <summary>
         * Gets node with given id from most recently refreshed topology.</summary>
         *
         * <param name="id">Node ID.</param>
         * <returns>Node for given ID or <c>null</c> if node with given id was not found.</returns>
         */
        IGridClientNode Node(Guid id);

        /**
         * <summary>
         * Gets nodes for the given IDs based on most recently refreshed topology.</summary>
         *
         * <param name="ids">Node IDs.</param>
         * <returns>Collection of nodes for provided IDs.</returns>
         */
        IList<IGridClientNode> Nodes(ICollection<Guid> ids);

        /**
         * <summary>
         * Gets nodes that passes the filter. If this compute instance is a projection, then only
         * nodes that passes projection criteria will be passed to the filter.</summary>
         *
         * <param name="filter">Node filter.</param>
         * <returns>Collection of nodes that satisfy provided filter.</returns>
         */
        IList<IGridClientNode> Nodes(Predicate<IGridClientNode> filter);

        /**
         * <summary>
         * Gets node by its ID.</summary>
         *
         * <param name="id">Node ID.</param>
         * <param name="includeAttrs">Whether to include node attributes.</param>
         * <param name="includeMetrics">Whether to include node metrics.</param>
         * <returns>Node descriptor or <c>null</c> if node doesn't exist.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientNode RefreshNode(Guid id, bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Asynchronously gets node by its ID.</summary>
         *
         * <param name="id">Node ID.</param>
         * <param name="includeAttrs">Whether to include node attributes.</param>
         * <param name="includeMetrics">Whether to include node metrics.</param>
         * <returns>Future.</returns>
         */
        IGridClientFuture<IGridClientNode> RefreshNodeAsync(Guid id, bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Gets node by IP address.</summary>
         *
         * <param name="ip">IP address.</param>
         * <param name="includeAttrs">Whether to include node attributes.</param>
         * <param name="includeMetrics">Whether to include node metrics.</param>
         * <returns>Node descriptor or <c>null</c> if node doesn't exist.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IGridClientNode RefreshNode(String ip, bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Asynchronously gets node by IP address.</summary>
         *
         * <param name="ip">IP address.</param>
         * <param name="includeAttrs">Whether to include node attributes.</param>
         * <param name="includeMetrics">Whether to include node metrics.</param>
         * <returns>Future.</returns>
         */
        IGridClientFuture<IGridClientNode> RefreshNodeAsync(String ip, bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Gets current topology.</summary>
         *
         * <param name="includeAttrs">Whether to include node attributes.</param>
         * <param name="includeMetrics">Whether to include node metrics.</param>
         * <returns>Node descriptors.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IList<IGridClientNode> RefreshTopology(bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Asynchronously gets current topology.</summary>
         *
         * <param name="includeAttrs">Whether to include node attributes.</param>
         * <param name="includeMetrics">Whether to include node metrics.</param>
         * <returns>Future.</returns>
         */
        IGridClientFuture<IList<IGridClientNode>> RefreshTopologyAsync(bool includeAttrs, bool includeMetrics);

        /**
         * <summary>
         * Gets contents of default log file (<c>GRIDGAIN_HOME/work/log/gridgain.log</c>).</summary>
         *
         * <param name="lineFrom">Index of line from which log is get, inclusive (starting from 0).</param>
         * <param name="lineTo">Index of line to which log is get, inclusive (starting from 0).</param>
         * <returns>Log contents.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IList<String> Log(int lineFrom, int lineTo);

        /**
         * <summary>
         * Asynchronously gets contents of default log file
         * (<c>GRIDGAIN_HOME/work/log/gridgain.log</c>).</summary>
         *
         * <param name="lineFrom">Index of line from which log is get, inclusive (starting from 0).</param>
         * <param name="lineTo">Index of line to which log is get, inclusive (starting from 0).</param>
         * <returns>Future.</returns>
         */
        IGridClientFuture<IList<String>> LogAsync(int lineFrom, int lineTo);

        /**
         * <summary>
         * Gets contents of custom log file.</summary>
         *
         * <param name="path">Log file path. Can be absolute or relative to GRIDGAIN_HOME.</param>
         * <param name="lineFrom">Index of line from which log is get, inclusive (starting from 0).</param>
         * <param name="lineTo">Index of line to which log is get, inclusive (starting from 0).</param>
         * <returns>Log contents.</returns>
         * <exception cref="GridClientException">In case of error.</exception>
         */
        IList<String> Log(String path, int lineFrom, int lineTo);

        /**
         * <summary>
         * Asynchronously gets contents of custom log file.</summary>
         *
         * <param name="path">Log file path. Can be absolute or relative to GRIDGAIN_HOME.</param>
         * <param name="lineFrom">Index of line from which log is get, inclusive (starting from 0).</param>
         * <param name="lineTo">Index of line to which log is get, inclusive (starting from 0).</param>
         * <returns>Future.</returns>
         */
        IGridClientFuture<IList<String>> LogAsync(String path, int lineFrom, int lineTo);
    }
}
