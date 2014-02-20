// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTCOMPUTE_HPP_INCLUDED
#define GRIDCLIENTCOMPUTE_HPP_INCLUDED

#include <gridgain/gridconf.hpp>
#include <gridgain/gridclienttypedef.hpp>
#include <gridgain/gridclientvariant.hpp>

/** Forward declaration */
class GridClientNode;

/**
 * A compute projection of grid client. Contains various methods for a task execution, full and partial (per node)
 * topology refresh and log downloading.
 *
 * @author @cpp.author
 * @version @cpp.version
 *
 */
class GRIDGAIN_API GridClientCompute {
public:
    /** Destructor. */
    virtual ~GridClientCompute() {
    }

    /**
     * Creates a projection that will communicate only with specified remote node.
     * <p>
     * If current projection is dynamic projection, then this method will check is passed node is in topology.
     * If any filters were specified in current topology, this method will check if passed node is accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed node is in that subset. If any of the checks fails an exception will be thrown.
     *
     * @param node Single node to which this projection will be restricted.
     * @return Resulting static projection that is bound to a given node.
     * @throw GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(const GridClientNode& node) = 0;

    /**
     * Creates a projection that will communicate only with specified remote nodes. For any particular call
     * a node to communicate will be selected using the balancer of this projection.
     * <p>
     * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
     * If any filters were specified in current topology, this method will check if passed nodes are accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed nodes are in that subset (i.e. calculate the intersection of two collections).
     * If any of the checks fails an exception will be thrown.
     *
     * @param nodes Collection of nodes to which this projection will be restricted.
     * @return Resulting static projection that is bound to the given nodes.
     * @throw GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(const TGridClientNodeList& nodes) = 0;

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the given filter.
     * <p>
     * If this projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node is selected. If this projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in this projection and were
     * accepted by the filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(TGridClientNodePredicatePtr filter) = 0;

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the given filter. The
     * balancer will override the default balancer specified in the configuration.
     * <p>
     * If this projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If this projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in this projection and were
     * accepted by the filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection.
     * @param balancer Balancer that will select balanced node in resulting projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(TGridClientNodePredicatePtr filter,
            TGridClientLoadBalancerPtr balancer) = 0;

    /**
     * Creates a projection that will communicate only with specified remote nodes. For any particular call
     * a node to communicate will be selected by the load balancer.
     * <p>
     * If this projection is a dynamic projection, then this method will check if the given nodes are in the topology.
     * If a filter is specified by this projection, this method will check if the given nodes are accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed nodes are in that subset (i.e. calculate the intersection of two collections).
     * If any of the checks fails an exception will be thrown.
     *
     * @param nodes Collection of nodes to which this projection will be restricted.
     * @param balancer Balancer that will select nodes in resulting projection.
     * @return Resulting static projection that is bound to a given nodes.
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(const TGridClientNodeList& nodes, TGridClientLoadBalancerPtr balancer) = 0;

    /**
     * Gets balancer used by this projection.
     *
     * @return Instance of {@link GridClientLoadBalancer}.
     */
    virtual TGridClientLoadBalancerPtr balancer() const = 0;

    /**
     * Executes task.
     *
     * @param taskName Task name or task class name.
     * @param taskArg Optional task argument.
     * @return Task execution result.
     * @throw GridClientException In case of error.
     * @throw GridServerUnreachableException If none of the servers can be reached.
     * @throw GridClientClosedException If client was closed manually.
     */
    virtual GridClientVariant execute(const std::string& taskName, const GridClientVariant& taskArg =
            GridClientVariant()) = 0;

    /**
     * Asynchronously executes task.
     *
     * @param taskName Task name or task class name.
     * @param taskArg Optional task argument.
     * @return Future.
     */
    virtual TGridClientFutureVariant executeAsync(const std::string& taskName, const GridClientVariant& taskArg =
            GridClientVariant()) = 0;

    /**
     * Executes task using cache affinity key for routing. This way the task will start executing
     * exactly on the node where this affinity key is cached.
     *
     * @param taskName Task name or task class name.
     * @param cacheName Name of the cache on which affinity should be calculated.
     * @param affKey Affinity key.
     * @param taskArg Optional task argument.
     * @return Task execution result.
     * @throw GridClientException In case of error.
     * @throw GridServerUnreachableException If none of the servers can be reached.
     * @throw GridClientClosedException If client was closed manually.
     */
    virtual GridClientVariant affinityExecute(const std::string& taskName, const std::string& cacheName,
            const GridClientVariant& affKey, const GridClientVariant& taskArg = GridClientVariant()) = 0;

    /**
     * Asynchronously executes task using cache affinity key for routing. This way
     * the task will start executing exactly on the node where this affinity key is cached.
     *
     * @param taskName Task name or task class name.
     * @param cacheName Name of the cache on which affinity should be calculated.
     * @param affKey Affinity key.
     * @param taskArg Optional task argument.
     * @return Future.
     */
    virtual TGridClientFutureVariant affinityExecuteAsync(const std::string& taskName, const std::string& cacheName,
            const GridClientVariant& affKey, const GridClientVariant& taskArg = GridClientVariant()) = 0;

    /**
     * Gets node with given id from most recently refreshed topology.
     *
     * @param id Node ID.
     * @return Node for given ID or <tt>null</tt> if node with given id was not found.
     */
    virtual TGridClientNodePtr node(const GridUuid& id) const = 0;

    /**
     * Gets nodes that passes the filter. If this compute instance is a projection, then only
     * nodes that passes projection criteria will be passed to the filter.
     *
     * @param filter Node filter.
     * @return Collection of nodes that satisfy provided filter.
     */
    virtual TGridClientNodeList nodes(TGridClientNodePredicatePtr filter) const = 0;

    /**
     * Gets all nodes in the projection.
     *
     * @return All nodes in the projection.
     */
    virtual TGridClientNodeList nodes() const = 0;

    /**
     * Gets nodes for the given IDs based on most recently refreshed topology.
     *
     * @param ids Node IDs.
     * @return Collection of nodes for provided IDs.
     */
    virtual TGridClientNodeList nodes(const std::vector<GridUuid>& ids) const = 0;

    /**
     * Gets node by its ID.
     *
     * @param id Node ID.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptor or <tt>null</tt> if node doesn't exist.
     * @throw GridClientException In case of error.
     * @throw GridServerUnreachableException If none of the servers can be reached.
     * @throw GridClientClosedException If client was closed manually.
     */
    virtual TGridClientNodePtr refreshNode(const GridUuid& id, bool includeAttrs, bool includeMetrics) = 0;

    /**
     * Asynchronously gets node by its ID.
     *
     * @param id Node ID.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     */
    virtual TGridClientNodeFuturePtr refreshNodeAsync(const GridUuid& id, bool includeAttrs, bool includeMetrics) = 0;

    /**
     * Gets node by IP address.
     *
     * @param ip IP address.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptor or <tt>null</tt> if node doesn't exist.
     * @throw GridClientException In case of error.
     * @throw GridServerUnreachableException If none of the servers can be reached.
     * @throw GridClientClosedException If client was closed manually.
     */
    virtual TGridClientNodePtr refreshNode(const std::string& ip, bool includeAttrs, bool includeMetrics) = 0;

    /**
     * Asynchronously gets node by IP address.
     *
     * @param ip IP address.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     */
    virtual TGridClientNodeFuturePtr refreshNodeAsync(const std::string& ip, bool includeAttrs,
            bool includeMetrics) = 0;

    /**
     * Gets current topology.
     *
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Node descriptors.
     * @throw GridClientException In case of error.
     * @throw GridServerUnreachableException If none of the servers can be reached.
     * @throw GridClientClosedException If client was closed manually.
     */
    virtual TGridClientNodeList refreshTopology(bool includeAttrs, bool includeMetrics) = 0;

    /**
     * Asynchronously gets current topology.
     *
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     */
    virtual TGridClientNodeFutureList refreshTopologyAsync(bool includeAttrs, bool includeMetrics) = 0;

    /**
     * Gets contents of default log file (<tt>GRIDGAIN_HOME/work/log/gridgain.log</tt>).
     *
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Log contents.
     * @throw GridClientException In case of error.
     * @throw GridServerUnreachableException If none of the servers can be reached.
     * @throw GridClientClosedException If client was closed manually.
     */
    virtual std::vector<std::string> log(int lineFrom, int lineTo) = 0;

    /**
     * Asynchronously gets contents of default log file
     * (<tt>GRIDGAIN_HOME/work/log/gridgain.log</tt>).
     *
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Future.
     */
    virtual TGridFutureStringList logAsync(int lineFrom, int lineTo) = 0;

    /**
     * Gets contents of custom log file.
     *
     * @param path Log file path. Can be absolute or relative to GRIDGAIN_HOME.
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Log contents.
     * @throw GridClientException In case of error.
     * @throw GridServerUnreachableException If none of the servers can be reached.
     * @throw GridClientClosedException If client was closed manually.
     */
    virtual std::vector<std::string> log(const std::string& path, int lineFrom, int lineTo) = 0;

    /**
     * Asynchronously gets contents of custom log file.
     *
     * @param path Log file path. Can be absolute or relative to GRIDGAIN_HOME.
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Future.
     */
    virtual TGridFutureStringList logAsync(const std::string& path, int lineFrom, int lineTo) = 0;

};

#endif // GRIDCLIENTCOMPUTE_HPP_INCLUDED
