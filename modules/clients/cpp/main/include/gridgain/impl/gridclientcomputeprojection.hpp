/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_COMPUTE_PROJECTION_IMP_HPP
#define GRID_CLIENT_COMPUTE_PROJECTION_IMP_HPP

#include "gridgain/gridclientcompute.hpp"
#include "gridgain/impl/gridclientprojection.hpp"
#include "gridgain/impl/utils/gridthreadpool.hpp"

class GridTopologyRequestCommand;

/**
 * Implementation of GridClientCompute.
 */
class GridClientComputeProjectionImpl : public GridClientCompute, public GridClientProjectionImpl {
public:
    /**
     * Constructor.
     *
     * @param data Shared data.
     * @param prjLsnr Projection listener.
     * @param pFilter Node filter (may be omitted).
     */
    GridClientComputeProjectionImpl(TGridClientSharedDataPtr data, GridClientProjectionListener& prjLsnr,
    		TGridClientNodePredicatePtr pFilter,
            TGridClientLoadBalancerPtr balancer, TGridThreadPoolPtr& threadPool);
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
    virtual TGridClientComputePtr projection(const GridClientNode& node);

     /**
      * Creates a projection that will communicate only with specified remote nodes. For any particular call
      * a node to communicate will be selected with passed balancer..
      * <p>
      * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
      * If any filters were specified in current topology, this method will check if passed nodes are accepted by
      * the filter. If current projection was restricted to any subset of nodes, this method will check if
      * passed nodes are in that subset (i.e. calculate the intersection of two collections).
      * If any of the checks fails an exception will be thrown.
      *
      * @param nodes Collection of nodes to which this projection will be restricted.
      * @param balancer Balancer that will select nodes in resulting projection.
      * @return Resulting static projection that is bound to a given nodes.
      * @throw GridClientException If resulting projection is empty.
      */
    virtual TGridClientComputePtr projection(const TGridClientNodeList& nodes);

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the passed filter.
     * <p>
     * If current projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in parent projection and were
     * accepted by the passed filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(TGridClientNodePredicatePtr filter);

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the passed filter.
     * <p>
     * If current projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in parent projection and were
     * accepted by the passed filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(std::function<bool (const GridClientNode&)> & filter);

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the passed filter. The
     * balancer passed will override default balancer specified in configuration.
     * <p>
     * If current projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in parent projection and were
     * accepted by the passed filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection.
     * @param balancer Balancer that will select balanced node in resulting projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(TGridClientNodePredicatePtr filter,
        TGridClientLoadBalancerPtr balancer);

    /**
     * Creates a projection that will communicate only with nodes that are accepted by the passed filter. The
     * balancer passed will override default balancer specified in configuration.
     * <p>
     * If current projection is dynamic projection, then filter will be applied to the most relevant
     * topology snapshot every time a node to communicate is selected. If current projection is a static projection,
     * then resulting projection will only be restricted to nodes that were in parent projection and were
     * accepted by the passed filter. If any of the checks fails an exception will be thrown.
     *
     * @param filter Filter that will select nodes for projection.
     * @param balancer Balancer that will select balanced node in resulting projection.
     * @return Resulting projection (static or dynamic, depending in parent projection type).
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(std::function<bool (const GridClientNode&)> & filter,
        TGridClientLoadBalancerPtr balancer);

    /**
     * Creates a projection that will communicate only with specified remote nodes. For any particular call
     * a node to communicate will be selected with passed balancer..
     * <p>
     * If current projection is dynamic projection, then this method will check is passed nodes are in topology.
     * If any filters were specified in current topology, this method will check if passed nodes are accepted by
     * the filter. If current projection was restricted to any subset of nodes, this method will check if
     * passed nodes are in that subset (i.e. calculate the intersection of two collections).
     * If any of the checks fails an exception will be thrown.
     *
     * @param nodes Collection of nodes to which this projection will be restricted.
     * @param balancer Balancer that will select nodes in resulting projection.
     * @return Resulting static projection that is bound to a given nodes.
     * @throws GridClientException If resulting projection is empty.
     */
    virtual TGridClientComputePtr projection(const TGridClientNodeList& nodes,
            TGridClientLoadBalancerPtr balancer);

    /**
     * Gets balancer used by projection.
     *
     * @return Instance of {@link GridClientLoadBalancer}.
     */
    virtual TGridClientLoadBalancerPtr balancer() const;

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
    virtual GridClientVariant execute(const std::string& taskName, const GridClientVariant& taskArg);

    /**
     * Asynchronously executes task.
     *
     * @param taskName Task name or task class name.
     * @param taskArg Optional task argument.
     * @return Future.
     */
    virtual TGridClientFutureVariant executeAsync(const std::string& taskName, const GridClientVariant& taskArg);
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
            const GridClientVariant& affKey, const GridClientVariant& taskArg);
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
    virtual TGridClientFutureVariant affinityExecuteAsync(const std::string& taskName,
        const std::string& cacheName, const GridClientVariant& affKey, const GridClientVariant& taskArg);
    /**
     * Gets node with given id from most recently refreshed topology.
     *
     * @param id Node ID.
     * @return Node for given ID or <tt>null</tt> if node with given id was not found.
     */
    virtual TGridClientNodePtr node(const GridClientUuid& id) const;

    /**
     * Gets nodes that pass the filter. If this compute instance is a projection, then only
     * nodes that pass projection criteria will be passed to the filter.
     *
     * @param filter Node filter.
     * @return Collection of nodes that satisfy provided filter.
     */
    virtual TGridClientNodeList nodes(TGridClientNodePredicatePtr filter) const;

    /**
     * Gets nodes that pass the filter. If this compute instance is a projection, then only
     * nodes that pass projection criteria will be passed to the filter.
     *
     * @param filter Node filter.
     * @return Collection of nodes that satisfy provided filter.
     */
    virtual TGridClientNodeList nodes(std::function<bool (const GridClientNode&)> & filter) const;

    /**
     * Gets all nodes in the projection.
     *
     * @param filter Node filter.
     * @return Collection of nodes that satisfy provided filter.
     */
    virtual TGridClientNodeList nodes() const;
    /**
     * Gets nodes that passes the filter. If this compute instance is a projection, then only
     * nodes that passes projection criteria will be passed to the filter.
     *
     * @param filter Node filter.
     * @return Collection of nodes that satisfy provided filter.
     */
   virtual TGridClientNodeList nodes(const std::vector<GridClientUuid>& ids) const;

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
    virtual TGridClientNodePtr refreshNode(const GridClientUuid& id, bool includeAttrs, bool includeMetrics);

    /**
     * Asynchronously gets node by its ID.
     *
     * @param id Node ID.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     */
    virtual TGridClientNodeFuturePtr refreshNodeAsync(const GridClientUuid& id, bool includeAttrs,
        bool includeMetrics);

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
    virtual TGridClientNodePtr refreshNode(const std::string& ip, bool includeAttrs, bool includeMetrics);

    /**
     * Asynchronously gets node by IP address.
     *
     * @param ip IP address.
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     */
    virtual TGridClientNodeFuturePtr refreshNodeAsync(const std::string& ip, bool includeAttrs,
        bool includeMetrics);

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
    virtual TGridClientNodeList refreshTopology(bool includeAttrs, bool includeMetrics);

    /**
     * Asynchronously gets current topology.
     *
     * @param includeAttrs Whether to include node attributes.
     * @param includeMetrics Whether to include node metrics.
     * @return Future.
     */
    virtual TGridClientNodeFutureList refreshTopologyAsync(bool includeAttrs, bool includeMetrics);

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
    virtual std::vector<std::string> log(int lineFrom, int lineTo);

    /**
     * Asynchronously gets contents of default log file
     * (<tt>GRIDGAIN_HOME/work/log/gridgain.log</tt>).
     *
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Future.
     */
    virtual TGridFutureStringList logAsync(int lineFrom, int lineTo);

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
    virtual std::vector<std::string> log(const std::string& path, int lineFrom, int lineTo);

    /**
     * Asynchronously gets contents of custom log file.
     *
     * @param path Log file path. Can be absolute or relative to GRIDGAIN_HOME.
     * @param lineFrom Index of line from which log is get, inclusive (starting from 0).
     * @param lineTo Index of line to which log is get, inclusive (starting from 0).
     * @return Future.
     */
    virtual TGridFutureStringList logAsync(const std::string& path, int lineFrom, int lineTo);

    /**
	 * Invalidates this data instance. This is done by the client to indicate
	 * that is has been stopped. After this call, all interface methods
	 * will throw GridClientClosedException.
	 */
    void invalidate();

protected:
    /**
     * Creates subprojection based on node filter and balancer.
     *
     * @param filter Node filter.
     * @param balancer Load balancer.
     */
    TGridClientComputePtr makeProjection(TGridClientNodePredicatePtr filter,
             TGridClientLoadBalancerPtr balancer);

private:
     /**
      * Internal refresh topology command.
      *
      * @param cmd Topology command to execute.
      * @return Refreshed nodes.
      */
     TGridClientNodeList refreshTopology(GridTopologyRequestCommand& cmd);

     /** List of current subprojections. */
     std::vector< std::shared_ptr<GridClientCompute> > subProjections;

     /** Invalidated flag. */
     TGridAtomicBool invalidated;

     /** Thread pool for running async operations. */
     TGridThreadPoolPtr threadPool;
};

#endif
