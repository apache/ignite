// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLEINT_PROJECTION_HPP_INCLUDED
#define GRID_CLEINT_PROJECTION_HPP_INCLUDED

#include "gridgain/gridclienttypedef.hpp"
#include "gridgain/impl/gridclientpartitionedaffinity.hpp"
#include "gridgain/impl/filter/gridclientfilter.hpp"
#include "gridgain/impl/gridclienttopology.hpp"
#include "gridgain/impl/gridclientshareddata.hpp"
#include "gridgain/gridclienttypedef.hpp"
#include "gridgain/impl/filter/gridclientnodefilter.hpp"
#include "gridgain/impl/gridclientprojectionlistener.hpp"

class ClientProjectionClosure;

/**
 * Implementation of client projection.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientProjectionImpl {
public:
    /**
     * Constructor of client projection.
     *
     * @param pData Pointer to shared data.
     * @param prjLsnr Projection listener.
     * @param pFilter Pointer to a node predicate.
     */
    GridClientProjectionImpl(TGridClientSharedDataPtr pData, GridClientProjectionListener& prjLsnr,
        TGridClientNodePredicatePtr pFilter = TGridClientNodePredicatePtr((TGridClientNodePredicate*) 0));

    /** Public destructor. */
    virtual ~GridClientProjectionImpl() {
    }

    /**
     * This method executes request to a communication layer and handles connection error, if it occurs.
     * In case of communication exception client instance is notified and new instance of client is created.
     * If none of the grid servers can be reached, an exception is thrown.
     *
     * @param c Closure to be executed.
     * @throws GridServerUnreachableException If either none of the server can be reached or each attempt
     *      of communication within {@link #RETRY_CNT} attempts resulted in {@link GridClientConnectionResetException}.
     * @throws GridClientException In case of problems.
     */
    void withReconnectHandling(ClientProjectionClosure& c) const;

    /**
     * This method executes request to a communication layer and handles connection error, if it occurs. Server
     * is picked up according to the projection affinity and key given. Connection will be made with the node
     * on which key is cached. In case of communication exception client instance is notified and new instance
     * of client is created. If none of servers can be reached, an exception is thrown.
     *
     * @param c Closure to be executed.
     * @param cacheName Cache name for which mapped node will be calculated.
     * @param affKey Affinity key.
     * @throws GridClientException In case of problems.
     */
    void withReconnectHandling(ClientProjectionClosure& c, const std::string& cacheName,
            const GridHasheableObject& affKey);

protected:

    /** Gets the primary node for the given cache and the affinity key */
    TGridClientNodePtr affinityNode(const std::string& cacheName, const GridHasheableObject& affKey);

    /** Gets the current node to be used for processing. */
    TGridClientNodePtr balancedNode() const;

    /** Gets nodes for the given sub-projection. */
    void subProjectionNodes(TNodesSet& nodes, const TGridClientNodePredicate& dynamicFilter =
            GridClientAllPassFilter()) const;

    /** Returns the unique ID of the client. */
    std::string clientUniqueId() const {
        return sharedData->clientUniqueId();
    }

    /** Returns the unique ID of the client. */
    GridUuid & clientUniqueUuid() {
        return sharedData->clientUniqueUuid();
    }

    /** Returns the actual list of the nodes in the current topology. */
    TNodesSet topologyNodes() const {
        return sharedData->topology()->nodes();
    }

    /** Returns the definition of the node by the given uuid. */
    TGridClientNodePtr node(const GridUuid& uuid) const {
        return sharedData->topology()->node(uuid);
    }

    /** Returns the current executor. */
    std::shared_ptr<GridClientCommandExecutorPrivate> executor() const {
        return sharedData->executor();
    }

    /** Returns the active version of the load balancer for the given projection. */
    virtual TGridClientLoadBalancerPtr loadBalancer() const {
        return loadBalancer_.get() != NULL ? loadBalancer_ : sharedData->loadBalancer();
    }

    /** Returns the current protocol used by the client. */
    GridClientProtocol protocol() const {
        return sharedData->protocol();
    }

    /** Shared data used by all the instances of projections */
    const TGridClientSharedDataPtr sharedData;

    /** Projection listener. */
    GridClientProjectionListener& prjLsnr;

    /** The current filter for nodes in the sub-projection. */
    TGridClientNodePredicatePtr filter;

    /** Affinity functions. */
    std::map<std::string, std::shared_ptr<GridClientDataAffinity>> affinityMap;

    /** Default affinity. */
    std::shared_ptr<GridClientDataAffinity> dfltAffinity;

    /** Local Load balancer if there is any. */
    TGridClientLoadBalancerPtr loadBalancer_;
private:
    TGridClientNodePtr balancedNode(const std::string& cacheName) const;

    TGridClientNodePtr balancedNode(const TNodesSet& nodes) const;

    /** Executes the closure on the particular node. */
    bool processClosure(TGridClientNodePtr node, ClientProjectionClosure& c) const;
};

#endif
